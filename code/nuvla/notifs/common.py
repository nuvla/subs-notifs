import os
import time
from elasticsearch.helpers import bulk

ES_HOSTS = [{'host': 'es', 'port': 9200}]
KAFKA_TOPIC_SUBS_CONFIG = 'subscription-config'
KAFKA_TOPIC_NUVLAEDGES = 'nuvlabox'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
    KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')
ES_HOSTS = [{'host': 'es', 'port': 9200}]
ES_INDEX_DELETED_ENTITIES = 'subsnotifs-deleted-entities'
ES_INDEX_RXTX = 'subsnotifs-rxtx'


def es_hosts():
    """
    Expected env var: ES_HOSTS=es1:9201,es2:92002
    :return:
    """
    if 'ES_HOSTS' in os.environ:
        es_conf = []
        for es in os.environ['ES_HOSTS'].split(','):
            host, port = es.split(':')
            es_conf.append({'host': host, 'port': int(port)})
        if es_conf:
            return es_conf
    return ES_HOSTS


def get_all_es_records(elastic_instance, index, query, log):
    """
        Fetch all the records for the given index and query
    :param elastic_instance: Elasticsearch instance
    :param index: Index to be searched
    :param query: Query to be used
    :return: All the ids of the records
    """
    offset = 0
    all_records = []
    while True:
        try:
            result = elastic_instance.search(index=index, body=query,
                                             size=50, _source=False, from_=offset)
        except Exception as ex:
            log.error(f'Failed to fetch records for {index} query {query}: {ex}')
            break
        log.debug(f'Found {len(result["hits"]["hits"])} records')
        if len(result["hits"]["hits"]) == 0:
            log.debug(f'No more records to search for query {query}')
            break

        offset += len(result["hits"]["hits"])
        all_records.extend([hit["_id"] for hit in result['hits']['hits']])
        time.sleep(0.05)
    return all_records


def bulk_es_delete(elastic_instance, ids, index, log, refresh=False) -> bool:
    actions = ({
        '_op_type': 'delete',
        '_id': _id
    } for _id in ids)

    try:
        bulk(client=elastic_instance, actions=actions, index=index, refresh=refresh)
    except Exception as ex:
        log.error(f'Exception in bulk delete: {ex}')
        return False
    return True
