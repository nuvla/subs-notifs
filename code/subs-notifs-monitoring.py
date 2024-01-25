#!/usr/bin/env python3

from kafka import KafkaConsumer
import os
import elasticsearch
from elasticsearch.helpers import bulk
import threading
import time, datetime
from nuvla.notifs.log import get_logger
import signal

KAFKA_TOPIC_SUBS_CONFIG = 'subscription-config'
KAFKA_TOPIC_NUVLAEDGES = 'nuvlabox'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
    KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')
ES_HOSTS = [{'host': 'es', 'port': 9200}]
ES_INDEX_DELETED_ENTITIES = 'subsnotifs-deleted-entities'
ES_INDEX_RXTX = 'subsnotifs-rxtx'
log = get_logger('monitoring')
BULK_SIZE = 1000


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


def fetch_deleted_entities(elastic_instance):
    config = dict(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest'
    )
    topics = [KAFKA_TOPIC_SUBS_CONFIG, KAFKA_TOPIC_NUVLAEDGES]
    kafka_consumer = KafkaConsumer(*topics, **config)
    for msg in kafka_consumer:
        log.debug(f'{msg.key} {msg.value}')
        if msg.value is None:
            created = elastic_instance.index(index=ES_INDEX_DELETED_ENTITIES, body={}, id=msg.key)
            log.info(f'Created deleted entity record {created["_id"]}')


def act_on_deleted_subscriptions(elastic_instance: elasticsearch.Elasticsearch):
    """
         1. Fetch all deleted subscriptions
         2. For each deleted subscription, delete all the rx/tx data
         3. Delete the deleted subscription from the deleted-subscriptions index
    :param elastic_instance:
    :return:
    """
    log.info('Acting on deleted subscriptions')
    query = {"query": {"match_all": {}}}
    offset = 0
    ids_to_be_deleted = []
    while True:
        result = elastic_instance.search(index=ES_INDEX_DELETED_ENTITIES, body=query,
                                         size=500, _source=False, from_=offset)
        log.info(f'Found {len(result["hits"]["hits"])} deleted subscriptions')
        if len(result["hits"]["hits"]) == 0:
            log.info('No more deleted subscriptions to act on')
            break

        ids_rxtx_to_be_deleted = search_if_present(elastic_instance, [hit["_id"] for hit in result['hits']['hits']])

        offset += len(result["hits"]["hits"])
        ids_to_be_deleted.extend([hit["_id"] for hit in result['hits']['hits']])
        # use bulk api to delete all the rx/tx data
        # and the deleted-subscriptions data
        if len(ids_to_be_deleted) >= BULK_SIZE:
            if bulk_delete(elastic_instance, ids_to_be_deleted, ES_INDEX_DELETED_ENTITIES):
                offset = 0
                ids_to_be_deleted.clear()
            else:
                log.error('Failed to delete deleted entities')
                break

        bulk_delete(elastic_instance, ids_rxtx_to_be_deleted, ES_INDEX_RXTX, True)

        time.sleep(0.05)

    if not bulk_delete(elastic_instance, ids_to_be_deleted, ES_INDEX_DELETED_ENTITIES, True):
        log.error('Failed to delete deleted entities index')
        return
    log.info('Done acting on deleted subscriptions')


def search_if_present(elastic_instance, deleted_subscription_or_nuvlaedge_ids: []) -> set:
    """
        Search for deleted subscription or nuvlaedge id in the deleted-subscriptions index

    :param deleted_subscription_or_nuvlaedge_ids: Provide the ids that needs to be used
        for deletion of rx/tx data
    :param elastic_instance: Elasticsearch instance
    :return: The _ids of complete record if present based on the subscription or nuvlaedge id
    """
    ids_rxtx_to_be_deleted = set()
    for ids in deleted_subscription_or_nuvlaedge_ids:
        if ids.startswith('nuvlabox'):
            query = {"query": {"match": {"ne_id": ids}}}
        elif ids.startswith('subscription-config'):
            query = {"query": {"match": {"subs_id": ids}}}
        else:
            continue
        result = get_all_records(elastic_instance, ES_INDEX_RXTX, query)
        for rs in result:
            ids_rxtx_to_be_deleted.add(rs)
    return ids_rxtx_to_be_deleted


def get_all_records(elastic_instance, index, query):
    """
        Fetch all the records for the given index and query
    :param elastic_instance: Elasticsearch instance
    :param index: Index to be searched
    :param query: Query to be used
    :return: All the records
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


def bulk_delete(elastic_instance, ids, index, refresh=False) -> bool:
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


def run_monitoring(elastic_instance):
    """
        Run the monitoring every day at midnight
    :param elastic_instance:
    :return:
    """
    curr_time = datetime.datetime.now()
    delta = datetime.timedelta(days=1)

    time_to_check = datetime.datetime(year=curr_time.year, month=curr_time.month,
                                      day=curr_time.day, hour=14, minute=40, second=0)

    while True:
        curr_time = datetime.datetime.now()
        if curr_time >= time_to_check:
            act_on_deleted_subscriptions(elastic_instance)
            time_to_check += delta
        time.sleep(20)


def main():
    es = elasticsearch.Elasticsearch(hosts=es_hosts())

    def signal_handler(sig, frame):
        act_on_deleted_subscriptions(es)

    signal.signal(signal.SIGUSR1, signal_handler)
    if not es.indices.exists(ES_INDEX_DELETED_ENTITIES):
        es.indices.create(index=ES_INDEX_DELETED_ENTITIES, ignore=400)
    t1 = threading.Thread(target=fetch_deleted_entities, args=(es,))
    t2 = threading.Thread(target=run_monitoring, args=(es,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == '__main__':
    main()
