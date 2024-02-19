import signal
import threading
import time

import schedule
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

from nuvla.notifs.common import es_hosts, ES_INDEX_RXTX, ES_INDEX_DELETED_ENTITIES, \
    KAFKA_TOPIC_SUBS_CONFIG, KAFKA_TOPIC_NUVLAEDGES, KAFKA_BOOTSTRAP_SERVERS, \
    prometheus_exporter_port
from nuvla.notifs.db.driver import es_get_all_records, es_delete_bulk
from nuvla.notifs.log import get_logger
from prometheus_client import start_http_server, REGISTRY, PROCESS_COLLECTOR, \
    ProcessCollector
import nuvla.notifs.stats.metrics as metrics

metrics.namespace = 'subs_notifs_monitoring'
log = get_logger('monitoring')

BULK_SIZE = 1000
DEFAULT_PROMETHEUS_EXPORTER_PORT = 9139


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


def act_on_deleted_entities(es: Elasticsearch):
    """
         1. Fetch all deleted subscriptions
         2. For each deleted subscription, delete all the rx/tx data
         3. Delete the deleted subscription from the deleted-subscriptions index
    :param es:
    :return:
    """
    log.info('Acting on deleted entities')
    query = {"query": {"match_all": {}}}
    offset = 0
    ids_to_be_deleted = []
    while True:
        result = es.search(index=ES_INDEX_DELETED_ENTITIES, body=query,
                           size=500, _source=False, from_=offset)
        log.info(f'Found {len(result["hits"]["hits"])} deleted entities')
        if len(result["hits"]["hits"]) == 0:
            log.info('No more deleted entities to act on')
            break

        ids_rxtx_to_be_deleted = search_if_present(es, [hit["_id"] for hit in result['hits']['hits']])

        offset += len(result["hits"]["hits"])
        ids_to_be_deleted.extend([hit["_id"] for hit in result['hits']['hits']])
        # use bulk api to delete all the rx/tx data
        # and the deleted-subscriptions data
        if len(ids_to_be_deleted) >= BULK_SIZE:
            if es_delete_bulk(es, ids_to_be_deleted, ES_INDEX_DELETED_ENTITIES, True):
                offset = 0
                ids_to_be_deleted.clear()
            else:
                log.error('Failed to delete deleted entities')
                break

        es_delete_bulk(es, ids_rxtx_to_be_deleted, ES_INDEX_RXTX, True)

        time.sleep(0.05)

    if not es_delete_bulk(es, ids_to_be_deleted, ES_INDEX_DELETED_ENTITIES, True):
        log.error('Failed to delete deleted entities index')
        return
    log.info('Done acting on deleted entities')


def search_if_present(es: Elasticsearch, deleted_entities_ids: []) -> set:
    """
    Search for deleted entities IDs in the corresponding index.

    :param deleted_entities_ids: IDs that need to be used for deletion of rx/tx data
    :param es: Elasticsearch instance
    :return: The set of IDs of the DB records
    """
    ids_rxtx_to_be_deleted = set()
    for ids in deleted_entities_ids:
        if ids.startswith('nuvlabox'):
            query = {"query": {"match": {"ne_id": ids}}}
        elif ids.startswith('subscription-config'):
            query = {"query": {"match": {"subs_id": ids}}}
        else:
            continue
        result = es_get_all_records(es, ES_INDEX_RXTX, query)
        for rs in result:
            ids_rxtx_to_be_deleted.add(rs)
    return ids_rxtx_to_be_deleted


def schedule_entities_deletion(es: Elasticsearch):
    """
    Schedule and run deletion of the data of deleted entities.

    :param es: Elasticsearch instance
    """

    schedule.every().day.at('00:00').do(act_on_deleted_entities, es=es)

    while True:
        schedule.run_pending()
        time.sleep(1)


def es_instance():
    es = Elasticsearch(hosts=es_hosts())
    if not es.indices.exists(ES_INDEX_DELETED_ENTITIES):
        es.indices.create(index=ES_INDEX_DELETED_ENTITIES, ignore=400)
    return es


def install_signal_handler(es: Elasticsearch):
    def signal_handler(sig, frame):
        act_on_deleted_entities(es)

    signal.signal(signal.SIGUSR1, signal_handler)


def main():
    es = es_instance()
    REGISTRY.unregister(PROCESS_COLLECTOR)
    ProcessCollector(namespace=metrics.namespace)
    install_signal_handler(es)

    start_http_server(prometheus_exporter_port(DEFAULT_PROMETHEUS_EXPORTER_PORT))
    t1 = threading.Thread(target=fetch_deleted_entities, args=(es,))
    t2 = threading.Thread(target=schedule_entities_deletion, args=(es,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()
