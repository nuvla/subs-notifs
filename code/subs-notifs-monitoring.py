#!/usr/bin/env python3

from kafka import KafkaConsumer
import os
import elasticsearch
from elasticsearch.helpers import bulk
import threading
import time, datetime
from nuvla.notifs.log import get_logger
import signal

kafka_topic_name = 'subscription-config'
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
    KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')
ES_HOSTS = [{'host': 'es', 'port': 9200}]
es_index_deleted_subscriptions = 'deleted-subscriptions'
es_index_nuvlabox_rx_tx = 'subsnotifs-rxtx'
log = get_logger('monitoring')


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


def fetch_deleted_subscriptions(elastic_instance):
    config = dict(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest'
    )
    kafka_consumer = KafkaConsumer(kafka_topic_name, **config)
    for msg in kafka_consumer:
        log.debug(f'{msg.key} {msg.value}')
        if msg.value is None:
            log.info(f'{msg.key} was deleted')
            created = elastic_instance.index(index=es_index_deleted_subscriptions, body={}, id=msg.key)
            log.info(f'Created deleted subscription {created["_id"]}')


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
    while True:
        result = elastic_instance.search(index=es_index_deleted_subscriptions, body=query,
                                         size=500, _source=False)
        log.info(f'Found {len(result["hits"]["hits"])} deleted subscriptions')
        if len(result["hits"]["hits"]) == 0:
            log.info('No more deleted subscriptions to act on')
            break

        ids_to_be_deleted = []
        ids_rxtx_to_be_deleted = set()
        for hit in result['hits']['hits']:
            ids_to_be_deleted.append(hit["_id"])
            rxtx_query = {"query": {"match": {"subs_id": hit["_id"]}}}
            try:
                rxtx_result = elastic_instance.search(index=es_index_nuvlabox_rx_tx, body=rxtx_query, _source=False)
            except Exception as ex:
                log.error(f'Failed to fetch rx/tx data for {hit["_id"]}: {ex}')
                continue
            for rxtx_hit in rxtx_result['hits']['hits']:
                ids_rxtx_to_be_deleted.add(rxtx_hit["_id"])

        # use bulk api to delete all the rx/tx data
        # and the deleted-subscriptions data
        actions = ({
            '_op_type': 'delete',
            '_id': ids
        } for ids in ids_to_be_deleted)

        try:
            bulk(client=elastic_instance, actions=actions, index=es_index_deleted_subscriptions, refresh=True)
        except Exception as ex:
            log.error(f'Exception in bulk delete in deleted-subscriptions: {ex}')

        actions = ({
            '_op_type': 'delete',
            '_id': ids
        } for ids in ids_rxtx_to_be_deleted)

        try:
            bulk(client=elastic_instance, actions=actions, index=es_index_nuvlabox_rx_tx, refresh=True)
        except Exception as ex:
            log.error(f'Exception in bulk delete in subsnotifs-rxtx: {ex}')

        time.sleep(0.05)


def run_monitoring(elastic_instance):
    """
        Run the monitoring every day at midnight
    :param elastic_instance:
    :return:
    """
    curr_time = datetime.datetime.now()
    delta = datetime.timedelta(days=1)

    time_to_check = datetime.datetime(year=curr_time.year, month=curr_time.month,
                                      day=curr_time.day, hour=0, minute=0, second=0)

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
    if not es.indices.exists(es_index_deleted_subscriptions):
        es.indices.create(index=es_index_deleted_subscriptions, ignore=400)
    t1 = threading.Thread(target=fetch_deleted_subscriptions, args=(es,))
    t2 = threading.Thread(target=run_monitoring, args=(es,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()


if __name__ == '__main__':
    main()
