"""
Module implementing the main application logic and providing main() procedure.
"""

from pprint import pformat
from datetime import datetime
import os
import signal
import socket
import time
import threading
import traceback
from typing import List

from nuvla.notifs.db.driver import RxTxDB, RxTxDriverES
from nuvla.notifs.log import get_logger
from nuvla.notifs.models.subscription import SelfUpdatingSubsCfgs, \
    SubscriptionCfg, SUBS_CONF_TOPIC, RESOURCE_KIND_NE, \
    RESOURCE_KIND_DATARECORD, RESOURCE_KIND_EVENT
from nuvla.notifs.kafka_driver import KafkaUpdater, kafka_consumer
from nuvla.notifs.notification import NotificationPublisher
from nuvla.notifs.matcher import NuvlaEdgeSubsCfgMatcher, \
    TaggedResourceNetworkSubsCfgMatcher, EventSubsCfgMatcher
from nuvla.notifs.models.metric import NuvlaEdgeMetrics
from nuvla.notifs.models.event import Event

log = get_logger('main')

NE_TELEM_TOPIC = os.environ.get('NE_TELEM_TOPIC', 'NUVLAEDGE_STATUS_REKYED_S')
NE_TELEM_GROUP_ID = NE_TELEM_TOPIC
EVENTS_TOPIC = os.environ.get('EVENTS_TOPIC', 'event')
EVENTS_GROUP_ID = EVENTS_TOPIC
NOTIF_TOPIC = 'NOTIFICATIONS_S'
DB_FILENAME = '/opt/subs-notifs/subs-notifs.db'
ES_HOSTS = [{'host': 'es', 'port': 9200}]


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


def consumer_id(base='consumer') -> str:
    return f'{base}-{socket.gethostname()}'


def populate_ne_net_db(net_db: RxTxDB, ne_metrics: NuvlaEdgeMetrics,
                       subs_cfgs: List[SubscriptionCfg]):
    """
    Populate network RxTx database with the network related metrics coming with
    NuvlaEdge telemetry.

    :param ne_metrics: NuvlaEdge telemetry metrics
    :param net_db: network DB
    :param subs_cfgs: NuvlaEdge subscription notification configurations
    """
    subs_cfgs_matched = TaggedResourceNetworkSubsCfgMatcher() \
        .resource_subscriptions(ne_metrics, subs_cfgs)
    net_db.update(ne_metrics, subs_cfgs_matched)


def ne_telem_process(metrics: dict, subs_cfgs: List[SubscriptionCfg],
                     net_db: RxTxDB, notif_publisher: NotificationPublisher):
    log.info('Got NE metrics: %s', metrics)

    nerm = NuvlaEdgeMetrics(metrics)

    populate_ne_net_db(net_db, nerm, subs_cfgs)

    notifs = NuvlaEdgeSubsCfgMatcher(nerm, net_db).match_all(subs_cfgs)
    log.info('To notify: %s', notifs)
    notif_publisher.publish_list(notifs, NOTIF_TOPIC)


def wait_sc_populated(subs_cfgs: SelfUpdatingSubsCfgs, resource_kind: str,
                      sleep=5, timeout=None):
    ts_end = time.time() + timeout if timeout else None
    while not subs_cfgs.get(resource_kind):
        if ts_end and time.time() >= ts_end:
            log.warning('Stopped waiting %s after %s sec', resource_kind,
                        timeout)
            return
        log.debug('waiting for %s in subscription config: %s', resource_kind,
                  list(subs_cfgs.keys()))
        time.sleep(sleep)


def subs_notif_nuvla_edge_telemetry(subs_cfgs: SelfUpdatingSubsCfgs):
    db_driver = RxTxDriverES(hosts=es_hosts())
    db_driver.connect()
    net_db = RxTxDB(db_driver)

    wait_sc_populated(subs_cfgs, RESOURCE_KIND_NE, timeout=60)

    notif_publisher = NotificationPublisher()

    log.info(f'Start NE telemetry processing. Topic: {NE_TELEM_TOPIC}')

    for msg in kafka_consumer(NE_TELEM_TOPIC,
                              group_id=NE_TELEM_GROUP_ID,
                              client_id=consumer_id(NE_TELEM_GROUP_ID),
                              auto_offset_reset='latest'):
        try:
            msg.value['id'] = msg.key
            ne_telem_process(msg.value,
                             list(subs_cfgs.get(RESOURCE_KIND_NE, {}).values()),
                             net_db, notif_publisher)
        except Exception as ex:
            log.error(''.join(traceback.format_tb(ex.__traceback__)))
            log.error('Failed processing %s with: %s', msg.key, ex)


def subs_notif_data_record(subs_cfgs: SelfUpdatingSubsCfgs):
    wait_sc_populated(subs_cfgs, RESOURCE_KIND_DATARECORD, timeout=60)
    log.info(f'Starting {RESOURCE_KIND_DATARECORD} processing...')


def events_process(event: dict, subs_cfgs: List[SubscriptionCfg],
                   notif_publisher: NotificationPublisher):
    log.info('Got event: %s', event)

    e = Event(event)

    notifs = EventSubsCfgMatcher(e).match_blackbox(subs_cfgs)
    log.info('To notify: %s', notifs)
    notif_publisher.publish_list(notifs, NOTIF_TOPIC)


def subs_notif_event(subs_cfgs: SelfUpdatingSubsCfgs):
    wait_sc_populated(subs_cfgs, RESOURCE_KIND_EVENT, timeout=60)
    log.info(f'Starting {RESOURCE_KIND_EVENT} processing...')

    notif_publisher = NotificationPublisher()

    log.info(f'Start Event telemetry processing. Topic: {EVENTS_TOPIC}')

    for msg in kafka_consumer(EVENTS_TOPIC,
                              group_id=EVENTS_GROUP_ID,
                              client_id=consumer_id(EVENTS_GROUP_ID),
                              auto_offset_reset='latest'):
        try:
            msg.value['id'] = msg.key
            events_process(msg.value,
                           list(subs_cfgs.get(RESOURCE_KIND_EVENT, {}).values()),
                           notif_publisher)
        except Exception as ex:
            log.error(''.join(traceback.format_tb(ex.__traceback__)))
            log.error('Failed processing %s with: %s', msg.key, ex)


def local_time():
    while True:
        log.info('Current time: %s', datetime.now().isoformat())
        time.sleep(5)


def main():
    def print_sub_conf(signum, trace):
        log.info(f'Subscription configs:\n{pformat(dyn_subs_cfgs)}')

    signal.signal(signal.SIGUSR1, print_sub_conf)

    dyn_subs_cfgs = SelfUpdatingSubsCfgs('subscription-config',
                                         KafkaUpdater(SUBS_CONF_TOPIC))

    t1 = threading.Thread(target=subs_notif_nuvla_edge_telemetry,
                          args=(dyn_subs_cfgs,), daemon=True)
    t2 = threading.Thread(target=subs_notif_data_record, args=(dyn_subs_cfgs,),
                          daemon=True)
    t3 = threading.Thread(target=subs_notif_event, args=(dyn_subs_cfgs,),
                          daemon=True)
    t4 = threading.Thread(target=local_time, daemon=True)
    t1.start()
    t2.start()
    t3.start()
    t4.start()
    while True:
        time.sleep(5)
