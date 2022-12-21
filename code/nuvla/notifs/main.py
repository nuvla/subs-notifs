"""
Module implementing the main application logic and providing main() procedure.
"""

from pprint import pformat
import os
import signal
import time
import threading
import traceback

from nuvla.notifs.db import RxTxDB, RxTxDriverES
from nuvla.notifs.log import get_logger
from nuvla.notifs.subscription import SelfUpdatingDict, \
    SubscriptionConfig, SUBS_CONF_TOPIC
from nuvla.notifs.kafka_driver import KafkaUpdater, kafka_consumer
from nuvla.notifs.notification import NotificationPublisher
from nuvla.notifs.matcher import NuvlaEdgeSubsConfMatcher
from nuvla.notifs.metric import NuvlaEdgeResourceMetrics


log = get_logger('main')

NE_TELEM_TOPIC = os.environ.get('NE_TELEM_TOPIC', 'NUVLAEDGE_STATUS_REKYED_S')
NE_TELEM_GROUP_ID = NE_TELEM_TOPIC
NOTIF_TOPIC = 'NOTIFICATIONS_S'
RESOURCE_KIND_NE = 'nuvlabox'
RESOURCE_KIND_EVENT = 'event'
RESOURCE_KIND_DATARECORD = 'data-record'
DB_FILENAME='/opt/subs-notifs/subs-notifs.db'
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


def ne_telem_process(msg, subs_confs: SelfUpdatingDict, net_db: RxTxDB,
                     notif_publisher: NotificationPublisher):
    log.info('Got message: %s', msg.value)
    msg.value['id'] = msg.key
    nerm = NuvlaEdgeResourceMetrics(msg.value)
    net_db.update(nerm)
    log.info('net db: %s', net_db)
    if subs_confs.get(RESOURCE_KIND_NE):
        nem = NuvlaEdgeSubsConfMatcher(nerm, net_db)
        notifs = nem.match_all(subs_confs[RESOURCE_KIND_NE].values())
        log.info('To notify: %s', notifs)
        notif_publisher.publish_list(notifs, NOTIF_TOPIC)
    else:
        log.warning('No %s subscriptions. Dropped: %s', RESOURCE_KIND_NE, msg)


def wait_sc_populated(subs_confs: SelfUpdatingDict, resource_kind: str, sleep=5, timeout=None):
    ts_end = time.time() + timeout if timeout else None
    while not subs_confs.get(resource_kind):
        if ts_end and time.time() >= ts_end:
            log.warning('Stopped waiting %s after %s sec', resource_kind, timeout)
            return
        log.debug('waiting for %s in subscription config: %s', resource_kind,
                  list(subs_confs.keys()))
        time.sleep(sleep)


def subs_notif_nuvla_edge_telemetry(subs_confs: SelfUpdatingDict):

    db_driver = RxTxDriverES(hosts=es_hosts())
    db_driver.connect()
    net_db = RxTxDB(db_driver)

    wait_sc_populated(subs_confs, RESOURCE_KIND_NE, timeout=60)

    notif_publisher = NotificationPublisher()

    log.info(f'Start NE telemetry processing. Topic: {NE_TELEM_TOPIC}')

    for msg in kafka_consumer(NE_TELEM_TOPIC,
                              group_id=NE_TELEM_GROUP_ID,
                              client_id=NE_TELEM_GROUP_ID + '-1',
                              auto_offset_reset='latest'):
        try:
            ne_telem_process(msg, subs_confs, net_db, notif_publisher)
        except Exception as ex:
            log.error(''.join(traceback.format_tb(ex.__traceback__)))
            log.error(f'Failed processing {msg.key} with: {ex}')


def subs_notif_data_record(subs_confs: SelfUpdatingDict):
    wait_sc_populated(subs_confs, RESOURCE_KIND_DATARECORD, timeout=60)
    log.info(f'Starting {RESOURCE_KIND_DATARECORD} processing...')


def subs_notif_event(subs_confs: SelfUpdatingDict):
    wait_sc_populated(subs_confs, RESOURCE_KIND_EVENT, timeout=60)
    log.info(f'Starting {RESOURCE_KIND_EVENT} processing...')


def main():
    def print_sub_conf(signum, trace):
        log.info(f'Subscription configs:\n{pformat(subs_confs)}')

    signal.signal(signal.SIGUSR1, print_sub_conf)

    subs_confs = SelfUpdatingDict('subscription-config',
                                 KafkaUpdater(SUBS_CONF_TOPIC),
                                 SubscriptionConfig)

    t1 = threading.Thread(target=subs_notif_nuvla_edge_telemetry,
                          args=(subs_confs,), daemon=True)
    t2 = threading.Thread(target=subs_notif_data_record, args=(subs_confs,),
                          daemon=True)
    t3 = threading.Thread(target=subs_notif_event, args=(subs_confs,),
                          daemon=True)
    t1.start()
    t2.start()
    t3.start()
    while True:
        time.sleep(5)
