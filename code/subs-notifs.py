#!/usr/bin/env python3

from pprint import pformat
import signal
import time
import threading

from nuvla.notifs.db import RxTxDB
from nuvla.notifs.log import get_logger
from nuvla.notifs.subscription import SelfUpdatingDict, \
    SubscriptionConfig, SUBS_CONF_TOPIC
from nuvla.notifs.kafka_driver import KafkaUpdater, kafka_consumer
from nuvla.notifs.notification import NotificationPublisher
from nuvla.notifs.matcher import NuvlaEdgeSubsConfMatcher
from nuvla.notifs.metric import NuvlaEdgeResourceMetrics


log = get_logger('main')


NE_TELEM_TOPIC = 'NB_TELEM_RESOURCES_REKYED_S'
NE_TELEM_GROUP_ID = NE_TELEM_TOPIC
NOTIF_TOPIC = 'NOTIFICATIONS_S'
RESOURCE_KIND_NE = 'nuvlabox'
RESOURCE_KIND_EVENT = 'event'
RESOURCE_KIND_DATARECORD = 'data-record'


def ne_telem_process(msg, sc: SelfUpdatingDict, net_db: RxTxDB,
                     notif_publisher: NotificationPublisher):
    log.info(f'Got message: {msg.value}')
    msg.value['id'] = msg.key
    nerm = NuvlaEdgeResourceMetrics(msg.value)
    net_db.update(nerm)
    log.info(f'net db: {net_db}')
    if sc.get(RESOURCE_KIND_NE):
        nem = NuvlaEdgeSubsConfMatcher(nerm, net_db)
        notifs = nem.match_all(sc[RESOURCE_KIND_NE].values())
        log.info(f'To notify: {notifs}')
        notif_publisher.publish_list(notifs, NOTIF_TOPIC)
    else:
        log.warning('No %s subscriptions. Dropped: %s', RESOURCE_KIND_NE, msg)


def wait_sc_populated(sc: SelfUpdatingDict, resource_kind: str, sleep=5, timeout=None):
    ts_end = time.time() + timeout if timeout else None
    while not sc.get(resource_kind):
        if ts_end and time.time() >= ts_end:
            log.warning('Stopped waiting %s after %s sec', resource_kind, timeout)
            return
        log.debug('waiting for %s in subscription config: %s', resource_kind,
                  list(sc.keys()))
        time.sleep(sleep)


def subs_notif_nuvla_edge_telemetry(sc: SelfUpdatingDict, net_db: RxTxDB):
    wait_sc_populated(sc, RESOURCE_KIND_NE, timeout=60)

    notif_publisher = NotificationPublisher()

    log.info(f'Start NE telemetry processing. Topic: {NE_TELEM_TOPIC}')

    for msg in kafka_consumer(NE_TELEM_TOPIC,
                              group_id=NE_TELEM_GROUP_ID,
                              client_id=NE_TELEM_GROUP_ID + '-1',
                              auto_offset_reset='latest'):
        try:
            ne_telem_process(msg, sc, net_db, notif_publisher)
        except Exception as ex:
            # TODO: print stacktrace
            log.error(f'Failed processing {msg.key} with: {ex}')


def subs_notif_data_record(sc: SelfUpdatingDict):
    wait_sc_populated(sc, RESOURCE_KIND_DATARECORD, timeout=60)
    log.info(f'Starting {RESOURCE_KIND_DATARECORD} processing...')


def subs_notif_event(sc: SelfUpdatingDict):
    wait_sc_populated(sc, RESOURCE_KIND_EVENT, timeout=60)
    log.info(f'Starting {RESOURCE_KIND_EVENT} processing...')


def main():
    def print_sub_conf(signum, trace):
        log.info(f'Subscription configs:\n{pformat(sc)}')

    signal.signal(signal.SIGUSR1, print_sub_conf)

    def print_net_db(signum, trace):
        log.info(f'Network DB:\n{pformat(net_db)}')

    signal.signal(signal.SIGUSR2, print_net_db)

    sc = SelfUpdatingDict('subscription-config',
                          KafkaUpdater(SUBS_CONF_TOPIC),
                          SubscriptionConfig)
    net_db = RxTxDB()

    t1 = threading.Thread(target=subs_notif_nuvla_edge_telemetry, args=(sc, net_db), daemon=True)
    t2 = threading.Thread(target=subs_notif_data_record, args=(sc,), daemon=True)
    t3 = threading.Thread(target=subs_notif_event, args=(sc,), daemon=True)
    t1.start()
    t2.start()
    t3.start()
    while True:
        time.sleep(5)


if __name__ == '__main__':
    main()
