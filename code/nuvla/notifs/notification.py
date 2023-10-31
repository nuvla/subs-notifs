import json
from typing import List, Dict, Union

from nuvla.notifs.models.event import Event
from nuvla.notifs.kafka_driver import kafka_producer
from nuvla.notifs.log import get_logger
from nuvla.notifs.models.subscription import SubscriptionCfg
from nuvla.notifs.models.resource import Resource

log = get_logger('notification')


class NotificationPublisher:
    def __init__(self):
        self._producer_init()

    def _producer_init(self):
        self.producer = kafka_producer()

    def publish(self, key: str, msg: dict, topic: str):
        future = self.producer.send(topic,
                                    key=bytes(key, encoding='utf8'),
                                    value=bytes(json.dumps(msg),
                                                encoding='utf8'))
        if future:
            log.debug('Kafka publish future value: %s', future.get(5))
            if future.succeeded():
                log.debug('Kafka publish future succeeded: %s', future)
            else:
                log.debug('Kafka publish future failed: %s', future)

    def publish_list(self, msgs: List[Dict], topic: str):
        for msg in msgs:
            log.debug('Publishing to %s: %s', topic, msg)
            self.publish(msg['id'], msg, topic)


class NuvlaEdgeNotification(dict):

    def __init__(self, sc: SubscriptionCfg, metrics: Resource):
        self._sc = sc
        self._m = metrics
        super().__init__({'id': sc['id'],
                          'subs_id': sc['id'],
                          'subs_name': sc['name'],
                          'method_ids': sc['method-ids'],
                          'subs_description': sc['description'],
                          'condition': sc['criteria']['condition'],
                          'condition_value': str(sc['criteria'].get('value', '')),
                          'resource_name': metrics.name(),
                          'resource_description': metrics.description(),
                          'resource_uri': f'edge/{metrics.uuid()}',
                          'timestamp': metrics.timestamp(),
                          'recovery': False})

    def timestamp_as_nuvla_timestamp(self):
        self['timestamp'] = self._m.nuvla_timestamp() or self['timestamp']

    def is_network_metric(self):
        return self._sc.is_network_metric()

    def render(self):
        return self


class NuvlaEdgeNotificationBuilder:

    METRIC_NAME_MAP = {
        'disk': 'NE disk %',
        'ram': 'NE ram %',
        'load': 'NE load %'
    }

    def __init__(self, sc: SubscriptionCfg, metrics: Resource):
        self._n = NuvlaEdgeNotification(sc, metrics)

    def metric_name(self, metric_name: str):
        self._n['metric'] = self.METRIC_NAME_MAP.get(metric_name, metric_name)
        return self

    def value(self, value: Union[int, float]):
        self._n['value'] = value
        return self

    @staticmethod
    def convert_bytes(byte_val: int, cond_val_gb: float) -> str:
        ndigits = 2
        ndigits_max = 10
        if byte_val >= 1024 ** 3:
            ndigits = cond_val_gb >= 1.0 and len(
                str(cond_val_gb).split('.')[1]) or ndigits
            while cond_val_gb >= round(byte_val / 1024 ** 3,
                                       ndigits) and ndigits < ndigits_max:
                ndigits += 1
            return str(round(byte_val / 1024 ** 3, ndigits)) + ' GiB'
        elif byte_val >= 1024 ** 2:
            while cond_val_gb >= round(byte_val / 1024 ** 2,
                                       ndigits) and ndigits < ndigits_max:
                ndigits += 1
            return str(round(byte_val / 1024 ** 2, ndigits)) + ' MiB'
        elif byte_val >= 1024:
            return str(round(byte_val / 1024, ndigits)) + ' KiB'
        else:
            return str(byte_val) + ' bytes'

    def value_rxtx_adjusted(self, value: Union[int, float]):
        self._n['value'] = self.convert_bytes(value,
                                              float(self._n['condition_value']))
        return self

    def recovery(self, recovery: bool):
        self._n['recovery'] = recovery
        return self

    def condition(self, condition: str):
        self._n['condition'] = condition
        return self

    def condition_value(self, condition: str):
        self._n['condition_value'] = condition
        return self

    def timestamp_as_nuvla_timestamp(self):
        self._n.timestamp_as_nuvla_timestamp()
        return self

    def build(self) -> NuvlaEdgeNotification:
        if self._n.is_network_metric():
            self._n['condition_value'] = self._n['condition_value'] + ' GiB'
        return self._n.render()


class BlackboxEventNotification(dict):

    def __init__(self, sc: SubscriptionCfg, event: Event):
        super().__init__({'id': sc['id'],
                          'subs_id': sc['id'],
                          'subs_name': sc['name'],
                          'method_ids': sc['method-ids'],
                          'subs_description': sc['description'],
                          'resource_name': 'blackbox',
                          'resource_description': 'blackbox',
                          'resource_uri': f'api/{event.resource_id()}',
                          'metric': 'content-type',
                          'condition': sc['criteria']['condition'],
                          'condition_value': str(sc['criteria'].get('value', '')),
                          'value': 'true',
                          'timestamp': event.timestamp().split('.')[0] + 'Z',
                          'recovery': True})
