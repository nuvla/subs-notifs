import json
from typing import List, Dict, Union

from nuvla.notifs.event import Event
from nuvla.notifs.kafka_driver import kafka_producer
from nuvla.notifs.log import get_logger
from nuvla.notifs.subscription import SubscriptionCfg
from nuvla.notifs.resource import Resource

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
                          'nuvla_timestamp': metrics.nuvla_timestamp(),
                          'recovery': False})

    def timestamp_as_nuvla_timestamp(self):
        self['timestamp'] = self.get('nuvla_timestamp', self['timestamp'])

    def render(self):
        res = self
        res.pop('nuvla_timestamp')
        return res


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
