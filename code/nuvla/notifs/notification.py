import json
from typing import List, Dict, Union

from nuvla.notifs.kafka_driver import kafka_producer
from nuvla.notifs.log import get_logger
from nuvla.notifs.subscription import SubscriptionConfig
from nuvla.notifs.metric import ResourceMetrics

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


class NuvlaEdgeNotificationBuilder(dict):
    def __init__(self, sc: SubscriptionConfig, metrics: ResourceMetrics):
        super().__init__({'id': sc['id'],
                          'subs_id': sc['id'],
                          'subs_name': sc['name'],
                          'method_ids': sc['method-ids'],
                          'subs_description': sc['description'],
                          'condition': sc['criteria']['condition'],
                          'condition_value': str(sc['criteria']['value']),
                          'resource_name': metrics.name(),
                          'resource_description': metrics.description(),
                          'resource_uri': f'edge/{metrics.uuid()}',
                          'timestamp': metrics.timestamp(),
                          'recovery': False})

    def name(self, name: str):
        self['metric'] = name
        return self

    def value(self, value: Union[int, float]):
        self['value'] = value
        return self

    def recovery(self, recovery: bool):
        self['recovery'] = recovery
        return self

    def condition(self, condition: str):
        self['condition'] = condition
        return self

    def condition_value(self, condition: str):
        self['condition_value'] = condition
        return self

    def build(self):
        return self
