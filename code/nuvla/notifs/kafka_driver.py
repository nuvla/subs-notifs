"""
Apache Kafka driver for the package.
"""

import json
import os
import uuid
from typing import Union

from nuvla.notifs.log import get_logger
from nuvla.notifs.dictupdater import DictUpdater

from kafka import KafkaConsumer
from kafka import KafkaProducer
from nuvla.notifs.stats.metrics import PROCESS_STATES, INTERVENTION_ERRORS

log = get_logger('kafka-driver')

KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
    KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')


def key_deserializer(key: bytes):
    return str(key.decode())


def value_deserializer(value: Union[bytes, None]):
    if isinstance(value, type(None)):
        return None
    try:
        val = value.decode()
    except Exception as ex:
        log.error('Failed to decode value (%s) : %s', value, ex)
        return ''
    try:
        return json.loads(val)
    except Exception as ex:
        log.error('Failed to parse value (%s) : %s', val, ex)
        return ''


def kafka_consumer(topic, group_id, client_id=None, bootstrap_servers=None,
                   auto_offset_reset='latest'):
    config = dict(
        bootstrap_servers=bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset=auto_offset_reset,
        group_id=group_id,
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer)
    if client_id:
        config['client_id'] = client_id
    consumer = KafkaConsumer(topic, **config)

    log.info("Kafka consumer created.")
    return consumer


def kafka_producer():
    return KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)


class KafkaUpdater(DictUpdater):
    """
    Kafka consumer subscribing to requested topic by name and yields {key: value}
    map of each consumed message.
    """

    def __init__(self, topic_name: str, new_group=True):
        super().__init__()
        self.topic_name = topic_name
        self.new_group = new_group

    def do_yield(self):
        if self.new_group:
            group_name = f'{self.topic_name}_{uuid.uuid4()}'
        else:
            group_name = self.topic_name
        for msg in kafka_consumer(self.topic_name, group_id=group_name,
                                  auto_offset_reset='earliest'):
            if isinstance(msg.value, (type(None), dict)):
                yield {msg.key: msg.value}
            else:
                log.error('Unexpected message key : %s value: %s', msg.key, msg.value)
                PROCESS_STATES.state('error - need intervention')
                INTERVENTION_ERRORS.labels(msg.key, msg.value).inc()
