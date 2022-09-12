import json
import os
import uuid
from typing import Union

from nuvla.notifs.log import get_logger
from nuvla.notifs.updater import DictUpdater

from kafka import KafkaConsumer
from kafka import KafkaProducer

log = get_logger('kafka-driver')

KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
    KAFKA_BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')


def key_deserializer(key: bytes):
    return str(key.decode())


def value_deserializer(value: Union[bytes, None]):
    if isinstance(value, type(None)):
        return None
    return json.loads(value.decode())


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
            yield {msg.key: msg.value}
