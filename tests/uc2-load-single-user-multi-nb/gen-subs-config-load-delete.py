#!/usr/bin/env python3

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'subscription-config'

_id = 'subscription-config/load-01'
producer.send(TOPIC, key=bytes(_id, encoding='utf8'), value=None)
print(f'produced: tombstone for {_id}')

producer.close()
