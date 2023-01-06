#!/usr/bin/env python3

import datetime
import json
from kafka import KafkaProducer


def ts():
    return datetime.datetime.now().isoformat()[:-3] + 'Z'


producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'NE_TELEM_RESOURCES_REKYED_S'

nb1_m1 = {'id': 'nuvlabox/01',
          'NAME': 'NE #1',
          'DESCRIPTION': 'NE #1',
          'TAGS': [''],
          'RESOURCES': {'RAM': {'used': 8.9, 'capacity': 10, 'topic': 'ram'}},
          'RESOURCES_PREV': {
              'RAM': {'used': 9.1, 'capacity': 10, 'topic': 'ram'}},
          'RESOURCES_RAM_USED_PERS': 85.0,
          'RESOURCES_PREV_RAM_USED_PERS': 91.0,
          'TIMESTAMP': ts(),
          'ACL': {'owners': ['user/01'],
                  'view-data': ['user/02']}}
producer.send(TOPIC,
              key=bytes(nb1_m1['id'], encoding='utf8'),
              value=bytes(json.dumps(nb1_m1), encoding='utf8'))
print(f'produced: {nb1_m1["id"]}')

nb2_m1 = {'id': 'nuvlabox/02',
          'NAME': 'NE #2',
          'DESCRIPTION': 'NE #2',
          'TAGS': ['x86-64'],
          'RESOURCES': {'RAM': {'used': 8.9, 'capacity': 10, 'topic': 'ram'}},
          'RESOURCES_PREV': {
              'RAM': {'used': 9.2, 'capacity': 10, 'topic': 'ram'}},
          'RESOURCES_RAM_ram_PERS': 89.0,
          'RESOURCES_PREV_RAM_ram_PERS': 92.0,
          'TIMESTAMP': ts(),
          'ACL': {'owners': ['user/02'],
                  'view-data': ['user/01']}}
producer.send(TOPIC,
              key=bytes(nb2_m1['id'], encoding='utf8'),
              value=bytes(json.dumps(nb2_m1), encoding='utf8'))
print(f'produced: {nb2_m1["id"]}')

producer.close()
