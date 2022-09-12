#!/usr/bin/env python3

import datetime
import json
from kafka import KafkaProducer


def ts():
    return datetime.datetime.now().isoformat()[:-3] + 'Z'


producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'NB_TELEM_RESOURCES_REKYED_S'

owners = ['user/01', 'user/02']

NB1_ID = 'nuvlabox/01'
NB2_ID = 'nuvlabox/02'

nb1_m2 = {'id': NB1_ID,
          'NAME': 'NE #1',
          'DESCRIPTION': 'NE #1',
          'TAGS': [],
          'RESOURCES': {'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'}},
          'RESOURCES_PREV': {
              'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'}},
          'RESOURCES_CPU_LOAD_PERS': 91.0,
          'RESOURCES_PREV_CPU_LOAD_PERS': 91.0,
          'TIMESTAMP': ts(),
          'ACL': {'owners': owners,
                  'view-data': []}}
producer.send(TOPIC,
              key=bytes(NB1_ID, encoding='utf8'),
              value=bytes(json.dumps(nb1_m2), encoding='utf8'))
print(f'produced: {NB1_ID}')

nb2_m2 = {'id': NB2_ID,
          'NAME': 'NE #2',
          'DESCRIPTION': 'NE #2',
          'TAGS': ['x86-64'],
          'RESOURCES': {'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'}},
          'RESOURCES_PREV': {
              'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'}},
          'RESOURCES_CPU_LOAD_PERS': 91.0,
          'RESOURCES_PREV_CPU_LOAD_PERS': 91.0,
          'TIMESTAMP': ts(),
          'ACL': {'owners': owners,
                  'view-data': []}}
producer.send(TOPIC,
              key=bytes(NB2_ID, encoding='utf8'),
              value=bytes(json.dumps(nb2_m2), encoding='utf8'))
print(f'produced: {NB2_ID}')

producer.close()
