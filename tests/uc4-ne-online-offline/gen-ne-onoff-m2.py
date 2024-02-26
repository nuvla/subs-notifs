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
          'TAGS': ['x86-64'],
          'ONLINE': False,
          'ONLINE_PREV': True,
          'RESOURCES': {},
          'RESOURCES_PREV': {},
          'TIMESTAMP': ts(),
          'ACL': {'owners': ['user/01'],
                  'view-data': ['user/02']}}
producer.send(TOPIC,
              key=bytes(nb1_m1['id'], encoding='utf8'),
              value=bytes(json.dumps(nb1_m1), encoding='utf8'))
print(f'produced: {nb1_m1["id"]}')

producer.close()
