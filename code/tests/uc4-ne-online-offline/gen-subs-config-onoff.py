#!/usr/bin/env python3

import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'subscription-config'

subs_conf_1 = {
    'id': 'subscription-config/onoff-01',
    'name': 'ne on/off',
    'description': 'ne on/off',
    'category': 'notification',
    'method-ids': [
        'notification-method/01'
    ],
    'criteria': {
        'metric': 'state',
        'kind': 'boolean',
        'condition': 'no',
        'value': True,
    },
    'resource-type': 'subscription-config',
    'acl': {'owners': ['user/01']},
    'resource-filter': "tags='x86-64'",
    'enabled': True,
    'resource-kind': 'nuvlabox'
}

producer.send(TOPIC,
              key=bytes(subs_conf_1['id'], encoding='utf8'),
              value=bytes(json.dumps(subs_conf_1), encoding='utf8'))
print(f'produced: {subs_conf_1["id"]}')

producer.close()
