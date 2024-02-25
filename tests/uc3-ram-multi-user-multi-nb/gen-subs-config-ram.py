#!/usr/bin/env python3

import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

TOPIC = 'subscription-config'

subs_conf_1 = {
    'id': 'subscription-config/ram-01',
    'name': 'nb ram',
    'description': 'nb ram',
    'category': 'notification',
    'method-ids': [
        'notification-method/01'
    ],
    'criteria': {
        'metric': 'ram',
        'condition': '>',
        'value': '90',
        'value-type': 'double',
        'kind': 'numeric'
    },
    'resource-type': 'subscription-config',
    'acl': {'owners': ['user/01']},
    'resource-filter': "tags='x86-64'",
    'enabled': True,
    'resource-kind': 'nuvlabox'
}

subs_conf_2 = {
    'id': 'subscription-config/ram-02',
    'name': 'nb ram',
    'description': 'nb ram',
    'category': 'notification',
    'method-ids': [
        'notification-method/02'
    ],
    'criteria': {
        'metric': 'ram',
        'condition': '>',
        'value': '90',
        'value-type': 'double',
        'kind': 'numeric'
    },
    'resource-type': 'subscription-config',
    'acl': {'owners': ['user/02']},
    'resource-filter': "tags='x86-64'",
    'enabled': True,
    'resource-kind': 'nuvlabox'
}

producer.send(TOPIC,
              key=bytes(subs_conf_1['id'], encoding='utf8'),
              value=bytes(json.dumps(subs_conf_1), encoding='utf8'))
print(f'produced: {subs_conf_1["id"]}')

producer.send(TOPIC,
              key=bytes(subs_conf_2['id'], encoding='utf8'),
              value=bytes(json.dumps(subs_conf_2), encoding='utf8'))
print(f'produced: {subs_conf_2["id"]}')

producer.close()
