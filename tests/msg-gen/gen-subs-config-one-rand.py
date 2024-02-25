#!/usr/bin/env python3

import json
import uuid
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic = 'subscription-config'

_id = f'subscription-config/{str(uuid.uuid4())}'
user = 'user/e5be6cc1-6942-4991-acf4-7d5cf95b9cc6'

subs_conf = {
    'description': 'nb load',
    'category': 'notification',
    'method-ids': [
        'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
    ],
    'updated': '2022-07-30T19:42:54.246Z',
    'name': 'nb load',
    'criteria': {
        'metric': 'load',
        'kind': 'numeric',
        'condition': '>',
        'value': '1'
    },
    'created': '2022-07-30T18:07:35.440Z',
    'updated-by': user,
    'created-by': user,
    'id': _id,
    'resource-type': 'subscription-config',
    'acl': {
        'edit-data': [
            user
        ],
        'owners': [
            user
        ],
        'view-acl': [
            'group/nuvla-admin'
        ],
        'delete': [
            'group/nuvla-admin'
        ],
        'view-meta': [
            'group/nuvla-admin'
        ],
        'edit-acl': [
            'group/nuvla-admin'
        ],
        'view-data': [
            'group/nuvla-admin'
        ],
        'manage': [
            'group/nuvla-admin'
        ],
        'edit-meta': [
            'group/nuvla-admin'
        ]
    },
    'operations': [
        {
            'rel': 'edit',
            'href': _id
        },
        {
            'rel': 'delete',
            'href': _id
        },
        {
            'rel': 'enable',
            'href': f'{_id}/enable'
        },
        {
            'rel': 'disable',
            'href': f'{_id}/disable'
        },
        {
            'rel': 'set-notif-method-ids',
            'href': f'{_id}/set-notif-method-ids'
        }
    ],
    'resource-filter': "tags='arch=x86-64'",
    'enabled': True,
    'resource-kind': 'nuvlabox'
}

future = producer.send(topic,
                       key=bytes(_id, encoding='utf8'),
                       value=bytes(json.dumps(subs_conf), encoding='utf8'))
print(f'produced: {_id}')

producer.close()