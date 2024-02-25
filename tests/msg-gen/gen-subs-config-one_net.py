#!/usr/bin/env python3

import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic = 'subscription-config'

_id = 'subscription-config/11111111-2222-3333-4444-555555555555'
user_owner = 'user/00000000-0000-0000-0000-000000000000'
user_view = 'user/11111111-1111-1111-1111-111111111111'

subs_conf = {
    'description': 'nb network cumulative Rx over 30 days',
    'category': 'notification',
    'method-ids': [
        'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
    ],
    'updated': '2022-07-30T19:42:54.246Z',
    'name': 'nb network Rx',
    'criteria': {
        'metric': 'RxGb',
        'condition': '>',
        'value': '5',
        'kind': 'numeric',
        'window': 'monthly',
        'dev-name': 'eth0'
    },
    'created': '2022-07-30T18:07:35.440Z',
    'updated-by': user_owner,
    'created-by': user_owner,
    'id': _id,
    'resource-type': 'subscription-config',
    'acl': {
        'edit-data': [
            'group/nuvla-admin'
        ],
        'owners': [
            user_owner
        ],
        'view-acl': [
            user_view
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