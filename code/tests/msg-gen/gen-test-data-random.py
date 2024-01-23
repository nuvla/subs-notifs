"""
    Generate random documents of rx/tx telemetry
    in elastic search for testing purposes
"""
import time

import elasticsearch
import threading
import logging
import sys
from kafka import KafkaProducer

INDEX_NAME = 'subsnotifs-rxtx'
HOSTS = [{'host': 'localhost', 'port': 9200}]

user_owner = 'user/bc62866b-2b64-47d9-b4dc-84670bfbfd8b'
user_view = 'user/150f31e8-44e2-4e92-89d0-9c8695c845a4'
kafka_topic = 'subscription-config'
subs_conf = {
    'description': 'nb network cumulative Rx over 30 days',
    'category': 'notification',
    'method-ids': [
        'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
    ],
    'updated': '2024-01-19T20:31:54.246Z',
    'created': '2024-01-02T18:07:35.440Z',
    'updated-by': {user_owner},
    'created-by': user_owner,
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
    'resource-filter': "tags='arch=x86-64'",
    'enabled': True,
    'resource-kind': 'nuvlabox'
}


def create_rx_tx_data():
    es = elasticsearch.Elasticsearch(hosts=HOSTS)

    es.indices.delete(index=INDEX_NAME, ignore=[400, 404])

    es.indices.create(index=INDEX_NAME, ignore=400)

    body = {
        'properties': {
            'subs_id': {
                'type': 'keyword'
            },
            'ne_id': {
                'type': 'keyword'
            },
            'iface': {
                'type': 'keyword'
            },
            'kind': {
                'type': 'keyword'
            }
        }
    }
    es.indices.put_mapping(index=INDEX_NAME, body=body)

    for i in range(1000):
        doc = {
            'subs_id': f'subscription-config/8a120eec-8a73-4318-bf30-2aeeadc{i:03d}',
            'ne_id': f'nuvlabox/8a120eec-8a73-4318-bf30-2aeeadc{i:03d}',
            'iface': 'eth0',
            'kind': 'rx' if i % 2 == 0 else 'tx',
        }
        es.index(index=INDEX_NAME, body=doc, refresh=False)

    es.indices.refresh(index=INDEX_NAME)


def create_subs_notif_data():
    producer = KafkaProducer(bootstrap_servers='0.0.0.0:9092')
    # create random configuration
    for i in range(1000):
        subs_conf['id'] = f'subscription-config/8a120eec-8a73-4318-bf30-2aeeadc{i:03d}'
        if i % 2 == 0:
            subs_conf['criteria'] = {
                "metric": "network-rx",
                "kind": "numeric",
                "reset-interval": "month",
                "reset-start-date": 1,
                "condition": ">",
                "value": "0.1"
            }
        else:
            subs_conf['criteria'] = {
                "metric": "network-tx",
                "kind": "numeric",
                "reset-interval": "month",
                "reset-start-date": 1,
                "condition": ">",
                "value": "0.1"
            }
        future = producer.send(kafka_topic,
                               key=bytes(subs_conf['id'], encoding='utf8'),
                               value=bytes(str(subs_conf), encoding='utf8'))
        try:
            future.get(timeout=10)
        except Exception as ex:
            print(f'Exception: {ex}')
            break
        print(f'produced: {subs_conf["id"]}')
        time.sleep(0.05)
    producer.flush()

    # now send messages for deletion of subscription config
    for i in range(0, 500):
        _id = f'subscription-config/8a120eec-8a73-4318-bf30-2aeeadc{i:03d}'
        producer.send(kafka_topic,
                      key=bytes(_id, encoding='utf8'))
        print(f'produced: {_id} with no value')
        time.sleep(0.05)
    producer.flush()
    producer.close()


def handle_logging():
    # Create a logger for 'kafka.client', 'kafka.conn', 'kafka.producer', and 'kafka.protocol'
    loggers = ['kafka.client', 'kafka.conn', 'kafka.producer', 'kafka.protocol']
    for logger_name in loggers:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)

        # Create a file handler
        handler = logging.FileHandler('kafka.log')
        handler.setLevel(logging.DEBUG)

        # Create a logging format
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Add the formatter to the handler
        handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(handler)


def main():
    handle_logging()
    t1 = threading.Thread(target=create_rx_tx_data)
    t1.start()
    t2 = threading.Thread(target=create_subs_notif_data)
    t2.start()
    t1.join()
    t2.join()


if __name__ == "__main__":
    main()
