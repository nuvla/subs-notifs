#!/usr/bin/env python3

import json
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

topic = 'NE_TELEM_RESOURCES_REKYED_S'

_id = 'nuvlabox/01'
user = 'user/00000000-0000-0000-0000-000000000000'

nb_status = {'id': _id,
             "NAME": "Nuvlabox TBL Münchwilen AG Zürcherstrasse #1",
             "DESCRIPTION": "None - self-registration number 220171415421241",
             "TAGS": ['arch=x86-64'],
             "NETWORK": {"default_gw": "eth0"},
             "ONLINE": True,
             "ONLINE_PREV": True,
             "RESOURCES": {"CPU": {"load": 5.52, "capacity": 4, "topic": "cpu"},
                           "RAM": {"used": 713, "capacity": 925, "topic": "ram"},
                           "DISKS": [{"used": 9, "capacity": 15, "device": "mmcblk0p2"}],
                           "NET-STATS": [
                               {"interface": "eth0",
                                "bytes-transmitted": 0,
                                "bytes-received": 0},
                               {"interface": "lo",
                                "bytes-transmitted": 63742086112,
                                "bytes-received": 63742086112
                               }]},
             "RESOURCES_PREV": {
                "CPU": {"load": 5.56, "capacity": 4, "topic": "cpu"},
                "RAM": {"used": 712, "capacity": 925, "topic": "ram"},
                "DISKS": [{"used": 9, "capacity": 15, "device": "mmcblk0p2"}]},
             "RESOURCES_CPU_LOAD_PERS": 138.0,
             "RESOURCES_RAM_USED_PERS": 77,
             "RESOURCES_DISK1_USED_PERS": 60,
             "RESOURCES_PREV_CPU_LOAD_PERS": 139.0,
             "RESOURCES_PREV_RAM_USED_PERS": 76,
             "RESOURCES_PREV_DISK1_USED_PERS": 60,
             "TIMESTAMP": "2022-08-02T15:21:46Z",
             "ACL": {"owners": [user],
                     "view-data": ["group/elektron",
                                   "infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7",
                                   "nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd"]}}

future = producer.send(topic,
                       key=bytes(_id, encoding='utf8'),
                       value=bytes(json.dumps(nb_status), encoding='utf8'))
print(f'produced: {_id}')

producer.close()