import unittest

from nuvla.notifs.matcher import NuvlaEdgeSubsConfMatcher
from nuvla.notifs.metric import NuvlaEdgeResourceMetrics
from nuvla.notifs.notification import NuvlaEdgeNotificationBuilder
from nuvla.notifs.subscription import SubscriptionConfig

user = 'user/00000000-0000-0000-0000-000000000000'


class TestNuvlaEdgeNotificationBuilder(unittest.TestCase):

    def test_builder_numeric_network(self):
        sc = SubscriptionConfig({
            'id': 'subscription-config/11111111-2222-3333-4444-555555555555',
            'name': 'nb Rx',
            'description': 'nb network cumulative Rx over 30 days',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'acl': {
                'owners': [
                    user
                ]},
            'criteria': {
                'metric': 'RxGb',
                'condition': '>',
                'value': '5',
                'kind': 'numeric',
                'window': 'monthly',
                'dev-name': 'eth0'
            }})
        metrics = NuvlaEdgeResourceMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'NETWORK': {'default_gw': 'eth0'},
             'ONLINE': True,
             'ONLINE_PREV': True,
             'RESOURCES': {'CPU': {'load': 5.52, 'capacity': 4, 'topic': 'cpu'},
                           'RAM': {'used': 713, 'capacity': 925,
                                   'topic': 'ram'},
                           'DISKS': [{'used': 9, 'capacity': 15,
                                      'device': 'mmcblk0p2'}],
                           'NET-STATS': [
                               {'interface': 'eth0',
                                'bytes-transmitted': 0,
                                'bytes-received': 0},
                               {'interface': 'lo',
                                'bytes-transmitted': 63742086112,
                                'bytes-received': 63742086112
                                }]},
             'RESOURCES_PREV': {
                 'CPU': {'load': 5.56, 'capacity': 4, 'topic': 'cpu'},
                 'RAM': {'used': 712, 'capacity': 925, 'topic': 'ram'},
                 'DISKS': [{'used': 9, 'capacity': 15, 'device': 'mmcblk0p2'}]},
             'RESOURCES_CPU_LOAD_PERS': 138.0,
             'RESOURCES_RAM_USED_PERS': 77,
             'RESOURCES_DISK1_USED_PERS': 60,
             'RESOURCES_PREV_CPU_LOAD_PERS': 139.0,
             'RESOURCES_PREV_RAM_USED_PERS': 76,
             'RESOURCES_PREV_DISK1_USED_PERS': 60,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': [user],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        notif_builder = NuvlaEdgeNotificationBuilder(sc, metrics)
        notif = notif_builder.name('RxGB').value(123).recovery(True).build()
        assert {'id': 'subscription-config/11111111-2222-3333-4444-555555555555',
                'subs_id': 'subscription-config/11111111-2222-3333-4444-555555555555',
                'subs_name': 'nb Rx',
                'method_ids': ['notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'],
                'subs_description': 'nb network cumulative Rx over 30 days',
                'condition': '>',
                'condition_value': '5',
                'resource_name': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
                'resource_description': 'None - self-registration number 220171415421241',
                'resource_uri': 'edge/01',
                'timestamp': '2022-08-02T15:21:46Z',
                'recovery': True,
                'metric': 'RxGB',
                'value': 123} == notif

    def test_builder_ne_onoff(self):
        sc = SubscriptionConfig({
            'id': 'subscription-config/01',
            'name': 'ne on/off',
            'description': 'ne on/off',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'acl': {
                'owners': [
                    user
                ]},
            'criteria': {
                'metric': 'state',
                'kind': 'boolean',
                'condition': 'no',
                'value': 'true'
            }})
        metrics = NuvlaEdgeResourceMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'ONLINE': True,
             'ONLINE_PREV': False,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': [user],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        nescm = NuvlaEdgeSubsConfMatcher(metrics)
        res = nescm.notif_build_online(sc, nescm.MATCHED_RECOVERY)
        assert res['method_ids'] == sc.get('method-ids')
        assert res['subs_description'] == sc.get('description')
        assert res['metric'] == 'NE online'
        assert res['condition'] == 'true'
        assert res['recovery'] is True
        assert res['value'] == ''
        assert res['condition_value'] == ''

        metrics = NuvlaEdgeResourceMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'ONLINE': False,
             'ONLINE_PREV': True,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': [user],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        nescm = NuvlaEdgeSubsConfMatcher(metrics)
        res = nescm.notif_build_online(sc, nescm.MATCHED)
        assert res['method_ids'] == sc.get('method-ids')
        assert res['subs_description'] == sc.get('description')
        assert res['metric'] == 'NE online'
        assert res['condition'] == 'false'
        assert res['recovery'] is False
        assert res['value'] == ''
        assert res['condition_value'] == ''
