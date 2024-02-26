import unittest

from nuvla.notifs.matching.ne_telem import NuvlaEdgeSubsCfgMatcher
from nuvla.notifs.models.metric import NuvlaEdgeMetrics
from nuvla.notifs.notification import NuvlaEdgeNotificationBuilder, \
    NuvlaEdgeNotification, to_timestamp_utc, \
    AppPublishedDeploymentsUpdateNotification
from nuvla.notifs.models.subscription import SubscriptionCfg
from nuvla.notifs.db.driver import gb_to_bytes

user = 'user/00000000-0000-0000-0000-000000000000'


class TestNuvlaEdgeNotificationBuilder(unittest.TestCase):

    def test_convert_bytes(self):
        assert '5.01 GiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            5 * 1024 ** 3 + 10 * 1024 ** 2, 5.001)

        # '5.0011 GiB' -> 100 KiB above condition value of 5.001 GiB
        cond_val_gb = 5.001  # user provided value
        curr_val_bytes = gb_to_bytes(
            cond_val_gb) + 100 * 1024  # 100 KiB above condition
        assert '5.0011 GiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

        # '5.0010000002 GiB' -> 1 byte above condition value of 5.001 GiB
        cond_val_gb = 5.001  # user provided value
        curr_val_bytes = gb_to_bytes(cond_val_gb) + 1  # 1 byte above condition
        assert '5.0010000002 GiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

        # '40.03 MiB' -> 100 KiB above condition value of 0.039 GiB (39 MiB)
        cond_val_gb = 0.039001  # user provided value
        curr_val_bytes = gb_to_bytes(
            cond_val_gb) + 100 * 1024  # 100 KiB above condition
        assert '40.03 MiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

        # '39.94 MiB' -> 1 byte above condition value of 0.039 GiB (39 MiB)
        cond_val_gb = 0.039  # user provided value
        curr_val_bytes = gb_to_bytes(cond_val_gb) + 1  # 1 byte above condition
        assert '39.94 MiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

        # '1.02 MiB' -> 1 byte above condition value of 0.001 GiB (1 MiB)
        cond_val_gb = 0.001  # user provided value
        curr_val_bytes = gb_to_bytes(cond_val_gb) + 1  # 1 byte above condition
        assert '1.02 MiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

        # '1.05 KiB' -> 1 byte above condition value of 0.000001 GiB (1 KiB)
        cond_val_gb = 0.000001  # user provided value
        curr_val_bytes = gb_to_bytes(cond_val_gb) + 1  # 1 byte above condition
        assert '1.05 KiB' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

        # '108 bytes' -> 1 byte above condition value of 0.0000001 GiB (100 bytes)
        cond_val_gb = 0.0000001  # user provided value
        curr_val_bytes = gb_to_bytes(cond_val_gb) + 1  # 1 byte above condition
        assert '108 bytes' == NuvlaEdgeNotificationBuilder.convert_bytes(
            curr_val_bytes, cond_val_gb)

    def test_to_timestamp_utc(self):
        ts_base = '2023-11-14T08:26:52'
        timestamps = [ts_base + '.114Z',
                      ts_base + '.114',
                      ts_base + 'Z',
                      ts_base]
        for ts in timestamps:
            assert ts_base + 'Z' == to_timestamp_utc(ts)

    def test_builder_numeric_network(self):
        cond_value = '5.1'
        sc = SubscriptionCfg({
            'id': 'subscription-config/01',
            'name': 'nb Rx',
            'description': 'nb network cumulative Rx over 30 days',
            'method-ids': [
                'notification-method/01'
            ],
            'acl': {
                'owners': [
                    user
                ]},
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': cond_value,
                'kind': 'numeric',
                'window': 'monthly',
                'dev-name': 'eth0'
            }})
        metrics = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
             'RESOURCES': {'NET-STATS': [
                               {'interface': 'eth0',
                                'bytes-transmitted': 0,
                                'bytes-received': 0},
                               {'interface': 'lo',
                                'bytes-transmitted': 63742086112,
                                'bytes-received': 63742086112
                                }]},
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': [user],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        notif_builder = NuvlaEdgeNotificationBuilder(sc, metrics)
        value = gb_to_bytes(float(cond_value)) + 12 * 1024 ** 2
        notif = notif_builder\
            .metric_name('RxGB')\
            .value_rxtx_adjusted(value)\
            .recovery(True)\
            .build()
        print(notif)
        assert {'id': 'subscription-config/01',
                'subs_id': 'subscription-config/01',
                'subs_name': 'nb Rx',
                'method_ids': ['notification-method/01'],
                'subs_description': 'nb network cumulative Rx over 30 days',
                'condition': '>',
                'condition_value': '5.1 GiB',
                'resource_name': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
                'resource_description': 'None - self-registration number 220171415421241',
                'resource_uri': 'edge/01',
                'timestamp': '2022-08-02T15:21:46Z',
                'recovery': True,
                'metric': 'RxGB',
                'value': f'{round(float(cond_value) + 0.01, 2)} GiB'} == notif

    def test_builder_value_rxtx_adjusted(self):
        metric = {'id': 'nuvlabox/01',
                  'NAME': 'NE',
                  'DESCRIPTION': 'NE',
                  'TAGS': ['arch=x86-64'],
                  'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
                  'RESOURCES': {'NET-STATS': [
                      {'interface': 'eth0',
                       'bytes-transmitted': 0,
                       'bytes-received': 0}]},
                  'TIMESTAMP': '2022-08-02T15:21:46Z',
                  'ACL': {'owners': [user],
                          'view-data': ['group/elektron',
                                        'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}}
        metrics = NuvlaEdgeMetrics(metric)
        subs_conf = {
            'id': 'subscription-config/01',
            'name': 'nb Rx',
            'description': 'nb network cumulative Rx over 30 days',
            'method-ids': [
                'notification-method/01'
            ],
            'acl': {
                'owners': [
                    user
                ]},
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': None,
                'kind': 'numeric',
                'window': 'monthly',
                'dev-name': 'eth0'
            }}
        cond_val, delta_bytes = '0.001', 1234
        subs_conf['criteria']['value'] = cond_val
        sc = SubscriptionCfg(subs_conf)
        notif_builder = NuvlaEdgeNotificationBuilder(sc, metrics)
        value = gb_to_bytes(float(cond_val)) + delta_bytes
        notif = notif_builder.value_rxtx_adjusted(value).build()
        assert f'{cond_val} GiB' == notif['condition_value']
        value, unit = notif['value'].split(' ')
        assert unit == 'MiB'
        assert float(value) * 1024 ** 2 > float(cond_val) * 1024 ** 3

        cond_val, delta_bytes = '5', 1
        subs_conf['criteria']['value'] = cond_val
        sc = SubscriptionCfg(subs_conf)
        notif_builder = NuvlaEdgeNotificationBuilder(sc, metrics)
        value = gb_to_bytes(float(cond_val)) + delta_bytes
        notif = notif_builder.value_rxtx_adjusted(value).build()
        assert f'{cond_val} GiB' == notif['condition_value']
        value, unit = notif['value'].split(' ')
        assert unit == 'GiB'
        assert float(value) * 1024 ** 3 > float(cond_val) * 1024 ** 3

        cond_val, delta_bytes = '1.039', 96
        subs_conf['criteria']['value'] = cond_val
        sc = SubscriptionCfg(subs_conf)
        notif_builder = NuvlaEdgeNotificationBuilder(sc, metrics)
        value = gb_to_bytes(float(cond_val)) + delta_bytes
        notif = notif_builder.value_rxtx_adjusted(value).build()
        assert f'{cond_val} GiB' == notif['condition_value']
        value, unit = notif['value'].split(' ')
        assert unit == 'GiB'
        assert float(value) * 1024 ** 3 > float(cond_val) * 1024 ** 3

    def test_builder_ne_onoff(self):
        sc = SubscriptionCfg({
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
        metrics = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'ONLINE': True,
             'ONLINE_PREV': False,
             'TIMESTAMP': '2022-08-02T15:21:46.321Z',
             'NUVLA_TIMESTAMP': '2022-08-02T15:25:00.123Z',
             'ACL': {'owners': [user],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_online(sc, nescm.MATCHED_RECOVERY)
        assert res['method_ids'] == sc.get('method-ids')
        assert res['subs_description'] == sc.get('description')
        assert res['metric'] == 'NE online'
        assert res['condition'] == 'true'
        assert res['recovery'] is True
        assert res['value'] == ''
        assert res['condition_value'] == ''
        assert res['timestamp'] == '2022-08-02T15:25:00Z'

        metrics = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'ONLINE': False,
             'ONLINE_PREV': True,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'NUVLA_TIMESTAMP': '2022-08-02T15:25:01.999Z',
             'ACL': {'owners': [user],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_online(sc, nescm.MATCHED)
        assert res['method_ids'] == sc.get('method-ids')
        assert res['subs_description'] == sc.get('description')
        assert res['metric'] == 'NE online'
        assert res['condition'] == 'false'
        assert res['recovery'] is False
        assert res['value'] == ''
        assert res['condition_value'] == ''
        assert res['timestamp'] == '2022-08-02T15:25:01Z'

    def test_builder_ne_disk(self):
        thld = 80
        sc = SubscriptionCfg(
            {
                'id': 'subscription-config/01',
                'name': 'disk >85%',
                'description': 'disk >85%',
                'method-ids': [
                    'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
                ],
                'acl': {
                    'owners': [
                        user
                    ]},
                'criteria': {
                    'metric': 'disk',
                    'kind': 'numeric',
                    'dev-name': 'vda1',
                    'condition': '>',
                    'value': str(thld)
                }
            }
        )

        # Value is above the threshold.
        metrics = self._metrics_above_thld(thld)
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_disk(sc, nescm.MATCHED)
        self._validate_above_thld(res, sc)

        # Recovery
        metrics = self._metrics_recovery(thld)
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_disk(sc, nescm.MATCHED_RECOVERY)
        self._validate_recovery(res, sc)

    def test_builder_ne_ram(self):
        thld = 80
        sc = SubscriptionCfg(
            {
                'id': 'subscription-config/01',
                'name': 'ram >85%',
                'description': 'ram >85%',
                'method-ids': [
                    'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
                ],
                'acl': {
                    'owners': [
                        user
                    ]},
                'criteria': {
                    'metric': 'ram',
                    'kind': 'numeric',
                    'condition': '>',
                    'value': str(thld)
                }
            }
        )

        # Value is above the threshold.
        metrics = self._metrics_above_thld(thld)
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_ram(sc, nescm.MATCHED)
        self._validate_above_thld(res, sc)

        # Recovery
        metrics = self._metrics_recovery(thld)
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_ram(sc, nescm.MATCHED_RECOVERY)
        self._validate_recovery(res, sc)

    def test_builder_ne_load(self):
        thld = 80
        sc = SubscriptionCfg(
            {
                'id': 'subscription-config/01',
                'name': 'load >85%',
                'description': 'load >85%',
                'method-ids': [
                    'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
                ],
                'acl': {
                    'owners': [
                        user
                    ]},
                'criteria': {
                    'metric': 'load',
                    'kind': 'numeric',
                    'condition': '>',
                    'value': str(thld)
                }
            }
        )

        # Value is above the threshold.
        metrics = self._metrics_above_thld(thld)
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_load(sc, nescm.MATCHED)
        self._validate_above_thld(res, sc)

        # Recovery
        metrics = self._metrics_recovery(thld)
        nescm = NuvlaEdgeSubsCfgMatcher(metrics)
        res = nescm.notif_build_load(sc, nescm.MATCHED_RECOVERY)
        self._validate_recovery(res, sc)


    @staticmethod
    def _metrics_recovery(thld_pct: int):
        delta = 1
        return NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'ONLINE': True,
             'ONLINE_PREV': False,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {
                 'owners': [user],
                 'view-data': ['group/elektron',
                               'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                               'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']},
             'RESOURCES': {
                 'CPU': {'load': (thld_pct - delta) / 10, 'capacity': 10, 'topic': 'cpu'},
                 'RAM': {'used': (thld_pct - delta) * 10, 'capacity': 1000, 'topic': 'ram'},
                 'DISKS': [{'used': thld_pct - delta, 'capacity': 100, 'device': 'vda1'}]
             },
             'RESOURCES_PREV': {
                 'CPU': {'load': (thld_pct + delta) / 10, 'capacity': 10, 'topic': 'cpu'},
                 'RAM': {'used': (thld_pct + delta) * 10, 'capacity': 1000, 'topic': 'ram'},
                 'DISKS': [{'used': thld_pct + delta, 'capacity': 100, 'device': 'vda1'}]},
             })

    @staticmethod
    def _metrics_above_thld(thld_pct: int):
        delta = 1
        return NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['arch=x86-64'],
             'ONLINE': True,
             'ONLINE_PREV': False,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {
                 'owners': [user],
                 'view-data': ['group/elektron',
                               'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                               'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']},
             'RESOURCES': {
                 'CPU': {'load': (thld_pct + delta) / 10, 'capacity': 10, 'topic': 'cpu'},
                 'RAM': {'used': (thld_pct + delta) * 10, 'capacity': 1000, 'topic': 'ram'},
                 'DISKS': [{'used': thld_pct + delta, 'capacity': 100, 'device': 'vda1'}]},
             'RESOURCES_PREV': {
                 'CPU': {'load': (thld_pct - delta) / 10, 'capacity': 10, 'topic': 'cpu'},
                 'RAM': {'used': (thld_pct - delta) * 10, 'capacity': 1000, 'topic': 'ram'},
                 'DISKS': [{'used': thld_pct - delta, 'capacity': 100, 'device': 'vda1'}]},
             })

    @staticmethod
    def _validate_above_thld(res, sc):
        assert res['method_ids'] == sc.get('method-ids')
        assert res['subs_description'] == sc.get('description')
        assert res['metric'] == NuvlaEdgeNotificationBuilder.METRIC_NAME_MAP[
            sc.criteria_metric()]
        assert res['condition'] == sc.criteria_condition()
        assert res['recovery'] is False
        assert res['condition_value'] == str(sc.criteria_value())
        assert res['value'] > sc.criteria_value()

    @staticmethod
    def _validate_recovery(res, sc):
        assert res['method_ids'] == sc.get('method-ids')
        assert res['subs_description'] == sc.get('description')
        assert res['metric'] == NuvlaEdgeNotificationBuilder.METRIC_NAME_MAP[sc.criteria_metric()]
        assert res['condition'] == sc.criteria_condition()
        assert res['recovery'] is True
        assert res['condition_value'] == str(sc.criteria_value())
        assert res['value'] < sc.criteria_value()


class TestNuvlaEdgeNotification(unittest.TestCase):

    def test_init_no_value(self):
        sc = SubscriptionCfg({
            'id': 'subscription-config/01',
            'name': 'ne on/off',
            'description': 'ne on/off',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'criteria': {
                'metric': 'state',
                'kind': 'boolean',
                'condition': 'no'
            }})
        metrics = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TIMESTAMP': '2022-08-02T15:21:46Z'})
        ne_notif = NuvlaEdgeNotification(sc, metrics)
        assert '' == ne_notif['condition_value']

    def test_nuvla_timestamp(self):
        sc = SubscriptionCfg({
            'id': 'subscription-config/01',
            'name': 'ne on/off',
            'description': 'ne on/off',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'criteria': {
                'metric': 'state',
                'kind': 'boolean',
                'condition': 'no'
            }})
        timestamp = '2022-08-02T15:21:46Z'
        metrics = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TIMESTAMP': None,
             'NUVLA_TIMESTAMP': timestamp})
        ne_notif = NuvlaEdgeNotification(sc, metrics)
        assert ne_notif['timestamp'] == timestamp
