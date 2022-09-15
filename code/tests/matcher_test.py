import unittest

from nuvla.notifs.db import RxTxDB, RxTx, bytes_to_gb
from nuvla.notifs.matcher import SubscriptionConfigMatcher, \
    NuvlaEdgeSubsConfMatcher
from nuvla.notifs.metric import ResourceMetrics, NuvlaEdgeResourceMetrics
from nuvla.notifs.subscription import SubscriptionConfig


class TestSubscriptionConfigMatcher(unittest.TestCase):

    def test_init(self):
        scm = SubscriptionConfigMatcher(ResourceMetrics({}))
        assert False is scm.resource_subscribed(SubscriptionConfig({}))

    def test_resource_subscribed(self):
        m = ResourceMetrics({'acl': {'owners': ['me']},
                             'tags': ['foo']})

        scm = SubscriptionConfigMatcher(m)

        sc = SubscriptionConfig({'enabled': True})
        assert False is scm.resource_subscribed(sc)

        sc = SubscriptionConfig({
            'enabled': True,
            'acl': {'owners': ['me']}})
        assert False is scm.resource_subscribed(sc)

        sc = SubscriptionConfig({
            'enabled': True,
            'resource-filter': "tags='foo'",
            'acl': {'owners': ['me']}})
        assert True is scm.resource_subscribed(sc)

    def test_resource_subscription(self):
        assert [] == \
               list(SubscriptionConfigMatcher(ResourceMetrics()) \
                    .resource_subscriptions([SubscriptionConfig({}),
                                             SubscriptionConfig(
                                                 {'enabled': True}),
                                             SubscriptionConfig({
                                                 'enabled': True,
                                                 'acl': {'owners': ['me']}}),
                                             SubscriptionConfig({
                                                 'enabled': True,
                                                 'resource-filter': "tags='foo'",
                                                 'acl': {'owners': ['me']}})]))

        scm = SubscriptionConfigMatcher(
            ResourceMetrics({'acl': {'owners': ['me']},
                             'tags': ['foo']}))
        scs = [SubscriptionConfig({}),
               SubscriptionConfig(
                   {'enabled': True}),
               SubscriptionConfig({
                   'enabled': True,
                   'acl': {'owners': ['me']}}),
               SubscriptionConfig({
                   'enabled': True,
                   'resource-filter': "tags='foo'",
                   'acl': {'owners': ['me']}})]
        assert [SubscriptionConfig({
            'enabled': True,
            'resource-filter': "tags='foo'",
            'acl': {'owners': ['me']}})] == list(scm.resource_subscriptions(scs))


class TestNuvlaEdgeSubsConfMatcher(unittest.TestCase):

    def test_init(self):
        scm = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({}))
        assert False is scm.resource_subscribed(SubscriptionConfig({}))

    def test_load_thld_above_below(self):
        sc = SubscriptionConfig({'criteria': {
            'kind': 'numeric',
            'value': '90'}})

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert False is nem._load_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_below_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 3.5, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert False is nem._load_moved_below_thld(sc)

    def test_load_thld_over(self):
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 3.5, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        sc = SubscriptionConfig({
            'criteria': {
                'kind': 'numeric',
                'value': '90'
            }})
        assert False is nem._load_moved_over_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_over_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_over_thld(sc)

    def test_match_load(self):
        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'foo',
                'condition': 'is',
                'value': 'false',
                'kind': 'bool'
            }})
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({}))
        assert None is nem.match_load(sc)

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'load',
                'condition': '>',
                'value': '90',
                'kind': 'numeric'
            }})
        # no change in load
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert None is nem.match_load(sc)

        # load increased above threshold
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert [True, False] == list(nem.match_load(sc).values())

        # load decreased below threshold and recovered
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert [True, True] == list(nem.match_load(sc).values())

    def test_ram_thld_above_below(self):
        sc = SubscriptionConfig({'criteria': {
            'kind': 'numeric',
            'value': '90'}})

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'RAM': {'used': 900, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 899, 'capacity': 1000, 'topic': 'ram'}}}))
        assert False is nem._ram_moved_above_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 850, 'capacity': 1000, 'topic': 'ram'}}}))
        assert True is nem._ram_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'RAM': {'used': 900, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'}}}))
        assert False is nem._ram_moved_below_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'RAM': {'used': 899, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'}}}))
        assert True is nem._ram_moved_below_thld(sc)

    def test_disk_thld_above_below(self):
        sc = SubscriptionConfig({'criteria': {
            'dev-name': 'C:',
            'kind': 'numeric',
            'value': '90'}})
        # None cases
        # disk not known
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]}}))
        assert None is nem._disk_moved_above_thld(sc)
        assert None is nem._disk_moved_below_thld(sc)

        # previous data for the disk not defined
        sc = SubscriptionConfig({'criteria': {
            'dev-name': 'A:',
            'kind': 'numeric',
            'value': '90'}})
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'C:'}]}}))
        assert None is nem._disk_moved_above_thld(sc)
        assert None is nem._disk_moved_below_thld(sc)
        # current data for the disk not defined
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'C:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]}}))
        assert None is nem._disk_moved_above_thld(sc)
        assert None is nem._disk_moved_below_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]}}))
        assert False is nem._disk_moved_above_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]}}))
        assert True is nem._disk_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]}}))
        assert False is nem._disk_moved_below_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]}}))
        assert True is nem._disk_moved_below_thld(sc)

    def test_match_net_rxtx_no_db(self):
        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeResourceMetrics({})
        rxtx_db = RxTxDB()
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'network-tx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        rxtx_db = RxTxDB()
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        rxtx_db = RxTxDB()
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            'NETWORK': {'default_gw': 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            'NETWORK': {'default_gw': 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': 0,
                               'bytes-received': 0},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        rxtx_db = RxTxDB()
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            'NETWORK': {'default_gw': 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': 6 * 1024 ** 4,
                               'bytes-received': 7 * 1024 ** 4},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               "bytes-received": 63742086112
                               }]},
            "RESOURCES_PREV": {
                "CPU": {"load": 3.0, "capacity": 4, "topic": "cpu"}}})
        rxtx_db = RxTxDB()
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        r_rx = nem.network_rx_above_thld(sc)
        print(r_rx)
        assert {'interface': 'eth0', 'value': 7.0} == r_rx
        r_tx = nem.network_tx_above_thld(sc)
        assert None is r_tx

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'network-tx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            }})
        rxtx_db = RxTxDB()
        rxtx_db.update(nerm)
        r_rx = nem.network_rx_above_thld(sc)
        assert None is r_rx
        r_tx = nem.network_tx_above_thld(sc)
        assert {'interface': 'eth0', 'value': 6.0} == r_tx

    def test_match_net_rxtx_with_db(self):
        rxtx_db = RxTxDB()

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeResourceMetrics({})
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)
        assert {} == rxtx_db.db

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeResourceMetrics({})
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert {} == rxtx_db.db

        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            "NETWORK": {"default_gw": "eth0"},
            "RESOURCES": {"net-stats": [
                {"interface": "eth0",
                 "bytes-transmitted": 1 * 1024 ** 3,
                 "bytes-received": 2 * 1024 ** 3}]}})
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.db['ne/1']['eth0']['rx']
        assert 2 * 1024 ** 3 == rx.total
        assert 2 * 1024 ** 3 == rx.prev

        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            "NETWORK": {"default_gw": "eth0"},
            "RESOURCES": {"net-stats": [
                {"interface": "eth0",
                 "bytes-transmitted": 1 * 1024 ** 4,
                 "bytes-received": 2 * 1024 ** 4}]}})
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.db['ne/1']['eth0']['rx']
        assert 2 * 1024 ** 4 == rx.total
        assert 2 * 1024 ** 4 == rx.prev

        # counter reset
        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            "NETWORK": {"default_gw": "eth0"},
            "RESOURCES": {"net-stats": [
                {"interface": "eth0",
                 "bytes-transmitted": 0,
                 "bytes-received": 3 * 1024 ** 3}]}})
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.db['ne/1']['eth0']['rx']
        assert 2 * 1024 ** 4 + 3 * 1024 ** 3 == rx.total
        assert 3 * 1024 ** 3 == rx.prev

        # above threshold
        nerm = NuvlaEdgeResourceMetrics({
            'id': 'ne/1',
            "NETWORK": {"default_gw": "eth0"},
            "RESOURCES": {"net-stats": [
                {"interface": "eth0",
                 "bytes-transmitted": 0,
                 "bytes-received": 4 * 1024 ** 4}]}})
        rxtx_db.update(nerm)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        value_bytes = 2 * 1024 ** 4 + 4 * 1024 ** 4
        value_gb = bytes_to_gb(value_bytes)
        assert {'interface': 'eth0', 'value': value_gb} == \
               nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.db['ne/1']['eth0']['rx']
        assert value_bytes == rx.total
        assert 4 * 1024 ** 4 == rx.prev

    def test_match_went_onoffline(self):
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({}))
        assert False is nem._went_online()
        assert False is nem._went_offline()
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': True}))
        assert False is nem._went_online()
        assert False is nem._went_offline()
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': False}))
        assert False is nem._went_online()
        assert False is nem._went_offline()

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': False}))
        assert True is nem._went_online()
        assert False is nem._went_offline()

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': True}))
        assert False is nem._went_online()
        assert True is nem._went_offline()

    def test_match_online(self):
        sc = SubscriptionConfig(
            {'criteria': {
                'metric': 'foo',
                'kind': 'boolean',
                'condition': 'bar',
                'value': 'true'
            }})
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics({}))
        assert None is nem.match_online(sc)

        sc = SubscriptionConfig(
            {'criteria': {
                'metric': 'state',
                'kind': 'boolean',
                'condition': 'no',
                'value': 'true'
            }})
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': True}))
        assert None is nem.match_online(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': False}))
        assert None is nem.match_online(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': True}))
        assert nem.MATCHED is nem.match_online(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeResourceMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': False}))
        assert nem.MATCHED_RECOVERY is nem.match_online(sc)

    def test_match_all_none(self):
        sc = SubscriptionConfig({
            'id': 'foo',
            'criteria': {
                'metric': 'foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'}})
        nerm = NuvlaEdgeResourceMetrics({})
        nescm = NuvlaEdgeSubsConfMatcher(nerm)
        assert [] == nescm.match_all([sc])

        sc = SubscriptionConfig({
            'enabled': True,
            'resource-filter': "tags='foo'",
            'acl': {'owners': ['me']},
            'criteria': {
                'metric': 'foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'}})
        nerm = NuvlaEdgeResourceMetrics({
            'ACL': {'owners': ['me']},
            'TAGS': ['foo']
        })
        nescm = NuvlaEdgeSubsConfMatcher(nerm)
        assert [] == nescm.match_all([sc])

    def test_match_all(self):
        sc_disk = SubscriptionConfig({
            'id': 'subscription-config/01',
            'name': 'NE disk',
            'description': 'NE disk',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'enabled': True,
            'resource-filter': "tags='x86-64'",
            'acl': {'owners': ['me']},
            'criteria': {
                'metric': 'disk',
                'condition': '>',
                'value': '90',
                'kind': 'numeric',
                'dev-name': 'disk0p1'
            }})
        sc_load = SubscriptionConfig({
            'id': 'subscription-config/02',
            'name': 'NE load',
            'description': 'NE load',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'enabled': True,
            'resource-filter': "tags='x86-64'",
            'acl': {'owners': ['me']},
            'criteria': {
                'metric': 'load',
                'condition': '>',
                'value': '90',
                'kind': 'numeric'}})
        sc_ram = SubscriptionConfig({
            'id': 'subscription-config/03',
            'name': 'NE ram',
            'description': 'NE ram',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'enabled': True,
            'resource-filter': "tags='x86-64'",
            'acl': {'owners': ['me']},
            'criteria': {
                'metric': 'ram',
                'condition': '>',
                'value': '90',
                'kind': 'numeric'}})
        sc_rx_wlan1 = SubscriptionConfig({
            'id': 'subscription-config/04',
            'name': 'NE network Rx',
            'description': 'NE network Rx',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'enabled': True,
            'resource-filter': "tags='x86-64'",
            'acl': {'owners': ['me']},
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '4.5',
                'kind': 'numeric',
                'dev-name': 'wlan1'
            }})
        sc_tx_gw = SubscriptionConfig({
            'id': 'subscription-config/05',
            'name': 'NE network Tx',
            'description': 'NE network Tx',
            'method-ids': [
                'notification-method/a909e4da-3ceb-4c4b-bb48-31ef371c62ae'
            ],
            'enabled': True,
            'resource-filter': "tags='x86-64'",
            'acl': {'owners': ['me']},
            'criteria': {
                'metric': 'network-tx',
                'condition': '>',
                'value': '4.5',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeResourceMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['x86-64'],
             'NETWORK': {'default_gw': 'eth0'},
             'ONLINE': True,
             'ONLINE_PREV': True,
             'RESOURCES': {'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'},
                           'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'},
                           'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'disk0p1'},
                                     {'used': 9.1, 'capacity': 10, 'device': 'disk0p2'}],
                           'net-stats': [
                               {'interface': 'eth0',
                                'bytes-transmitted': 5 * 1024 ** 4,
                                'bytes-received': 0},
                               {'interface': 'lo',
                                'bytes-transmitted': 0,
                                'bytes-received': 0},
                               {'interface': 'wlan1',
                                'bytes-transmitted': 0,
                                'bytes-received': 5 * 1024 ** 4}
                           ]},
             'RESOURCES_PREV': {
                 'CPU': {'load': 8.1, 'capacity': 10, 'topic': 'cpu'},
                 'RAM': {'used': 899, 'capacity': 1000, 'topic': 'ram'},
                 'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'disk0p1'}]},
             'RESOURCES_CPU_LOAD_PERS': 138.0,
             'RESOURCES_RAM_USED_PERS': 77,
             'RESOURCES_DISK1_USED_PERS': 60,
             'RESOURCES_PREV_CPU_LOAD_PERS': 139.0,
             'RESOURCES_PREV_RAM_USED_PERS': 76,
             'RESOURCES_PREV_DISK1_USED_PERS': 60,
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': ['me'],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        net_db = RxTxDB()
        net_db.update(nerm)
        nescm = NuvlaEdgeSubsConfMatcher(nerm, net_db)
        res = nescm.match_all([sc_disk, sc_load, sc_ram, sc_rx_wlan1, sc_tx_gw])
        ids_res = list(map(lambda x: x['id'], res))
        ids = set([f'subscription-config/0{i}' for i in range(1, 6)])
        assert set() == ids.difference(ids_res)
