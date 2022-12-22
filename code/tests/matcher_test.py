import unittest

from db_test import TestRxTxDriverESMockedBase
from nuvla.notifs.db import RxTxDB, RxTx, bytes_to_gb, gb_to_bytes, \
    RxTxDriverInMem
from nuvla.notifs.matcher import ResourceSubsConfigMatcher, \
    NuvlaEdgeSubsConfMatcher, TaggedResourceSubsConfigMatcher, \
    TaggedResourceNetSubsConfigMatcher
from nuvla.notifs.metric import NuvlaEdgeMetrics
from nuvla.notifs.resource import Resource
from nuvla.notifs.subscription import SubscriptionConfig


class TestResourceSubsConfigMatcher(unittest.TestCase):

    def test_init(self):
        rscm = ResourceSubsConfigMatcher()
        assert False is rscm.resource_subscribed(Resource({}),
                                                 SubscriptionConfig({}))

    def test_resource_subscribed(self):
        r = Resource({'acl': {'owners': ['me']},
                      'foo': ['bar']})

        scm = ResourceSubsConfigMatcher()

        sc = SubscriptionConfig({'enabled': True})
        assert False is scm.resource_subscribed(r, sc)

        sc = SubscriptionConfig({
            'enabled': False,
            'acl': {'owners': ['me']}})
        assert False is scm.resource_subscribed(r, sc)

        sc = SubscriptionConfig({
            'enabled': True,
            'acl': {'owners': ['me']}})
        assert True is scm.resource_subscribed(r, sc)

    def test_resource_subscription(self):
        r = Resource()
        assert [] == \
               list(ResourceSubsConfigMatcher()
                    .resource_subscriptions(r,
                                            [SubscriptionConfig({}),
                                             SubscriptionConfig(
                                                 {'enabled': True}),
                                             SubscriptionConfig({
                                                 'enabled': True,
                                                 'acl': {'owners': ['me']}}),
                                             SubscriptionConfig({
                                                 'enabled': True,
                                                 'resource-filter': "tags='foo'",
                                                 'acl': {'owners': ['me']}})]))

        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})
        rscm = ResourceSubsConfigMatcher()
        scs = [SubscriptionConfig({}),
               SubscriptionConfig(
                   {'enabled': True}),
               SubscriptionConfig({
                   'enabled': True,
                   'acl': {'owners': ['me']}}),
               SubscriptionConfig({
                   'enabled': False,
                   'acl': {'owners': ['me']}})]
        assert [SubscriptionConfig({
            'enabled': True,
            'acl': {'owners': ['me']}})] == list(rscm.resource_subscriptions(r, scs))

    def test_resource_subs_ids(self):
        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})
        rscm = ResourceSubsConfigMatcher()
        scs = [SubscriptionConfig({'id': 'subs/01'}),
               SubscriptionConfig(
                   {'enabled': True,
                    'id': 'subs/02'}),
               SubscriptionConfig({
                   'id': 'subs/03',
                   'enabled': True,
                   'acl': {'owners': ['me']}}),
               SubscriptionConfig({
                   'id': 'subs/04',
                   'enabled': False,
                   'acl': {'owners': ['me']}})]
        assert ['subs/03'] == list(rscm.resource_subscriptions_ids(r, scs))


class TestTaggedResourceSubsConfigMatcher(unittest.TestCase):

    def test_init(self):
        trscm = TaggedResourceSubsConfigMatcher()
        assert False is trscm.resource_subscribed(Resource({}),
                                                  SubscriptionConfig({}))

    def test_resource_subscribed(self):
        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})

        trscm = TaggedResourceSubsConfigMatcher()

        sc = SubscriptionConfig({'enabled': True})
        assert False is trscm.resource_subscribed(r, sc)

        sc = SubscriptionConfig({
            'enabled': False,
            'acl': {'owners': ['me']}})
        assert False is trscm.resource_subscribed(r, sc)

        sc = SubscriptionConfig({
            'enabled': True,
            'resource-filter': "tags='foo'",
            'acl': {'owners': ['me']}})
        assert True is trscm.resource_subscribed(r, sc)

        sc = SubscriptionConfig({
            'enabled': True,
            'resource-filter': "tags='bar'",
            'acl': {'owners': ['me']}})
        assert False is trscm.resource_subscribed(r, sc)

    def test_resource_subscription(self):
        r = Resource()
        assert [] == \
               list(TaggedResourceSubsConfigMatcher()
                    .resource_subscriptions(r,
                                            [SubscriptionConfig({}),
                                             SubscriptionConfig(
                                                 {'enabled': True}),
                                             SubscriptionConfig({
                                                 'enabled': True,
                                                 'acl': {'owners': ['me']}}),
                                             SubscriptionConfig({
                                                 'enabled': True,
                                                 'resource-filter': "tags='foo'",
                                                 'acl': {'owners': ['me']}})]))

        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})
        scm = TaggedResourceSubsConfigMatcher()
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
            'acl': {'owners': ['me']}})] == list(scm.resource_subscriptions(r, scs))

    def test_resource_subs_ids(self):
        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo', 'bar']})
        rscm = TaggedResourceSubsConfigMatcher()
        scs = [SubscriptionConfig({'id': 'subs/01'}),
               SubscriptionConfig(
                   {'enabled': True,
                    'id': 'subs/02'}),
               SubscriptionConfig({
                   'id': 'subs/03',
                   'enabled': True,
                   'resource-filter': "tags='foo'",
                   'acl': {'owners': ['me']}}),
               SubscriptionConfig({
                   'id': 'subs/04',
                   'enabled': False,
                   'resource-filter': "tags='foo'",
                   'acl': {'owners': ['me']}}),
               SubscriptionConfig({
                   'id': 'subs/05',
                   'enabled': True,
                   'resource-filter': "tags='bar'",
                   'acl': {'owners': ['me']}})]
        assert ['subs/03', 'subs/05'] == list(rscm.resource_subscriptions_ids(r, scs))


class TestNuvlaEdgeSubsConfMatcher(unittest.TestCase):

    def test_init(self):
        scm = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({}))
        assert [] == scm.resource_subscriptions([SubscriptionConfig({})])

    def test_load_thld_above_below(self):
        sc = SubscriptionConfig({'criteria': {
            'kind': 'numeric',
            'value': '90'}})

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert False is nem._load_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_below_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 3.5, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert False is nem._load_moved_below_thld(sc)

    def test_load_thld_over(self):
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 3.5, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        sc = SubscriptionConfig({
            'criteria': {
                'kind': 'numeric',
                'value': '90'
            }})
        assert False is nem._load_moved_over_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert True is nem._load_moved_over_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
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
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({}))
        assert None is nem.match_load(sc)

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'load',
                'condition': '>',
                'value': '90',
                'kind': 'numeric'
            }})
        # no change in load
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert None is nem.match_load(sc)

        # load increased above threshold
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert [True, False] == list(nem.match_load(sc).values())

        # load decreased below threshold and recovered
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {
                'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}}}))
        assert [True, True] == list(nem.match_load(sc).values())

    def test_ram_thld_above_below(self):
        sc = SubscriptionConfig({'criteria': {
            'kind': 'numeric',
            'value': '90'}})

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'RAM': {'used': 900, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 899, 'capacity': 1000, 'topic': 'ram'}}}))
        assert False is nem._ram_moved_above_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 850, 'capacity': 1000, 'topic': 'ram'}}}))
        assert True is nem._ram_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'RAM': {'used': 900, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {
                'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'}}}))
        assert False is nem._ram_moved_below_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
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
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
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
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'C:'}]}}))
        assert None is nem._disk_moved_above_thld(sc)
        assert None is nem._disk_moved_below_thld(sc)
        # current data for the disk not defined
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'C:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]}}))
        assert None is nem._disk_moved_above_thld(sc)
        assert None is nem._disk_moved_below_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]}}))
        assert False is nem._disk_moved_above_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]}}))
        assert True is nem._disk_moved_above_thld(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]}}))
        assert False is nem._disk_moved_below_thld(sc)
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({
            'RESOURCES': {
                'DISKS': [{'used': 8.9, 'capacity': 10, 'device': 'A:'}]},
            'RESOURCES_PREV': {
                'DISKS': [{'used': 9, 'capacity': 10, 'device': 'A:'}]}}))
        assert True is nem._disk_moved_below_thld(sc)

    def test_match_went_onoffline(self):
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({}))
        assert False is nem._went_online()
        assert False is nem._went_offline()
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': True}))
        assert False is nem._went_online()
        assert False is nem._went_offline()
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': False}))
        assert False is nem._went_online()
        assert False is nem._went_offline()

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': False}))
        assert True is nem._went_online()
        assert False is nem._went_offline()

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
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
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics({}))
        assert None is nem.match_online(sc)

        sc = SubscriptionConfig(
            {'criteria': {
                'metric': 'state',
                'kind': 'boolean',
                'condition': 'no',
                'value': 'true'
            }})
        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': True}))
        assert None is nem.match_online(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': False}))
        assert None is nem.match_online(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': False,
             'ONLINE_PREV': True}))
        assert nem.MATCHED is nem.match_online(sc)

        nem = NuvlaEdgeSubsConfMatcher(NuvlaEdgeMetrics(
            {'ONLINE': True,
             'ONLINE_PREV': False}))
        assert nem.MATCHED_RECOVERY is nem.match_online(sc)

    def test_network_device_name(self):

        sc = SubscriptionConfig({'criteria': {}})
        nerm = NuvlaEdgeMetrics()
        nem = NuvlaEdgeSubsConfMatcher(nerm)
        assert None is nem.network_device_name(sc)

        sc = SubscriptionConfig({'criteria': {'dev-name': 'wlan0'}})
        nerm = NuvlaEdgeMetrics({})
        nem = NuvlaEdgeSubsConfMatcher(nerm)
        assert 'wlan0' == nem.network_device_name(sc)

        sc = SubscriptionConfig({'criteria': {'dev-name': 'wlan0'}})
        nerm = NuvlaEdgeMetrics({
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'}})
        nem = NuvlaEdgeSubsConfMatcher(nerm)
        assert 'wlan0' == nem.network_device_name(sc)

        sc = SubscriptionConfig({'criteria': {}})
        nerm = NuvlaEdgeMetrics({
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth1'}})
        nem = NuvlaEdgeSubsConfMatcher(nerm)
        assert 'eth1' == nem.network_device_name(sc)


class TestNuvlaEdgeSubsConfMatcherDBInMem(unittest.TestCase):

    driver = None

    def setUp(self) -> None:
        self.driver = RxTxDriverInMem()

    def test_match_net_above_thld_empty_metrics(self):
        """
        No metrics provided. No matching will be performed.
        """

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'network-tx',
                'condition': '>',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeMetrics({})
        rxtx_db = RxTxDB(self.driver)
        rxtx_db.update(nerm, ['subs/01'])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

    def test_match_net_above_thld_criteria_no_match(self):
        """
        Metric and condition in criteria of the subscription configuration
        don't match expected Rx or Tx ones.
        """

        sc = SubscriptionConfig({
            'id': 'subs/01',
            'acl': {'owners': ['me']},
            'enabled': True,
            "resource-filter": "tags='nuvlabox=True'",
            'criteria': {
                'metric': 'network-foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(2),
                               'bytes-received': gb_to_bytes(2)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher()\
            .resource_subscriptions_ids(nerm, [sc])
        assert 1 == len(subs_conf_ids)
        rxtx_db = RxTxDB(self.driver)
        rxtx_db.update(nerm, subs_conf_ids)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

    def test_match_net_above_thld_no_device_name(self):
        """
        No device name can be derived because it's not provided in the
        subscription configuration and is missing in the metrics.
        """

        sc = SubscriptionConfig({
            'id': 'subs/01',
            'acl': {'owners': ['me']},
            'enabled': True,
            "resource-filter": "tags='nuvlabox=True'",
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(2),
                               'bytes-received': gb_to_bytes(2)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher() \
            .resource_subscriptions_ids(nerm, [sc])
        assert 1 == len(subs_conf_ids)
        rxtx_db = RxTxDB(self.driver)
        rxtx_db.update(nerm, ['subs/01'])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        return

    def test_match_net_above_thld_no_net_db_provided(self):
        """
        Network DB is not provided. No network metrics matching will be done.
        """

        sc = SubscriptionConfig({
            'id': 'subs/01',
            'acl': {'owners': ['me']},
            'enabled': True,
            "resource-filter": "tags='nuvlabox=True'",
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(2),
                               'bytes-received': gb_to_bytes(2)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher() \
            .resource_subscriptions_ids(nerm, [sc])
        assert 1 == len(subs_conf_ids)
        nem = NuvlaEdgeSubsConfMatcher(nerm, net_db=None)
        assert None is nem.network_rx_above_thld(sc)

    def test_match_net_above_thld_value_goes_above_thld_no_reset_two_subs(self):
        """
        Rx and Tx go above the threshold on a network interface of NE.
        """

        sc_rx = SubscriptionConfig({
            'id': 'subs/01',
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            },
            'resource-filter': "tags='nuvlabox=True'",
            'acl': {'owners': ['me']},
            'enabled': True
        })
        sc_tx = SubscriptionConfig({
            'id': 'subs/02',
            'criteria': {
                'metric': 'network-tx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            },
            'resource-filter': "tags='nuvlabox=True'",
            'acl': {'owners': ['me']},
            'enabled': True
        })

        # Deltas for the traffic increase in GB. In the end they go above the
        # thresholds set in the subscription configurations.
        deltas_rx = [0.0, 2.0, 4.0, 0.0]
        deltas_tx = [0.0, 3.0, 0.0, 4.0]

        # The initial values just set the baseline for the network Rx and Tx.
        gb_rx = 2.0 + deltas_rx[0]
        gb_tx = 3.0 + deltas_tx[0]

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(gb_tx),
                               'bytes-received': gb_to_bytes(gb_rx)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher \
            .resource_subscriptions_ids(nerm, [sc_rx, sc_tx])
        assert 2 == len(subs_conf_ids)
        rxtx_db = RxTxDB(self.driver)
        rxtx_db.update(nerm, subs_conf_ids)

        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc_rx)
        assert None is nem.network_tx_above_thld(sc_tx)

        # An extra 2GB of data transmitted on the default gw.
        # This will still be below the threshold defined by the subscription
        # configurations.
        gb_rx += deltas_rx[1]
        gb_tx += deltas_tx[1]

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(gb_tx),
                               'bytes-received': gb_to_bytes(gb_rx)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher \
            .resource_subscriptions_ids(nerm, [sc_rx, sc_tx])
        assert 2 == len(subs_conf_ids)
        rxtx_db.update(nerm, subs_conf_ids)

        assert sum(deltas_rx[:2]) == \
               rxtx_db.get_rx_gb(sc_rx['id'], 'ne/1', 'eth0')
        assert sum(deltas_tx[:2]) == \
               rxtx_db.get_tx_gb(sc_tx['id'], 'ne/1', 'eth0')

        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc_rx)
        assert None is nem.network_tx_above_thld(sc_tx)

        # An extra XGB of data transmitted on the default gw.
        gb_rx += deltas_rx[2]
        gb_tx += deltas_tx[2]

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(gb_tx),
                               'bytes-received': gb_to_bytes(gb_rx)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher \
            .resource_subscriptions_ids(nerm, [sc_rx, sc_tx])
        assert 2 == len(subs_conf_ids)
        rxtx_db.update(nerm, subs_conf_ids)

        assert sum(deltas_rx[:3]) == \
               rxtx_db.get_rx_gb(sc_rx['id'], 'ne/1', 'eth0')
        assert sum(deltas_tx[:3]) == \
               rxtx_db.get_tx_gb(sc_tx['id'], 'ne/1', 'eth0')

        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert {'interface': 'eth0', 'value': sum(deltas_rx[:3])} == \
               nem.network_rx_above_thld(sc_rx)
        assert None is nem.network_rx_above_thld(sc_rx)

        # An extra XGB of data transmitted on the default gw.
        gb_rx += deltas_rx[3]
        gb_tx += deltas_tx[3]

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'TAGS': ['nuvlabox=True'],
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'me',
                        'user/03']},
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              {'interface': 'eth0',
                               'bytes-transmitted': gb_to_bytes(gb_tx),
                               'bytes-received': gb_to_bytes(gb_rx)},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_conf_ids = TaggedResourceSubsConfigMatcher \
            .resource_subscriptions_ids(nerm, [sc_rx, sc_tx])
        assert 2 == len(subs_conf_ids)
        rxtx_db.update(nerm, subs_conf_ids)

        assert sum(deltas_rx[:4]) == \
               rxtx_db.get_rx_gb(sc_rx['id'], 'ne/1', 'eth0')
        assert sum(deltas_tx[:4]) == \
               rxtx_db.get_tx_gb(sc_tx['id'], 'ne/1', 'eth0')

        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)

        assert None is nem.network_rx_above_thld(sc_rx)
        assert True is rxtx_db.get_above_thld(sc_rx['id'], 'ne/1', 'eth0', 'rx')

        assert {'interface': 'eth0', 'value': sum(deltas_tx[:4])} == \
               nem.network_tx_above_thld(sc_tx)
        assert True is rxtx_db.get_above_thld(sc_tx['id'], 'ne/1', 'eth0', 'tx')

    def test_match_rx_full_workflow(self):
        """
        Workflow of Rx stream of NE network interface non-monotonically
        increasing (with zeroing) and going above threshold defined in the
        subscription configuration.
        """

        rxtx_db = RxTxDB(self.driver)

        sc = SubscriptionConfig({
            'criteria': {
                'metric': 'foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeMetrics({})
        rxtx_db.update(nerm, ['sub/01'])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        assert None is nem.network_tx_above_thld(sc)

        sc = SubscriptionConfig({
            'id': 'subs/01',
            'criteria': {
                'metric': 'network-rx',
                'condition': '>',
                'value': '5',
                'kind': 'numeric'
            }})
        nerm = NuvlaEdgeMetrics({})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': 1 * 1024 ** 2,
                 'bytes-received': 2 * 1024 ** 2}]}})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.get_data(sc['id'], 'ne/1', 'eth0', 'rx')
        assert 0 == rx.total()

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': gb_to_bytes(1),
                 'bytes-received': gb_to_bytes(2)}]}})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.get_data(sc['id'], 'ne/1', 'eth0', 'rx')
        assert 2.0 == bytes_to_gb(rx.total())

        # counter reset
        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': 0,
                 'bytes-received': 300 * 1024 ** 2}]}})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.get_data(sc['id'], 'ne/1', 'eth0', 'rx')
        assert 2.3 == round(bytes_to_gb(rx.total()), 1)

        # above threshold
        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': 0,
                 'bytes-received': gb_to_bytes(4)}]}})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        value_bytes = gb_to_bytes(6)
        value_gb = bytes_to_gb(value_bytes)
        assert {'interface': 'eth0', 'value': value_gb} == \
               nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.get_data(sc['id'], 'ne/1', 'eth0', 'rx')
        assert value_gb == bytes_to_gb(rx.total())

        # continuing above threshold, but not reporting it.
        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': 0,
                 'bytes-received': gb_to_bytes(5)}]}})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        value_bytes = gb_to_bytes(7)
        value_gb = bytes_to_gb(value_bytes)
        assert None is nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.get_data(sc['id'], 'ne/1', 'eth0', 'rx')
        assert value_gb == bytes_to_gb(rx.total())

        # artificially resetting network above threshold triggers the
        # network above threshold again when new metrics come.
        rxtx_db.reset_above_thld(sc['id'], nerm['id'], 'eth0', 'rx')
        rx: RxTx = rxtx_db.get_data(sc['id'], nerm['id'], 'eth0', 'rx')
        assert False is rx.get_above_thld()

        nerm = NuvlaEdgeMetrics({
            'id': 'ne/1',
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
            'RESOURCES': {'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': 0,
                 'bytes-received': gb_to_bytes(6)}]}})
        rxtx_db.update(nerm, [sc['id']])
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        value_gb = bytes_to_gb(gb_to_bytes(8))
        assert {'interface': 'eth0', 'value': value_gb} == \
               nem.network_rx_above_thld(sc)
        rx: RxTx = rxtx_db.get_data(sc['id'], nerm['id'], 'eth0', 'rx')
        assert True is rx.get_above_thld()
        assert value_gb == bytes_to_gb(rx.total())

    def test_net_multiple_successive_subs_triggered(self):
        """
        This tests dependence of the 'above threshold' flag in the network
        DB on the individual subscription configurations.
        """

        sc1 = SubscriptionConfig({
            'id': 'subscription-config/01',
            'acl': {'owners': ['user/01']},
            'category': 'notification',
            'criteria': {'condition': '>',
                         'dev-name': 'eth0',
                         'kind': 'numeric',
                         'metric': 'network-rx',
                         'value': '2.5'},
            'name': 'Rx > 2.5gb',
            'description': 'Rx > 2.5gb',
            'enabled': True,
            'method-ids': [
                'notification-method/419a0079-d4d4-4893-a6b5-f7645dd8fe59',
                'notification-method/d3e800d5-79df-4af0-b57e-23d80b6152a5'],
            'resource-filter': "tags='nuvlabox=True'",
            'resource-kind': 'nuvlabox',
            'resource-type': 'subscription-config'})
        sc2 = SubscriptionConfig({
            'id': 'subscription-config/02',
            'acl': {'owners': ['user/02']},
            'category': 'notification',
            'criteria': {'condition': '>',
                         'kind': 'numeric',
                         'metric': 'network-rx',
                         'value': '3.5'},
            'name': 'Rx > 3.5gb',
            'description': 'Rx > 3.5gb',
            'enabled': True,
            'method-ids': [
                'notification-method/419a0079-d4d4-4893-a6b5-f7645dd8fe59'],
            'resource-filter': "tags='nuvlabox=True'",
            'resource-kind': 'nuvlabox',
            'resource-type': 'subscription-config'})
        sc3 = SubscriptionConfig({
            'id': 'subscription-config/03',
            'acl': {'owners': ['user/03']},
            'category': 'notification',
            'criteria': {'condition': '>',
                         'kind': 'numeric',
                         'metric': 'network-rx',
                         'value': '9.5'},
            'name': 'Rx > 9.5gb',
            'description': 'Rx > 9.5gb',
            'enabled': True,
            'method-ids': [
                'notification-method/419a0079-d4d4-4893-a6b5-f7645dd8fe59'],
            'resource-filter': "tags='nuvlabox=True'",
            'resource-kind': 'nuvlabox',
            'resource-type': 'subscription-config'})

        rxtx_db = RxTxDB(self.driver)
        nerm = NuvlaEdgeMetrics({
            'id': 'nuvlabox/01',
            'NAME': 'NuvlaEdge Test',
            'DESCRIPTION': 'NuvlaEdge Test 2',
            'TAGS': ['nuvlabox=True'],
            'ONLINE': True, 'ONLINE_PREV': True,
            'NETWORK': {'default-gw': 'eth0'},
            'TIMESTAMP': '2022-08-02T15:21:46Z',
            'RESOURCES': {
                'net-stats': [
                    {'interface': 'eth0',
                     'bytes-transmitted': 57112506,
                     'bytes-received': gb_to_bytes(1)},
                    {'interface': 'docker0',
                     'bytes-transmitted': 0,
                     'bytes-received': 0}]},
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'user/02',
                        'user/03']}})

        subs_conf_ids = TaggedResourceSubsConfigMatcher()\
            .resource_subscriptions_ids(nerm, [sc1, sc2, sc3])

        # set baseline
        rxtx_db.update(nerm, subs_conf_ids)

        # increment to go above thresholds defined for subs 1 and 2
        rxtx_db = RxTxDB(self.driver)
        nerm = NuvlaEdgeMetrics({
            'id': 'nuvlabox/01',
            'NAME': 'NuvlaEdge Test',
            'DESCRIPTION': 'NuvlaEdge Test 2',
            'TAGS': ['nuvlabox=True'],
            'ONLINE': True, 'ONLINE_PREV': True,
            'NETWORK': {'default-gw': 'eth0'},
            'TIMESTAMP': '2022-08-02T15:21:46Z',
            'RESOURCES': {
                'net-stats': [
                    {'interface': 'eth0',
                     'bytes-transmitted': 57112506,
                     'bytes-received': gb_to_bytes(6)},
                    {'interface': 'docker0',
                     'bytes-transmitted': 0,
                     'bytes-received': 0}]},
            'ACL': {'owners': ['group/nuvla-admin'],
                    'view-data': [
                        'nuvlabox/1c56dc02-0c16-4423-a4b1-9265d855621d',
                        'user/01',
                        'user/02',
                        'user/03']}})
        rxtx_db.update(nerm, subs_conf_ids)

        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        match_cs1 = nem.match_all([sc1])
        assert 1 == len(match_cs1)
        assert True is rxtx_db.get_above_thld(sc1['id'], nerm['id'], 'eth0', 'rx')
        assert False is rxtx_db.get_above_thld(sc2['id'], nerm['id'], 'eth0', 'rx')
        assert False is rxtx_db.get_above_thld(sc3['id'], nerm['id'], 'eth0', 'rx')
        assert sc1['id'] == match_cs1[0]['id']

        match_cs2 = nem.match_all([sc2])
        assert 1 == len(match_cs2)
        assert True is rxtx_db.get_above_thld(sc1['id'], nerm['id'], 'eth0', 'rx')
        assert True is rxtx_db.get_above_thld(sc2['id'], nerm['id'], 'eth0', 'rx')
        assert False is rxtx_db.get_above_thld(sc3['id'], nerm['id'], 'eth0', 'rx')
        assert sc2['id'] == match_cs2[0]['id']

        match_cs3 = nem.match_all([sc3])
        assert 0 == len(match_cs3)
        assert True is rxtx_db.get_above_thld(sc1['id'], nerm['id'], 'eth0', 'rx')
        assert True is rxtx_db.get_above_thld(sc2['id'], nerm['id'], 'eth0', 'rx')
        assert False is rxtx_db.get_above_thld(sc3['id'], nerm['id'], 'eth0', 'rx')

        # now go above threshold for the subs 3
        nerm['RESOURCES'] = {
            'net-stats': [
                {'interface': 'eth0',
                 'bytes-transmitted': 57112506,
                 'bytes-received': gb_to_bytes(11)},
                {'interface': 'docker0',
                 'bytes-transmitted': 0,
                 'bytes-received': 0}]}
        rxtx_db.update(nerm, subs_conf_ids)
        nem = NuvlaEdgeSubsConfMatcher(nerm, rxtx_db)
        match_all = nem.match_all([sc1, sc2, sc3])
        assert 1 == len(match_all)
        assert True is rxtx_db.get_above_thld(sc1['id'], nerm['id'], 'eth0', 'rx')
        assert True is rxtx_db.get_above_thld(sc2['id'], nerm['id'], 'eth0', 'rx')
        assert True is rxtx_db.get_above_thld(sc3['id'], nerm['id'], 'eth0', 'rx')
        assert sc3['id'] == match_all[0]['id']

    def test_match_all_none(self):
        sc = SubscriptionConfig({
            'id': 'foo',
            'criteria': {
                'metric': 'foo',
                'condition': 'bar',
                'value': '0',
                'kind': 'numeric'}})
        nerm = NuvlaEdgeMetrics({})
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
        nerm = NuvlaEdgeMetrics({
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
        nerm1 = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['x86-64'],
             'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
             'ONLINE': True,
             'ONLINE_PREV': True,
             'RESOURCES': {'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'},
                           'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'},
                           'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'disk0p1'},
                                     {'used': 9.1, 'capacity': 10, 'device': 'disk0p2'}],
                           'net-stats': [
                               {'interface': 'eth0',
                                'bytes-transmitted': 0,
                                'bytes-received': 0},
                               {'interface': 'lo',
                                'bytes-transmitted': 0,
                                'bytes-received': 0},
                               {'interface': 'wlan1',
                                'bytes-transmitted': 0,
                                'bytes-received': 0}
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

        nerm2 = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['x86-64'],
             'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
             'ONLINE': True,
             'ONLINE_PREV': True,
             'RESOURCES': {'CPU': {'load': 9.1, 'capacity': 10, 'topic': 'cpu'},
                           'RAM': {'used': 901, 'capacity': 1000, 'topic': 'ram'},
                           'DISKS': [{'used': 9.1, 'capacity': 10, 'device': 'disk0p1'},
                                     {'used': 9.1, 'capacity': 10, 'device': 'disk0p2'}],
                           'net-stats': [
                               {'interface': 'eth0',
                                'bytes-transmitted': gb_to_bytes(5),
                                'bytes-received': 0},
                               {'interface': 'lo',
                                'bytes-transmitted': 0,
                                'bytes-received': 0},
                               {'interface': 'wlan1',
                                'bytes-transmitted': 0,
                                'bytes-received': gb_to_bytes(5)}
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

        subs_confs = [sc_disk, sc_load, sc_ram, sc_rx_wlan1, sc_tx_gw]

        subs_confs_ids = TaggedResourceNetSubsConfigMatcher() \
            .resource_subscriptions_ids(nerm1, subs_confs)

        net_db = RxTxDB(self.driver)

        # Minimum two updates are required to detect the increase in the Rx/Tx.
        net_db.update(nerm1, subs_confs_ids)
        net_db.update(nerm2, subs_confs_ids)

        nescm = NuvlaEdgeSubsConfMatcher(nerm2, net_db)
        res = nescm.match_all(subs_confs)
        ids_res = list(map(lambda x: x['id'], res))
        ids = set(f'subscription-config/0{i}' for i in range(1, len(subs_confs) + 1))
        assert set() == ids.difference(ids_res)

    def test_net_rxtx_lifecycle_no_retrigger(self):

        thold = 4.5

        # User sets 4.5gb threshold for the notification.
        sc_rx_wlan1 = SubscriptionConfig({
            'id': 'subscription-config/01',
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
                'value': str(thold),
                'kind': 'numeric',
                'dev-name': 'eth0',
                'window': '1d'
            }})

        nerm0 = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['x86-64'],
             'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
             'ONLINE': True,
             'ONLINE_PREV': True,
             'RESOURCES': {'net-stats': [
                 {'interface': 'eth0',
                  'bytes-transmitted': 0,
                  'bytes-received': 0}]},
             'RESOURCES_PREV': {},
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': ['me'],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})
        ne_subs_ids = [sc_rx_wlan1['id']]
        net_db = RxTxDB(self.driver)
        net_db.update(nerm0, ne_subs_ids)

        # NuvlaEdge sends metric with value of 4.6gb total received data.
        # This is 110mb above the threshold.
        nerm1 = NuvlaEdgeMetrics(
            {'id': 'nuvlabox/01',
             'NAME': 'Nuvlabox TBL Münchwilen AG Zürcherstrasse #1',
             'DESCRIPTION': 'None - self-registration number 220171415421241',
             'TAGS': ['x86-64'],
             'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'eth0'},
             'ONLINE': True,
             'ONLINE_PREV': True,
             'RESOURCES': {'net-stats': [
                               {'interface': 'eth0',
                                'bytes-transmitted': 0,
                                'bytes-received': gb_to_bytes(thold + 0.1)}]},
             'RESOURCES_PREV': {},
             'TIMESTAMP': '2022-08-02T15:21:46Z',
             'ACL': {'owners': ['me'],
                     'view-data': ['group/elektron',
                                   'infrastructure-service/eb8e09c2-8387-4f6d-86a4-ff5ddf3d07d7',
                                   'nuvlabox/ac81118b-730b-4df9-894c-f89e50580abd']}})

        net_db.update(nerm1, ne_subs_ids)
        nescm = NuvlaEdgeSubsConfMatcher(nerm1, net_db)

        # The network metric matched the threshold condition.
        res = nescm.match_all([sc_rx_wlan1])
        print(res)
        assert 1 == len(res)
        assert res[0]['subs_description'] == 'NE network Rx'
        assert res[0]['timestamp'] == '2022-08-02T15:21:46Z'

        # NuvlaEdge sends metric with value of 5.0gb total received data.
        # This makes increment of 110mb and total 220mb above the 4.5 threshold.
        nerm1['RESOURCES']['net-stats'][0]['bytes-received'] = \
            gb_to_bytes(thold + 0.2)
        net_db.update(nerm1, ne_subs_ids)

        # Because we already matched the condition (and it was persisted), the
        # new match will not be triggered.
        nescm = NuvlaEdgeSubsConfMatcher(nerm1, net_db)
        res = nescm.match_all([sc_rx_wlan1])
        assert 0 == len(res)

        # The current window hasn't closed yet, but the user increases the
        # threshold to 5gb.
        thold = 5.5
        sc_rx_wlan1['criteria']['value'] = str(thold)

        resource = (nerm1['id'], 'eth0', 'rx')

        # The metric that comes is below the newly set threshold.
        # Result:
        # a) the metric is not matched,
        # b) "above_thold" flag on the RxTx value in the network DB is reset.
        nerm1['RESOURCES']['net-stats'][0]['bytes-received'] = \
            gb_to_bytes(thold - 0.5)
        net_db.update(nerm1, ne_subs_ids)

        nescm = NuvlaEdgeSubsConfMatcher(nerm1, net_db)
        res = nescm.match_all([sc_rx_wlan1])
        assert 0 == len(res)
        assert False is net_db.get_above_thld(sc_rx_wlan1['id'], *resource)

        # Metric goes above the new threshold and it is matched.
        nerm1['RESOURCES']['net-stats'][0]['bytes-received'] = \
            gb_to_bytes(thold + 0.1)
        net_db.update(nerm1, ne_subs_ids)

        nescm = NuvlaEdgeSubsConfMatcher(nerm1, net_db)
        # The network metric matched the threshold condition.
        res = nescm.match_all([sc_rx_wlan1])
        assert 1 == len(res)
        assert res[0]['subs_description'] == 'NE network Rx'
        assert res[0]['timestamp'] == '2022-08-02T15:21:46Z'
        assert True is net_db.get_above_thld(sc_rx_wlan1['id'], *resource)


class TestNuvlaEdgeSubsConfMatcherDBES(TestRxTxDriverESMockedBase,
                                       TestNuvlaEdgeSubsConfMatcherDBInMem):
    pass
