import os
import unittest

from es_patch import es_index_put_mapping
import elasticmock.fake_indices
elasticmock.fake_indices.FakeIndicesClient.put_mapping = es_index_put_mapping

from es_patch import get_elasticmock
elasticmock = get_elasticmock()

from fake_updater import get_updater
from nuvla.notifs.common import es_hosts, ES_HOSTS
from nuvla.notifs.main import populate_ne_net_db, \
    wait_sc_populated
from nuvla.notifs.models.metric import NuvlaEdgeMetrics
from nuvla.notifs.models.subscription import SubscriptionCfg, \
    RESOURCE_KIND_NE, RESOURCE_KIND_APPLICATION, SelfUpdatingSubsCfgs
from nuvla.notifs.models.dyndict import SelfUpdatingDict
from nuvla.notifs.db.driver import RxTxDB


class TestEsHosts(unittest.TestCase):

    def test_no_env(self):
        assert ES_HOSTS == es_hosts()

    def test_with_env(self):
        os.environ['ES_HOSTS'] = 'es:9200'
        assert [{'host': 'es', 'port': 9200}] == es_hosts()

        os.environ['ES_HOSTS'] = 'es1:9201,es2:9202'
        assert [{'host': 'es1', 'port': 9201},
                {'host': 'es2', 'port': 9202}] == es_hosts()


class TestPopulateNetDB(unittest.TestCase):

    @elasticmock
    def test_empty(self):
        net_db = RxTxDB()
        assert net_db.is_connected()
        populate_ne_net_db(net_db, NuvlaEdgeMetrics({}), [])
        assert 0 == len(net_db)

    @elasticmock
    def test_populated(self):
        net_db = RxTxDB()
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
                               'bytes-transmitted': 1,
                               'bytes-received': 1},
                              {'interface': 'lo',
                               'bytes-transmitted': 63742086112,
                               'bytes-received': 63742086112
                               }]},
            'RESOURCES_PREV': {
                'CPU': {'load': 3.0, 'capacity': 4, 'topic': 'cpu'}}})
        subs_cfgs_data = [('1-2-3-4', {'resource-kind': RESOURCE_KIND_NE,
                                       'criteria': {'metric': 'network-rx'},
                                       'id': '1-2-3-4',
                                       'resource-filter': "tags='nuvlabox=True'",
                                       'acl': {'owners': ['me']}}),
                          ('a-b-c-d', {'resource-kind': RESOURCE_KIND_NE,
                                       'criteria': {'metric': 'network-tx'},
                                       'id': 'a-b-c-d',
                                       'resource-filter': "tags='nuvlabox=True'",
                                       'acl': {'owners': ['me']}})]
        sud = SelfUpdatingDict('test', get_updater(subs_cfgs_data),
                               SubscriptionCfg)
        sud.wait_key_set(RESOURCE_KIND_NE)
        sud.wait_not_empty()
        populate_ne_net_db(net_db, nerm, sud[RESOURCE_KIND_NE].values())
        assert 8 == len(net_db)

    def test_wait_populated(self):
        assert False is wait_sc_populated(
            SelfUpdatingSubsCfgs('foo', get_updater()), ['foo', 'bar', 'baz'],
            sleep=.1, timeout=.1)

        subs_cfgs_data = [('1-2-3-4', {'resource-kind': RESOURCE_KIND_NE,
                                       'criteria': {'metric': 'network-rx'},
                                       'id': '1-2-3-4',
                                       'resource-filter': "tags='nuvlabox=True'",
                                       'acl': {'owners': ['me']}}),
                          ('a-b-c-d', {'resource-kind': RESOURCE_KIND_NE,
                                       'criteria': {'metric': 'network-tx'},
                                       'id': 'a-b-c-d',
                                       'resource-filter': "tags='nuvlabox=True'",
                                       'acl': {'owners': ['me']}}),
                          ('A-B-C-D', {'resource-kind': RESOURCE_KIND_APPLICATION,
                                       'criteria': {'metric': 'app-metric'},
                                       'id': 'A-B-C-D',
                                       'acl': {'owners': ['me']}})]
        sud = SelfUpdatingDict('test', get_updater(subs_cfgs_data),
                               SubscriptionCfg)
        assert True is wait_sc_populated(sud,
            [RESOURCE_KIND_APPLICATION, RESOURCE_KIND_NE], sleep=.1, timeout=.1)
