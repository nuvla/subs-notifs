import unittest

from fake_updater import get_updater
from nuvla.notifs.models.subscription import SubscriptionCfg, \
    RESOURCE_KINDS, RESOURCE_KIND_NE, RESOURCE_KIND_DATARECORD
from nuvla.notifs.models.dyndict import LoggingDict, SelfUpdatingDict


class TestLoggingDict(unittest.TestCase):

    def test_empty_update(self):
        ld = LoggingDict('test')
        assert ld.empty()
        ld.update(foo='bar')
        assert not ld.empty()


class TestSubscriptionConfig(unittest.TestCase):

    def test_init(self):
        sc = SubscriptionCfg({'foo': 'bar'})
        assert 'bar' == sc['foo']
        assert False is sc.is_enabled()
        assert False is sc.can_view_resource({})

    def test_can_view(self):
        sc = SubscriptionCfg({'acl': {'owners': ['me']}})
        assert True is sc.can_view_resource({'owners': ['me']})

        sc = SubscriptionCfg({'acl': {'owners': ['you']}})
        assert True is sc.can_view_resource({'owners': ['you'],
                                             'view-data': ['me']})
        assert True is sc.can_view_resource({'owners': ['me'],
                                             'view-data': ['you']})

    def test_tags_from_resource_filter(self):
        sc = SubscriptionCfg({'resource-filter': "tags='foo'"})
        assert ['foo'] == sc._tags_from_resource_filter()
        assert True is sc.is_tags_set()

        sc = SubscriptionCfg({'resource-filter': ''})
        assert 0 == len(sc._tags_from_resource_filter())
        assert False is sc.is_tags_set()

        sc = SubscriptionCfg({})
        assert 0 == len(sc._tags_from_resource_filter())
        assert False is sc.is_tags_set()

    def test_tags_match(self):
        sc = SubscriptionCfg({})
        assert False is sc.tags_match(None)
        assert False is sc.tags_match([])
        assert False is sc.tags_match(['foo'])

        sc = SubscriptionCfg({'resource-filter': "tags='foo'"})
        assert True is sc.tags_match(['foo'])
        assert True is sc.tags_match(['foo', 'bar'])
        assert False is sc.tags_match(['baz'])

    def test_is_metric_cond(self):
        sc = SubscriptionCfg({'criteria': {'metric': 'foo',
                                           'condition': 'bar'}})
        assert False is sc.is_metric_cond('baz', 'toto')
        assert False is sc.is_metric_cond('foo', 'baz')
        assert True is sc.is_metric_cond('foo', 'bar')

    def test_dict(self):
        sc = SubscriptionCfg({'foo': 'bar'})
        assert 'bar' == sc.foo


class TestSelfUpdatingDict(unittest.TestCase):

    def test_init(self):
        sud = SelfUpdatingDict('test', get_updater(), object)
        assert 0 == len(sud)

    def test_update(self):
        data = [('1-2-3-4', {'resource-kind': RESOURCE_KIND_NE,
                             'id': '1-2-3-4'}),
                ('2-3-4-5', {'resource-kind': RESOURCE_KIND_DATARECORD,
                             'id': '2-3-4-5'})]
        sud = SelfUpdatingDict('test', get_updater(data),
                               SubscriptionCfg)
        sud.wait_key_set(RESOURCE_KIND_NE)
        sud.wait_key_set(RESOURCE_KIND_DATARECORD)
        sud.wait_not_empty()

        assert 2 == len(sud)
        assert RESOURCE_KIND_NE in sud.keys() and \
               RESOURCE_KIND_DATARECORD in sud.keys()

        for val in sud.values():
            for k, v in val.items():
                assert isinstance(v, SubscriptionCfg)

    def test_resource_kind_values(self):
        sc_ne1 = {'resource-kind': RESOURCE_KIND_NE, 'id': '1-2-3-4'}
        sc_ne2 = {'resource-kind': RESOURCE_KIND_NE, 'id': 'a-b-c-d'}
        sc_dr = {'resource-kind': RESOURCE_KIND_DATARECORD, 'id': '2-3-4-5'}
        data = [('1-2-3-4', sc_ne1),
                ('a-b-c-d', sc_ne2),
                ('2-3-4-5', sc_dr)]
        sud = SelfUpdatingDict('test', get_updater(data), SubscriptionCfg)
        sud.wait_key_set(RESOURCE_KIND_NE)
        sud.wait_key_set(RESOURCE_KIND_DATARECORD)
        sud.wait_not_empty()
        assert [] == sud.get_resource_kind_values('foo')
        vals = sud.get_resource_kind_values(RESOURCE_KIND_NE)
        for v in vals:
            assert v['id'] in ('1-2-3-4', 'a-b-c-d')
        assert 2 == len(vals)


class TestResourceKinds(unittest.TestCase):

    def test_resource_kinds(self):
        assert 0 < len(RESOURCE_KINDS)
