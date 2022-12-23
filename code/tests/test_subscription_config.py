import unittest
from typing import Dict

from nuvla.notifs.subscription import LoggingDict, SubscriptionConfig, \
    SelfUpdatingDict
from nuvla.notifs.updater import DictUpdater


class TestLoggingDict(unittest.TestCase):

    def test_empty_update(self):
        ld = LoggingDict('test')
        assert ld.empty()
        ld.update(foo='bar')
        assert not ld.empty()


class TestSubscriptionConfig(unittest.TestCase):

    def test_init(self):
        sc = SubscriptionConfig({'foo': 'bar'})
        assert 'bar' == sc['foo']
        assert False is sc.is_enabled()
        assert False is sc.can_view_resource({})

    def test_can_view(self):
        sc = SubscriptionConfig({'acl': {'owners': ['me']}})
        assert True is sc.can_view_resource({'owners': ['me']})

        sc = SubscriptionConfig({'acl': {'owners': ['you']}})
        assert True is sc.can_view_resource({'owners': ['you'],
                                             'view-data': ['me']})
        assert True is sc.can_view_resource({'owners': ['me'],
                                             'view-data': ['you']})

    def test_tags_from_resource_filter(self):
        sc = SubscriptionConfig({'resource-filter': "tags='foo'"})
        assert ['foo'] == sc._tags_from_resource_filter()

        sc = SubscriptionConfig({'resource-filter': ''})
        assert 0 == len(sc._tags_from_resource_filter())

        sc = SubscriptionConfig({})
        assert 0 == len(sc._tags_from_resource_filter())

    def test_tags_match(self):
        sc = SubscriptionConfig({})
        assert False is sc.tags_match(None)
        assert False is sc.tags_match([])
        assert False is sc.tags_match(['foo'])

        sc = SubscriptionConfig({'resource-filter': "tags='foo'"})
        assert True is sc.tags_match(['foo'])
        assert True is sc.tags_match(['foo', 'bar'])
        assert False is sc.tags_match(['baz'])

    def test_is_metric_cond(self):
        sc = SubscriptionConfig({'criteria': {'metric': 'foo',
                                              'condition': 'bar'}})
        assert False is sc.is_metric_cond('baz', 'toto')
        assert False is sc.is_metric_cond('foo', 'baz')
        assert True is sc.is_metric_cond('foo', 'bar')

    def test_dict(self):
        sc = SubscriptionConfig({'foo': 'bar'})
        assert 'bar' == sc.foo


class TestSelfUpdatingDict(unittest.TestCase):

    def test_init(self):
        class FakeUpdater(DictUpdater):
            def do_yield(self) -> Dict[str, dict]:
                yield {None: None}

        sud = SelfUpdatingDict('test', FakeUpdater(), object)
        assert 0 == len(sud)

    def test_update(self):
        class Updater(DictUpdater):

            data = [('1-2-3-4', {'resource-kind': 'nuvlaedge',
                                 'id': '1-2-3-4'}),
                    ('2-3-4-5', {'resource-kind': 'data-record',
                                 'id': '2-3-4-5'})]

            def do_yield(self) -> Dict[str, dict]:
                for k, v in self.data:
                    yield {k: v}

        sud = SelfUpdatingDict('test', Updater(), SubscriptionConfig)
        sud.wait_key_set('nuvlaedge')
        sud.wait_key_set('data-record')
        sud.wait_not_empty()

        assert 2 == len(sud)
        assert 'nuvlaedge' in sud.keys() and 'data-record' in sud.keys()

        for val in sud.values():
            for k, v in val.items():
                assert isinstance(v, SubscriptionConfig)
