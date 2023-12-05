import unittest

from nuvla.notifs.matching.base import gt, lt, ResourceSubsCfgMatcher, \
    TaggedResourceSubsCfgMatcher
from nuvla.notifs.models.resource import Resource
from nuvla.notifs.models.subscription import SubscriptionCfg, RequiredAttributedMissing


def mb_to_bytes(n):
    return n * 1024 ** 2


class TestUtils(unittest.TestCase):

    def test_gt_lt(self):
        for fun in [gt, lt]:
            self.assertRaises(RequiredAttributedMissing, fun, None,
                              SubscriptionCfg())
            self.assertRaises(RequiredAttributedMissing, fun, 1,
                              SubscriptionCfg())
            self.assertRaises(RequiredAttributedMissing, fun, 1,
                              SubscriptionCfg({'criteria': {}}))
            # kind is required attribute
            self.assertRaises(RequiredAttributedMissing, fun, 1,
                              SubscriptionCfg({'criteria': {'value': 1}}))

        assert False is gt(0, SubscriptionCfg({'criteria': {'value': 1,
                                                            'kind': 'integer'}}))

        assert True is lt(0, SubscriptionCfg({'criteria': {'value': 1,
                                                           'kind': 'integer'}}))

        assert False is gt(0.0, SubscriptionCfg({'criteria': {'value': 0.0,
                                                              'kind': 'integer'}}))

        assert False is lt(0.0, SubscriptionCfg({'criteria': {'value': 0.0,
                                                              'kind': 'integer'}}))


class TestResourceSubsConfigMatcher(unittest.TestCase):

    def test_init(self):
        rscm = ResourceSubsCfgMatcher()
        assert False is rscm.resource_subscribed(Resource({}),
                                                 SubscriptionCfg({}))

    def test_timestamps(self):
        r = Resource({})
        self.assertRaises(KeyError, r.timestamp)
        self.assertRaises(KeyError, r.nuvla_timestamp)

        r = Resource({'timestamp': '', 'nuvla_timestamp': ''})
        assert '' == r.timestamp()
        assert '' == r.nuvla_timestamp()

        r = Resource({'timestamp': '2022-08-02T15:25:01Z',
                      'nuvla_timestamp': '2022-08-02T15:25:02Z'})
        assert '2022-08-02T15:25:01Z' == r.timestamp()
        assert '2022-08-02T15:25:02Z' == r.nuvla_timestamp()

        r = Resource({'timestamp': '2022-08-02T15:25:01.123Z',
                      'nuvla_timestamp': '2022-08-02T15:25:02Z'})
        assert '2022-08-02T15:25:01Z' == r.timestamp()
        assert '2022-08-02T15:25:02Z' == r.nuvla_timestamp()

        r = Resource({'timestamp': '2022-08-02T15:25:01Z',
                      'nuvla_timestamp': '2022-08-02T15:25:02.999Z'})
        assert '2022-08-02T15:25:01Z' == r.timestamp()
        assert '2022-08-02T15:25:02Z' == r.nuvla_timestamp()

        r = Resource({'timestamp': '2022-08-02T15:25:01Z'})
        assert '2022-08-02T15:25:01Z' == r.timestamp()
        assert '2022-08-02T15:25:01Z' == r.nuvla_timestamp()

        r = Resource({'nuvla_timestamp': '2022-08-02T15:25:02.777Z'})
        self.assertRaises(KeyError, r.timestamp)
        assert '2022-08-02T15:25:02Z' == r.nuvla_timestamp()

    def test_resource_subscribed(self):
        r = Resource({'acl': {'owners': ['me']},
                      'foo': ['bar']})

        scm = ResourceSubsCfgMatcher()

        sc = SubscriptionCfg({'enabled': True})
        assert False is scm.resource_subscribed(r, sc)

        sc = SubscriptionCfg({
            'enabled': False,
            'acl': {'owners': ['me']}})
        assert False is scm.resource_subscribed(r, sc)

        sc = SubscriptionCfg({
            'enabled': True,
            'acl': {'owners': ['me']}})
        assert True is scm.resource_subscribed(r, sc)

    def test_resource_subscription(self):
        r = Resource()
        assert [] == \
               list(ResourceSubsCfgMatcher()
                    .resource_subscriptions(r,
                                            [SubscriptionCfg({}),
                                             SubscriptionCfg(
                                                 {'enabled': True}),
                                             SubscriptionCfg({
                                                 'enabled': True,
                                                 'acl': {'owners': ['me']}}),
                                             SubscriptionCfg({
                                                 'enabled': True,
                                                 'resource-filter': "tags='foo'",
                                                 'acl': {'owners': ['me']}})]))

        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})
        rscm = ResourceSubsCfgMatcher()
        scs = [SubscriptionCfg({}),
               SubscriptionCfg(
                   {'enabled': True}),
               SubscriptionCfg({
                   'enabled': True,
                   'acl': {'owners': ['me']}}),
               SubscriptionCfg({
                   'enabled': False,
                   'acl': {'owners': ['me']}})]
        assert [SubscriptionCfg({
            'enabled': True,
            'acl': {'owners': ['me']}})] == list(rscm.resource_subscriptions(r, scs))

    def test_resource_subs_ids(self):
        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})
        rscm = ResourceSubsCfgMatcher()
        scs = [SubscriptionCfg({'id': 'subs/01'}),
               SubscriptionCfg(
                   {'enabled': True,
                    'id': 'subs/02'}),
               SubscriptionCfg({
                   'id': 'subs/03',
                   'enabled': True,
                   'acl': {'owners': ['me']}}),
               SubscriptionCfg({
                   'id': 'subs/04',
                   'enabled': False,
                   'acl': {'owners': ['me']}})]
        assert ['subs/03'] == list(rscm.resource_subscriptions_ids(r, scs))


class TestTaggedResourceSubsConfigMatcher(unittest.TestCase):

    def test_init(self):
        trscm = TaggedResourceSubsCfgMatcher()
        assert False is trscm.resource_subscribed(Resource({}),
                                                  SubscriptionCfg({}))

    def test_resource_subscribed(self):
        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})

        trscm = TaggedResourceSubsCfgMatcher()

        sc = SubscriptionCfg({'enabled': True})
        assert False is trscm.resource_subscribed(r, sc)

        sc = SubscriptionCfg({
            'enabled': False,
            'acl': {'owners': ['me']}})
        assert False is trscm.resource_subscribed(r, sc)

        sc = SubscriptionCfg({
            'enabled': True,
            'resource-filter': "tags='foo'",
            'acl': {'owners': ['me']}})
        assert True is trscm.resource_subscribed(r, sc)

        sc = SubscriptionCfg({
            'enabled': True,
            'resource-filter': "tags='bar'",
            'acl': {'owners': ['me']}})
        assert False is trscm.resource_subscribed(r, sc)

    def test_resource_subscription(self):
        r = Resource()
        assert [] == \
               list(TaggedResourceSubsCfgMatcher()
                    .resource_subscriptions(r,
                                            [SubscriptionCfg({}),
                                             SubscriptionCfg(
                                                 {'enabled': True}),
                                             SubscriptionCfg({
                                                 'enabled': True,
                                                 'acl': {'owners': ['me']}}),
                                             SubscriptionCfg({
                                                 'enabled': True,
                                                 'resource-filter': "tags='foo'",
                                                 'acl': {'owners': ['me']}})]))

        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo']})
        scm = TaggedResourceSubsCfgMatcher()
        sc1 = SubscriptionCfg({
            'enabled': True,
            'acl': {'owners': ['me']}})
        sc2 = SubscriptionCfg({
            'enabled': True,
            'resource-filter': "tags='foo'",
            'acl': {'owners': ['me']}})
        scs = [SubscriptionCfg({}),
               SubscriptionCfg(
                   {'enabled': True}),
               sc1,
               sc2]
        assert [sc1, sc2] == list(scm.resource_subscriptions(r, scs))

    def test_resource_subs_ids(self):
        r = Resource({'acl': {'owners': ['me']},
                      'tags': ['foo', 'bar']})
        rscm = TaggedResourceSubsCfgMatcher()
        scs = [SubscriptionCfg({'id': 'subs/01'}),
               SubscriptionCfg(
                   {'enabled': True,
                    'id': 'subs/02'}),
               SubscriptionCfg({
                   'id': 'subs/03',
                   'enabled': True,
                   'resource-filter': "tags='foo'",
                   'acl': {'owners': ['me']}}),
               SubscriptionCfg({
                   'id': 'subs/04',
                   'enabled': False,
                   'resource-filter': "tags='foo'",
                   'acl': {'owners': ['me']}}),
               SubscriptionCfg({
                   'id': 'subs/05',
                   'enabled': True,
                   'resource-filter': "tags='bar'",
                   'acl': {'owners': ['me']}})]
        assert ['subs/03', 'subs/05'] == list(rscm.resource_subscriptions_ids(r, scs))

