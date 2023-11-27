import unittest

from nuvla.notifs.matching.event import EventSubsCfgMatcher
from nuvla.notifs.models.event import Event
from nuvla.notifs.models.subscription import SubscriptionCfg


class TestBlackboxSubsCfgMatcher(unittest.TestCase):

    def test_filter_subs_cfgs(self):
        subs_cfg = SubscriptionCfg(
            {
                'description': 'BB',
                'category': 'notification',
                'method-ids': [
                    'notification-method/b004854e-d00a-4ff2-8293-343748eb2774',
                    'notification-method/58996b63-3a47-42cc-8cff-bd84248bb000'
                ],
                'name': 'BB',
                'criteria': {
                    'metric': 'tag',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'application/blackbox'
                },
                'id': 'subscription-config/031a9699-46e6-453b-b9a0-406b272b01a7',
                'resource-type': 'subscription-config',
                'acl': {
                    'edit-data': [
                        'group/nuvla-admin'
                    ],
                    'owners': [
                        'me'
                    ]
                },
                'resource-filter': "tag='application/blackbox'",
                'enabled': True,
                'resource-kind': 'event'
            }
        )

        event = Event(
            {
                'category': 'user',
                'tags': [
                    'application/blackbox'
                ],
                'content': {
                    'resource': {
                        'href': 'data-record/ae2d50dc-76ae-44ee-bd14-deaf139e4a67'
                    },
                    'state': 'created'
                },
                'created-by': 'me',
                'id': 'event/7ed45fc7-f63d-4c3e-9558-25c370c7f4c5',
                'severity': 'medium',
                'resource-type': 'event',
                'acl': {
                    'owners': [
                        'me'
                    ],
                    'view-data': [
                        'group/nuvla-admin'
                    ],
                },
                'operations': [
                    {
                        'rel': 'delete',
                        'href': 'event/7ed45fc7-f63d-4c3e-9558-25c370c7f4c5'
                    }
                ],
                'timestamp': '2023-01-04T15:23:37.383Z'
            }
        )

        subs_cfgs = EventSubsCfgMatcher(event).resource_subscriptions(
            [subs_cfg])
        assert 1 == len(subs_cfgs)

        subs_cfg = SubscriptionCfg(
            {
                'description': 'BB',
                'category': 'notification',
                'method-ids': [
                    'notification-method/b004854e-d00a-4ff2-8293-343748eb2774',
                    'notification-method/58996b63-3a47-42cc-8cff-bd84248bb000'
                ],
                'name': 'BB',
                'criteria': {
                    'metric': 'tag',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'application/whitebox'
                },
                'id': 'subscription-config/031a9699-46e6-453b-b9a0-406b272b01a7',
                'resource-type': 'subscription-config',
                'acl': {
                    'edit-data': [
                        'group/nuvla-admin'
                    ],
                    'owners': [
                        'me'
                    ]
                },
                'resource-filter': "tag='application/whitebox'",
                'enabled': True,
                'resource-kind': 'event'
            }
        )

        subs_cfgs = EventSubsCfgMatcher(event).resource_subscriptions(
            [subs_cfg])
        assert 0 == len(subs_cfgs)

    def test_match_blackbox_events(self):
        subs_cfg = SubscriptionCfg(
            {
                'description': 'BB',
                'category': 'notification',
                'method-ids': [
                    'notification-method/b004854e-d00a-4ff2-8293-343748eb2774',
                    'notification-method/58996b63-3a47-42cc-8cff-bd84248bb000'
                ],
                'name': 'BB',
                'criteria': {
                    'metric': 'tag',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'application/blackbox'
                },
                'id': 'subscription-config/031a9699-46e6-453b-b9a0-406b272b01a7',
                'resource-type': 'subscription-config',
                'acl': {
                    'edit-data': [
                        'group/nuvla-admin'
                    ],
                    'owners': [
                        'me'
                    ]
                },
                'resource-filter': "tag='application/blackbox'",
                'enabled': True,
                'resource-kind': 'event'
            }
        )

        href = 'data-record/ae2d50dc-76ae-44ee-bd14-deaf139e4a67'
        event = Event(
            {
                'category': 'user',
                'tags': [
                    'application/blackbox'
                ],
                'content': {
                    'resource': {
                        'href': href
                    },
                    'state': 'created'
                },
                'created-by': 'me',
                'id': 'event/7ed45fc7-f63d-4c3e-9558-25c370c7f4c5',
                'severity': 'medium',
                'resource-type': 'event',
                'acl': {
                    'owners': [
                        'me'
                    ],
                    'view-data': [
                        'group/nuvla-admin'
                    ],
                },
                'timestamp': '2023-01-04T15:23:37.383Z'
            }
        )

        notifs = EventSubsCfgMatcher(event).match_blackbox([subs_cfg])
        assert 1 == len(notifs)
        assert notifs[0]['resource_uri'].endswith(href)


class TestModulePublished(unittest.TestCase):

    def test_filter_module_publish_subscriptions_no_match(self):
        """Subscription must be enabled, match resource-kind and the criteria.
        """

        subs_cfg_no_resource_kind = SubscriptionCfg(
            {
                'enabled': True,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': EventSubsCfgMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_no_resource_kind]))
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_no_resource_kind]))

        subs_cfg_disabled_appsbq = SubscriptionCfg(
            {
                'enabled': False,
                'resource-kind': EventSubsCfgMatcher.RESOURCE_KIND_APPSBOUQUET,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': EventSubsCfgMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_disabled_appsbq]))

        subs_cfg_disabled_depl = SubscriptionCfg(
            {
                'enabled': False,
                'resource-kind': EventSubsCfgMatcher.RESOURCE_KIND_DEPLOYMENT,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': EventSubsCfgMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_disabled_depl]))

        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_disabled_appsbq, subs_cfg_no_resource_kind]))
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_disabled_depl, subs_cfg_no_resource_kind]))

        subs_cfg_not_published_depl = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': EventSubsCfgMatcher.RESOURCE_KIND_DEPLOYMENT,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'module.not-published'
                }
            }
        )
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_not_published_depl]))

        subs_cfg_not_published_appsbq = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': EventSubsCfgMatcher.RESOURCE_KIND_APPSBOUQUET,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'module.not-published'
                }
            }
        )
        assert 0 == len(
            EventSubsCfgMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_not_published_appsbq]))



    def test_filter_module_publish_subscriptions_match(self):
        """Subscription must be enabled, match resource-kind and the criteria.
        """

        subs_cfg_appsbq = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': EventSubsCfgMatcher.RESOURCE_KIND_APPSBOUQUET,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': EventSubsCfgMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 1 == len(
            EventSubsCfgMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_appsbq]))

        subs_cfg_depl = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': EventSubsCfgMatcher.RESOURCE_KIND_DEPLOYMENT,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': EventSubsCfgMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 1 == len(
            EventSubsCfgMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_depl]))

        assert 1 == len(
            EventSubsCfgMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_appsbq, subs_cfg_depl]))

        assert 1 == len(
            EventSubsCfgMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_appsbq, subs_cfg_depl]))
