import unittest

from nuvla.notifs.matching.event import (EventSubsCfgMatcher,
                                         ModulePublishMatcher,
                                         APP_TYPE_K8S,
                                         APP_TYPE_DOCKER)
from nuvla.notifs.models.event import Event
from nuvla.notifs.models.subscription import SubscriptionCfg, \
    RESOURCE_KIND_APPLICATION_BOUQUET, RESOURCE_KIND_DEPLOYMENT


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
                    'metric': 'content-type',
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
                        'href': 'data-record/ae2d50dc-76ae-44ee-bd14-deaf139e4a67',
                        'content': {'content-type': 'application/blackbox'}
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

        subs_cfgs = EventSubsCfgMatcher(event).resource_subscriptions([subs_cfg])
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
                    'metric': 'content-type',
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

        subs_cfgs = EventSubsCfgMatcher(event).resource_subscriptions([subs_cfg])
        assert 1 == len(subs_cfgs)

    def test_match_data_record_events(self):
        subs_cfg_wb = SubscriptionCfg(
            {
                'description': 'my data records created',
                'category': 'notification',
                'method-ids': [
                    'notification-method/b004854e-d00a-4ff2-8293-343748eb2774',
                    'notification-method/58996b63-3a47-42cc-8cff-bd84248bb000'
                ],
                'name': 'my data records created',
                'criteria': {
                    'metric': 'content-type',
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
                'enabled': True,
                'resource-kind': 'event'
            }
        )

        subs_cfg_bb = SubscriptionCfg(
            {
                'description': 'BB',
                'category': 'notification',
                'method-ids': [
                    'notification-method/b004854e-d00a-4ff2-8293-343748eb2774',
                    'notification-method/58996b63-3a47-42cc-8cff-bd84248bb000'
                ],
                'name': 'BB',
                'criteria': {
                    'metric': 'content-type',
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
                'enabled': True,
                'resource-kind': 'event'
            }
        )

        href = 'data-record/ae2d50dc-76ae-44ee-bd14-deaf139e4a67'
        event_wb = Event(
            {
                'category': 'user',
                'name': 'whitebox created',
                'description': 'whitebox created',
                'content': {
                    'resource': {
                        'href': href,
                        'content': {'content-type': 'application/whitebox'}
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

        notifs = EventSubsCfgMatcher(event_wb).match_data_record([subs_cfg_wb,
                                                                  subs_cfg_bb])
        assert 1 == len(notifs)
        assert notifs[0]['resource_uri'].endswith(href)
        assert notifs[0]['resource_name'] == 'whitebox created'
        assert notifs[0]['resource_description'] == 'whitebox created'

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
                    'metric': 'content-type',
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
                'enabled': True,
                'resource-kind': 'event'
            }
        )

        href = 'data-record/ae2d50dc-76ae-44ee-bd14-deaf139e4a67'
        event = Event(
            {
                'category': 'user',
                'content': {
                    'resource': {
                        'href': href,
                        'content': {'content-type': 'application/blackbox'}
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

        notifs = EventSubsCfgMatcher(event).match_data_record([subs_cfg])
        assert 1 == len(notifs)
        assert notifs[0]['resource_uri'].endswith(href)
        assert notifs[0]['resource_name'] == 'blackbox'
        assert notifs[0]['resource_description'] == 'blackbox'


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
                    'value': ModulePublishMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_no_resource_kind]))
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_no_resource_kind]))

        subs_cfg_disabled_appsbq = SubscriptionCfg(
            {
                'enabled': False,
                'resource-kind': RESOURCE_KIND_APPLICATION_BOUQUET,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': ModulePublishMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_disabled_appsbq]))

        subs_cfg_disabled_depl = SubscriptionCfg(
            {
                'enabled': False,
                'resource-kind': RESOURCE_KIND_DEPLOYMENT,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': ModulePublishMatcher.MODULE_PUBLISHED_CRITERIA
                }
            }
        )
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_disabled_depl]))

        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_disabled_appsbq, subs_cfg_no_resource_kind]))
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_disabled_depl, subs_cfg_no_resource_kind]))

        subs_cfg_not_published_depl = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': RESOURCE_KIND_DEPLOYMENT,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'module.not-published'
                }
            }
        )
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_not_published_depl]))

        subs_cfg_not_published_appsbq = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': RESOURCE_KIND_APPLICATION_BOUQUET,
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': 'module.not-published'
                }
            }
        )
        assert 0 == len(
            ModulePublishMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_not_published_appsbq]))

    def test_filter_module_publish_subscriptions_match(self):
        """Subscription must be enabled, match resource-kind and the criteria.
        """

        subs_cfg_appsbq = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': 'application',
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': f'{ModulePublishMatcher.MODULE_PUBLISHED_CRITERIA}.{RESOURCE_KIND_APPLICATION_BOUQUET}'
                }
            }
        )
        assert 1 == len(
            ModulePublishMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_appsbq]))

        subs_cfg_depl = SubscriptionCfg(
            {
                'enabled': True,
                'resource-kind': 'application',
                'criteria': {
                    'metric': 'name',
                    'kind': 'string',
                    'condition': 'is',
                    'value': f'{ModulePublishMatcher.MODULE_PUBLISHED_CRITERIA}.{RESOURCE_KIND_DEPLOYMENT}'
                }
            }
        )
        assert 1 == len(
            ModulePublishMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_depl]))

        assert 1 == len(
            ModulePublishMatcher.filter_event_module_publish_appsbouquet_subscriptions(
                [subs_cfg_appsbq, subs_cfg_depl]))

        assert 1 == len(
            ModulePublishMatcher.filter_event_module_publish_deployment_subscriptions(
                [subs_cfg_appsbq, subs_cfg_depl]))

    def test_detect_kubernetes_app(self):
        matcher = ModulePublishMatcher()
        matcher._match_app_published_app_simple = lambda n,a,b,c,d: n.extend(['foo'])
        notifs = matcher.match_app_published(object, object, '',
                                             APP_TYPE_K8S, Event())
        assert notifs == ['foo']

    def test_detect_docker_app(self):
        matcher = ModulePublishMatcher()
        matcher._match_app_published_app_simple = lambda n,a,b,c,d: n.extend(['bar'])
        notifs = matcher.match_app_published(object, object, '',
                                             APP_TYPE_DOCKER, Event())
        assert notifs == ['bar']


class TestResourceSubsCfgMatcher(unittest.TestCase):

    def test_resource_subscribed(self):
        subs_cfg = SubscriptionCfg({
            "description": "data taska ms1",
            "category": "notification",
            "method-ids": [
                "notification-method/af74aa51-1347-4ccc-b212-181163100b7a",
                "notification-method/bc2245ba-facf-4726-b88d-ea0b62b5d6d1"
            ],
            "updated": "2024-06-17T11:36:33.542Z",
            "name": "data taska ms1",
            "criteria": {
                "metric": "content-type",
                "kind": "string",
                "condition": "is",
                "value": "application/taska-ms1"
            },
            "created": "2024-06-12T13:03:37.647Z",
            "updated-by": "user/78c063ac-fd2a-4c96-bcd5-43059cf73ebe",
            "created-by": "user/78c063ac-fd2a-4c96-bcd5-43059cf73ebe",
            "id": "subscription-config/2985260b-8a35-403a-ae23-dc66dfe93384",
            "resource-type": "subscription-config",
            "acl": {
                "edit-data": [
                    "group/nuvla-admin"
                ],
                "owners": [
                    "group/extract"
                ],
                "view-acl": [
                    "group/nuvla-admin"
                ],
                "delete": [
                    "group/nuvla-admin"
                ],
                "view-meta": [
                    "group/nuvla-admin"
                ],
                "edit-acl": [
                    "group/nuvla-admin"
                ],
                "view-data": [
                    "group/nuvla-admin"
                ],
                "manage": [
                    "group/nuvla-admin"
                ],
                "edit-meta": [
                    "group/nuvla-admin"
                ]
            },
            "resource-filter": "tag='application/taska-ms1'",
            "enabled": True,
            "resource-kind": "event"
        })

        event = Event({
            "category": "user",
            "tags": [
                "application/taska-ms4"
            ],
            "content": {
                "resource": {
                    "href": "data-record/291d8a2e-aa33-41c6-ab2b-96298fd7efdf",
                    "content": {
                        "content-type": "application/taska-ms4"
                    }
                },
                "state": "created"
            },
            "updated": "2024-06-17T12:58:13.526Z",
            "created": "2024-06-17T12:58:13.526Z",
            "created-by": "user/78c063ac-fd2a-4c96-bcd5-43059cf73ebe",
            "id": "event/529efb78-7a1d-4ff8-be78-0aecb7bb0de0",
            "severity": "medium",
            "resource-type": "event",
            "acl": {
                "edit-data": [
                    "group/nuvla-admin"
                ],
                "owners": [
                    "group/extract"
                ],
                "view-acl": [
                    "group/nuvla-admin"
                ],
                "delete": [
                    "group/nuvla-admin"
                ],
                "view-meta": [
                    "group/nuvla-admin"
                ],
                "edit-acl": [
                    "group/nuvla-admin"
                ],
                "view-data": [
                    "group/nuvla-admin"
                ],
                "manage": [
                    "group/nuvla-admin"
                ],
                "edit-meta": [
                    "group/nuvla-admin"
                ]
            },
            "operations": [
                {
                    "rel": "delete",
                    "href": "event/529efb78-7a1d-4ff8-be78-0aecb7bb0de0"
                }
            ],
            "timestamp": "2024-06-14T07:11:34.483Z"
        })

        subs_cfgs = EventSubsCfgMatcher(event).resource_subscriptions([subs_cfg])
        assert 1 == len(subs_cfgs)
