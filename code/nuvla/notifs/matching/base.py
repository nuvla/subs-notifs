"""
This module provides implementations for matching criteria defined in
user notification subscriptions against the metrics coming from different
components of Nuvla the user has access to.
"""

from typing import Union, List

from nuvla.notifs.db.driver import gb_to_bytes
from nuvla.notifs.models.resource import Resource
from nuvla.notifs.models.subscription import SubscriptionCfg


def gt(val_bytes: Union[int, float], sc: SubscriptionCfg) -> bool:
    """greater than"""
    return val_bytes > gb_to_bytes(sc.criteria_value())


def lt(val_bytes: Union[int, float], sc: SubscriptionCfg) -> bool:
    """less than"""
    return val_bytes < gb_to_bytes(sc.criteria_value())


class ResourceSubsCfgMatcher:
    """
    Helper methods to check if subscription configurations are subscribed to
    a resource.
    """

    @classmethod
    def resource_subscribed(cls, resource: Resource,
                            subs_cfg: SubscriptionCfg,
                            with_disabled=False) -> bool:
        if with_disabled:
            return subs_cfg.can_view_resource(
                resource.get('acl', resource.get('ACL', {})))
        return subs_cfg.is_enabled() and \
               subs_cfg.can_view_resource(
                   resource.get('acl', resource.get('ACL', {})))

    @classmethod
    def resource_subscriptions(cls, resource: Resource,
                               subs_cfgs: List[SubscriptionCfg]) -> \
            List[SubscriptionCfg]:
        subs_cfgs_subscribed = []
        for subs_cfg in subs_cfgs:
            if cls.resource_subscribed(resource, subs_cfg):
                subs_cfgs_subscribed.append(subs_cfg)
        return subs_cfgs_subscribed

    @classmethod
    def resource_subscriptions_ids(cls, resource: Resource,
                                   subs_cfgs: List[SubscriptionCfg]) \
            -> List[str]:
        return [sc['id'] for sc in
                cls.resource_subscriptions(resource, subs_cfgs)]


class TaggedResourceSubsCfgMatcher(ResourceSubsCfgMatcher):
    """
    Helper methods to check if subscription configurations are subscribed to
    a tagged resource.
    """

    def __init__(self):
        super().__init__()

    @classmethod
    def resource_subscribed(cls, resource: Resource,
                            subs_cfg: SubscriptionCfg,
                            with_disabled=False) -> bool:
        if super().resource_subscribed(resource, subs_cfg, with_disabled):
            if subs_cfg.is_tags_set():
                return subs_cfg.tags_match(
                    resource.get('tags', resource.get('TAGS', [])))
            return True
        return False


class TaggedResourceNetworkSubsCfgMatcher(TaggedResourceSubsCfgMatcher):
    """
    Helper methods to check if subscription configurations are subscribed to
    networking metrics on a tagged resource.
    """

    def __init__(self):
        super().__init__()

    @classmethod
    def resource_subscribed(cls, resource: Resource,
                            subs_cfg: SubscriptionCfg,
                            with_disabled=True) -> bool:
        return super().resource_subscribed(resource, subs_cfg,
                                           with_disabled) and \
               subs_cfg.is_network_metric()
