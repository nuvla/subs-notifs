"""
A set of classes for holding and updating local representation of the
notification subscription configurations.
"""
import re

from typing import List, Union

from nuvla.notifs.log import get_logger
from nuvla.notifs.dictupdater import DictUpdater
from nuvla.notifs.models.dyndict import SelfUpdatingDict, DictDataClass

log = get_logger('subscription')

SUBS_CONF_TOPIC = 'subscription-config'

RESOURCE_KIND_NE = 'nuvlabox'
RESOURCE_KIND_EVENT = 'event'
RESOURCE_KIND_DATARECORD = 'data-record'
RESOURCE_KIND_DEPLOYMENT = 'deployment'
RESOURCE_KIND_DEPLOYMENT_SET = 'deployment-set'
RESOURCE_KIND_APPLICATION_BOUQUET = 'apps-bouquet'
RESOURCE_KINDS = [v for k, v in globals().items() if
                  k.startswith('RESOURCE_KIND_')]

NETWORK_METRIC_PREFIX = 'network-'


class RequiredAttributedMissing(KeyError):

    def __init__(self, attr):
        super(RequiredAttributedMissing, self).__init__(attr)


def required_attr_not_found_ex_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as ex:
            log.error('SubscriptionCfg: Required attribute is missing: %s', ex)
            raise RequiredAttributedMissing(str(ex)) from ex

    return wrapper


class SubscriptionCfg(DictDataClass):
    """
    Dictionary holding notification subscription configuration. Contains helper
    methods for easier data access and predicates to checking states and
    internal conditions.
    """

    KEY_RESET_INTERVAL = 'reset-interval'
    KEY_RESET_START_DAY = 'reset-start-date'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __getattr__(self, attr):
        return self[attr]

    @staticmethod
    def resource_kinds():
        return RESOURCE_KINDS

    #
    # Required attributes on criteria.
    #

    @required_attr_not_found_ex_handler
    def criteria(self):
        """
        'criteria' is required attribute on subscription configuration.
        """
        return self['criteria']

    @required_attr_not_found_ex_handler
    def criteria_condition(self) -> str:
        """
        'condition' is required attribute on criteria.
        """
        return self.criteria()['condition']

    @required_attr_not_found_ex_handler
    def criteria_metric(self) -> str:
        """
        'metric' is required attribute on criteria.
        """
        return self.criteria()['metric']

    @required_attr_not_found_ex_handler
    def criteria_kind(self) -> str:
        """
        'kind' is required attribute on criteria.
        """
        return self.criteria()['kind']

    #
    # Optional attributes on criteria.
    #

    def criteria_value(self) -> Union[int, float, bool, str]:
        """
        'value' is optional attribute in criteria.
        """
        val = self.criteria().get('value')
        if self.criteria_kind() == 'numeric':
            if self.criteria().get('value-type') == 'double' or '.' in val:
                return float(val)
            return int(val)
        if self.criteria_kind() == 'boolean':
            return val in ['true', 'True']
        return val

    def criteria_dev_name(self) -> str:
        """
        'dev-name' is optional attribute in criteria.
        """
        return self.criteria().get('dev-name')

    def criteria_reset_interval(self) -> Union[None, str]:
        """
        reset interval is optional attribute in criteria.
        """
        return self.criteria().get(self.KEY_RESET_INTERVAL)

    def criteria_reset_start_date(self) -> Union[None, int]:
        """
        reset start date is optional attribute in criteria.
        """
        return self.criteria().get(self.KEY_RESET_START_DAY)

    #
    # Predicate methods.
    #

    def is_enabled(self) -> bool:
        return self.get('enabled', False)

    def is_metric(self, metric: str) -> bool:
        return self.criteria()['metric'] == metric

    def is_condition(self, condition: str) -> bool:
        return self.criteria()['condition'] == condition

    def is_metric_cond(self, metric: str, cond: str) -> bool:
        return self.is_metric(metric) and self.is_condition(cond)

    def is_network_metric(self):
        return self.criteria_metric().startswith(NETWORK_METRIC_PREFIX)

    def owner(self):
        owners = self.get('acl', {}).get('owners', [])
        if owners:
            return owners[0]
        return None

    def can_view_resource(self, resource_acl: dict) -> bool:
        subs_owner = self.owner()
        return subs_owner in resource_acl.get('owners', []) or \
               subs_owner in resource_acl.get('view-data', [])

    def _tags_from_resource_filter(self) -> list:
        if self.get('resource-filter'):
            return list(
                filter(lambda x: x != '' and not re.match('tags *?=', x),
                       self['resource-filter'].split("'")))
        return []

    def is_tags_set(self) -> bool:
        return 0 < len(self._tags_from_resource_filter())

    def tags_match(self, tags: Union[List, None]) -> bool:
        if tags:
            return bool(
                set(self._tags_from_resource_filter()).intersection(set(tags)))
        return False

    def resource_kind(self) -> str:
        return self.get('resource-kind')


class SelfUpdatingSubsCfgs(SelfUpdatingDict):

    def __init__(self, name, updater: DictUpdater, *args, **kwargs):
        super().__init__(name, updater, SubscriptionCfg, *args, **kwargs)
