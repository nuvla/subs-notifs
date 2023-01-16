"""
A set of classes for holding and updating local representation of the
notification subscription configurations.
"""
import inspect
import logging
import re
import time

from typing import List, Union
from threading import Lock

from nuvla.notifs.log import get_logger
from nuvla.notifs.updater import DictUpdater

log = get_logger('main')

SUBS_CONF_TOPIC = 'subscription-config'

RESOURCE_KIND_NE = 'nuvlabox'
RESOURCE_KIND_EVENT = 'event'
RESOURCE_KIND_DATARECORD = 'data-record'
RESOURCE_KINDS = [v for k, v in globals().items() if
                  k.startswith('RESOURCE_KIND_')]


class RequiredAttributedMissing(KeyError):

    def __init__(self, attr):
        super(RequiredAttributedMissing, self).__init__(attr)


def required_attr_not_found_ex_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as ex:
            log.error('SubscriptionCfg: Required attribute is missing: %s', ex)
            raise RequiredAttributedMissing(str(ex))

    return wrapper


class SubscriptionCfg(dict):
    """
    Dictionary holding notification subscription configuration. Contains helper
    methods for easier data access and predicates to checking states and
    internal conditions.
    """

    KEY_RESET_INTERVAL = 'reset-interval'
    KEY_RESET_START_DAY = 'reset-start-date'

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, attr):
        return self[attr]

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

    def criteria_value(self) -> Union[int, float, bool]:
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

    def _owner(self):
        owners = self.get('acl', {}).get('owners', [])
        if owners:
            return owners[0]
        return None

    def can_view_resource(self, resource_acl: dict) -> bool:
        subs_owner = self._owner()
        return subs_owner in resource_acl.get('owners', []) or \
               subs_owner in resource_acl.get('view-data', [])

    def _tags_from_resource_filter(self) -> list:
        if self.get('resource-filter'):
            return list(
                filter(lambda x: x != '' and not re.match('tags *?=', x),
                       self['resource-filter'].split("'")))
        return []

    def tags_match(self, tags: Union[List, None]) -> bool:
        if tags:
            return bool(
                set(self._tags_from_resource_filter()).intersection(set(tags)))
        return False

    def resource_kind(self) -> str:
        return self.get('resource-kind')


class LoggingDict(dict):
    """
    An extension of the `dict` object to provide dict-based classes with logging
    the access to the attributes of the dictionary.
    """

    def _log_caller(self):
        stack = inspect.stack()
        cls_fn_name = stack[1].function
        caller = stack[2]
        cc = caller.code_context
        code_context = cc[0] if cc and len(cc) >= 1 else ''
        log.debug('%s.%s called by %s:%s %s %s', self.name, cls_fn_name,
                  caller.filename, caller.lineno, caller.function, code_context)

    @staticmethod
    def _is_debug() -> bool:
        return log.level == logging.DEBUG

    def __init__(self, name, *args, **kwargs):
        self.name: str = name
        dict.__init__(self, *args, **kwargs)
        if self._is_debug():
            self._log_caller()
            log.debug('%s __init__: args: %s, kwargs: %s', self.name, args,
                      kwargs)
        self.__lock = Lock()

    def __setitem__(self, key, value):
        with self.__lock:
            dict.__setitem__(self, key, value)
        if self._is_debug():
            self._log_caller()
            log.debug('%s set %s = %s', self.name, key, value)

    def __repr__(self):
        return f'{type(self).__name__}({dict.__repr__(self)})'

    def __delitem__(self, key):
        with self.__lock:
            try:
                dict.__delitem__(self, key)
                if self._is_debug():
                    self._log_caller()
                    log.debug('%s del %s', self.name, key)
            except KeyError:
                if self._is_debug():
                    self._log_caller()
                    log.error('%s del %s: no such key', self.name, key)

    def __getitem__(self, key):
        """Try with keys in upper case to account for ksqlDB key transformation.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError as ex:
            try:
                return dict.__getitem__(self, key.upper())
            except KeyError:
                raise ex

    def update(self, *args, **kwargs):
        log.debug('%s update: args: %s, kwargs: %s', self.name, args, kwargs)
        dict.update(self, *args, **kwargs)
        if self._is_debug():
            self._log_caller()
            log.debug('%s updated: %s', self.name, self)

    def empty(self):
        return 0 == len(list(self.keys()))


class SelfUpdatingDict(LoggingDict):
    """Self-updating dict.

    Provided `updater` object (of type `DictUpdater`) will be used to update the
    instance of the SelfUpdatingDict class with the values of the
    `sub_dict_class` class.
    """

    def __init__(self, name, updater: DictUpdater, sub_dict_class, *args,
                 **kwargs):
        super().__init__(name, *args, **kwargs)
        self._sub_dict_class = sub_dict_class
        self._updater = updater
        self._updater.run(self)

    def __setitem__(self, key, value):
        if value is None:
            # we don't know where the key is defined,
            # so delete from all the top level maps.
            for k in self.keys():
                try:
                    del self[k][key]
                except KeyError as ex:
                    log.warning('Deleting sub-key: no %s under %s', str(ex), k)
        else:
            rk = value.get('resource-kind')
            if rk not in RESOURCE_KINDS:
                self._resource_kind_not_known(rk)
            if rk in self:
                self[rk].update({key: self._sub_dict_class(value)})
            else:
                self._log_caller()
                log.debug(f'adding new resource-kind: {rk}')
                super().__setitem__(rk, {})
                dict.__setitem__(self[rk], key, self._sub_dict_class(value))

        if log.level == logging.DEBUG:
            log.debug('current keys:')
            for k in self.keys():
                log.debug(f'   {k}: {list(self[k].keys())}')

    def wait_not_empty(self, timeout=5):
        t_end = time.time() + timeout
        while self.empty():
            if time.time() >= t_end:
                raise Exception('Timed out waiting dict is not empty.')
            time.sleep(0.1)

    def wait_key_set(self, key, timeout=5):
        t_end = time.time() + timeout
        while key not in self:
            if time.time() >= t_end:
                raise Exception(f'Timed out waiting {key} to be set.')
            time.sleep(0.1)

    def wait_keys_set(self, keys=None):
        if keys is None:
            keys = RESOURCE_KINDS
        for k in keys:
            self.wait_key_set(k)

    def get_resource_kind_values(self, rk: str) -> List[SubscriptionCfg]:
        if rk in self:
            return self.get(rk).values()
        else:
            self._resource_kind_not_known(rk)
            return []

    def _resource_kind_not_known(self, rk: str):
        log.warning('Resource kind %s not known in %s', rk, RESOURCE_KINDS)


class SelfUpdatingSubsCfgs(SelfUpdatingDict):

    def __init__(self, name, updater: DictUpdater, *args, **kwargs):
        super().__init__(name, updater, SubscriptionCfg, *args, **kwargs)
