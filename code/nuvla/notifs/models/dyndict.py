"""
Dynamic dictionary updating itself using an updater driver.
"""

import inspect
import logging
import time
from threading import Lock
from typing import List, Type

from nuvla.notifs.dictupdater import DictUpdater
from nuvla.notifs.log import get_logger

log = get_logger('dyndict')


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


class DictDataClass(dict):

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    @staticmethod
    def resource_kinds() -> object:
        raise NotImplementedError()


class SelfUpdatingDict(LoggingDict):
    """Self-updating dict.

    Provided `updater` object (of type `DictUpdater`) will be used to update the
    instance of the SelfUpdatingDict class with the values of the
    `sub_dict_class` class.
    """

    def __init__(self, name, updater: DictUpdater, dict_data_class: Type[DictDataClass],
                 *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self._dict_data_class: Type[DictDataClass] = dict_data_class
        self._updater = updater
        self._updater.run(self)

    def __setitem__(self, key, value):
        """
        Set `key` with `value` on the dictionary. If `value` is None, delete
        the corresponding `key` from dict, if defined.
        :param key:
        :param value:
        :return:
        """
        if value is None:
            # we don't know where the key is defined,
            # so delete from all the top level maps.
            for k in self.keys():
                try:
                    del self[k][key]
                except KeyError as ex:
                    log.warning('Deleting sub-key: no key %s under %s', str(ex), k)
        else:
            rk = value.get('resource-kind')
            if rk not in self._dict_data_class.resource_kinds():
                self._resource_kind_not_known(rk)
            if rk in self:
                self[rk].update({key: self._dict_data_class(value)})
            else:
                self._log_caller()
                log.debug('adding new resource-kind: %s', rk)
                super().__setitem__(rk, {})
                dict.__setitem__(self[rk], key, self._dict_data_class(value))

        if log.level == logging.DEBUG:
            log.debug('current keys:')
            for k in self.keys():
                log.debug('   %s: %s', k, list(self[k].keys()))

    def wait_not_empty(self, timeout=5):
        t_end = time.time() + timeout
        while self.empty():
            if time.time() >= t_end:
                raise TimeoutError('Timed out waiting dict is not empty.')
            time.sleep(0.1)

    def wait_key_set(self, key, timeout=5):
        t_end = time.time() + timeout
        while key not in self:
            if time.time() >= t_end:
                raise TimeoutError(f'Timed out waiting {key} to be set.')
            time.sleep(0.1)

    def wait_keys_set(self, keys=None):
        if keys is None:
            keys = self._dict_data_class.resource_kinds()
        for k in keys:
            self.wait_key_set(k)

    def get_resource_kind_values(self, rk: str) -> List[dict]:
        if rk in self:
            return self.get(rk).values()
        self._resource_kind_not_known(rk)
        return []

    def _resource_kind_not_known(self, rk: str):
        log.warning('Resource kind %s not known in %s', rk,
                    self._dict_data_class.resource_kinds())
