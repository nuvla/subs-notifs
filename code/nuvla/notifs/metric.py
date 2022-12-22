"""
Module for handling resource metrics.

This module contains
- Classes for storing and retrieving resource metrics specific to NuvlaEdge
  resources.
- Helper function and exception for handling KeyError exceptions in a consistent
  way when a metric is not found in a collection of metrics.
"""

from typing import Union, Callable, Any

from nuvla.notifs.resource import Resource
from nuvla.notifs.log import get_logger


log = get_logger('metric')


EX_MSG_TMPL_KEY_NOT_FOUND = '{} not found under {}'


class MetricNotFound(Exception):
    """
    Exception raised when a metric is not found in a collection of metrics.
    """

    def __init__(self, metric_name: str, parent_key: str):
        """Initializes the MetricNotFound exception with the name of the metric
        that was not found.

        :param metric_name: The name of the metric that was not found.
        :param parent_key: Parent key under which the metric was searched.
        """
        self.metric_name = metric_name
        message = EX_MSG_TMPL_KEY_NOT_FOUND.format(metric_name, parent_key)
        super().__init__(message)


def key_error_ex_handler(func) -> Callable[..., Any]:
    """
    Exception handler as decorator for catching KeyError when searching for
    various metrics.
    :param func: function or method to wrap
    :return: wrapped function
    """

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as ex:
            raise MetricNotFound(str(ex), args[1]) from ex
    return wrapper


class NuvlaEdgeMetrics(Resource):
    """
    Representation of the NuvlaEdge metrics. Provides helper methods for
    accessing to and translating values of various metrics (e.g., load, ram,
    cpu, network).
    """

    RESOURCES_KEY = 'RESOURCES'
    RESOURCES_PREV_KEY = 'RESOURCES_PREV'

    NET_KEY = 'NETWORK'
    NET_STATS_KEY = 'net-stats'
    NET_TX_KEY = 'bytes-transmitted'
    NET_RX_KEY = 'bytes-received'
    DEFAULT_GW_KEY = 'default-gw'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @key_error_ex_handler
    def _load_pct(self, what: str) -> float:
        return 100 * self[what]['CPU']['load'] / self[what]['CPU']['capacity']

    @key_error_ex_handler
    def _ram_pct(self, what: str) -> float:
        return 100 * self[what]['RAM']['used'] / self[what]['RAM']['capacity']

    @key_error_ex_handler
    def _disk_pct(self, what: str, disk_name: str) -> Union[None, float]:
        for disk in self[what]['DISKS']:
            if disk_name == disk['device']:
                return 100 * disk['used'] / disk['capacity']

    def load_pct_curr(self) -> float:
        return self._load_pct(self.RESOURCES_KEY)

    def load_pct_prev(self) -> float:
        return self._load_pct(self.RESOURCES_PREV_KEY)

    def ram_pct_curr(self) -> float:
        return self._ram_pct(self.RESOURCES_KEY)

    def ram_pct_prev(self) -> float:
        return self._ram_pct(self.RESOURCES_PREV_KEY)

    def disk_pct_curr(self, disk_name) -> Union[None, float]:
        return self._disk_pct(self.RESOURCES_KEY, disk_name)

    def disk_pct_prev(self, disk_name) -> Union[None, float]:
        return self._disk_pct(self.RESOURCES_PREV_KEY, disk_name)

    def default_gw_name(self) -> Union[None, str]:
        net = self.get(self.NET_KEY, {})
        if net:
            return net.get(self.DEFAULT_GW_KEY)
        return None

    def default_gw_data(self) -> dict:
        gw_name = self.default_gw_name()
        if gw_name:
            for v in self[self.RESOURCES_KEY].get(self.NET_STATS_KEY, []):
                if gw_name == v.get('interface'):
                    return v
        return {}

    def _default_gw_txrx(self, kind) -> dict:
        gw = self.default_gw_data()
        if gw:
            return {'interface': gw['interface'],
                    'value': gw[kind]}
        return {}

    def default_gw_tx(self) -> dict:
        return self._default_gw_txrx(self.NET_TX_KEY)

    def default_gw_rx(self) -> dict:
        return self._default_gw_txrx(self.NET_RX_KEY)

    def net_rx_all(self) -> list:
        res = []
        for v in self[self.RESOURCES_KEY].get(self.NET_STATS_KEY, []):
            res.append({'interface': v.get('interface'),
                        'value': v.get(self.NET_RX_KEY)})
        return res

    def net_tx_all(self) -> list:
        res = []
        for v in self[self.RESOURCES_KEY].get(self.NET_STATS_KEY, []):
            res.append({'interface': v.get('interface'),
                        'value': v.get(self.NET_TX_KEY)})
        return res
