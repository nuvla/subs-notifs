"""
Module for handling resource metrics.

This module contains
- Classes for storing and retrieving resource metrics specific to NuvlaEdge
  resources.
- Helper function and exception for handling KeyError exceptions in a consistent
  way when a metric is not found in a collection of metrics.
"""

from typing import Union, Callable, Any, List

from nuvla.notifs.models.resource import Resource
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


class NENetMetric(dict):

    REQUIRED = {'id', 'iface', 'kind', 'value'}

    def __init__(self, *args, **kwargs):
        self._validate_input(*args, **kwargs)
        dict.__init__(self, *args, **kwargs)

    def __getattr__(self, attr):
        return self[attr]

    def _validate_input(self, *args, **kwargs):
        keys = []
        for v in args:
            if isinstance(v, dict):
                keys.extend(v.keys())
        keys.extend(kwargs.keys())
        assert self.REQUIRED.issubset(keys)

    def id(self):
        return self['id']

    def iface(self):
        return self['iface']

    def kind(self):
        return self['kind']

    def value(self):
        return self['value']


class NuvlaEdgeMetrics(Resource):
    """
    Representation of the NuvlaEdge metrics. Provides helper methods for
    accessing to and translating values of various metrics (e.g., load, ram,
    cpu, network).
    """

    RESOURCES_KEY = 'RESOURCES'
    RESOURCES_PREV_KEY = 'RESOURCES_PREV'
    NE_VERSION_KEY = 'NUVLABOX_ENGINE_VERSION'

    NET_KEY = 'NETWORK'
    NET_STATS_KEY = 'net-stats'
    NET_TX_KEY = 'bytes-transmitted'
    NET_RX_KEY = 'bytes-received'
    RX_KIND = 'rx'
    TX_KIND = 'tx'
    RXTX_KINDS = [RX_KIND, TX_KIND]
    NET_RXTX_KEY_MAP = {RX_KIND: NET_RX_KEY,
                        TX_KIND: NET_TX_KEY}
    DEFAULT_GW_KEY = 'default-gw'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @key_error_ex_handler
    def _load_pct(self, what: str) -> Union[None, float]:
        if not self.get(what):
            return
        cpu = self[what]['CPU']
        if not cpu:
            return
        return 100 * cpu['load'] / cpu['capacity']

    @key_error_ex_handler
    def _ram_pct(self, what: str) -> Union[None, float]:
        if not self.get(what):
            return
        ram = self[what]['RAM']
        if not ram:
            return
        return 100 * ram['used'] / ram['capacity']

    @key_error_ex_handler
    def _disk_pct(self, what: str, disk_name: str) -> Union[None, float]:
        if not self.get(what):
            return
        for disk in self[what].get('DISKS', []):
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

    def disk_pct_curr(self, disk_name: str) -> Union[None, float]:
        return self._disk_pct(self.RESOURCES_KEY, disk_name)

    def disk_pct_prev(self, disk_name: str) -> Union[None, float]:
        return self._disk_pct(self.RESOURCES_PREV_KEY, disk_name)

    def default_gw_name(self) -> Union[None, str]:
        net = self.get(self.NET_KEY, {})
        if net:
            return net.get(self.DEFAULT_GW_KEY)
        return None

    def _default_gw_data(self) -> dict:
        if not self.get(self.RESOURCES_KEY):
            return {}
        gw_name = self.default_gw_name()
        if gw_name:
            for v in self[self.RESOURCES_KEY].get(self.NET_STATS_KEY, []):
                if gw_name == v.get('interface'):
                    return v
        return {}

    def _is_rxtx_kind(self, kind: str):
        if kind not in self.RXTX_KINDS:
            msg = f'Wrong Rx/Tx kind {kind}. Allowed {self.RXTX_KINDS}'
            log.error(msg)
            raise ValueError(msg)

    def _default_gw_rxtx(self, kind: str) -> Union[NENetMetric, None]:
        """
        Get Rx or Tx network metrics for default gateway.

        :param kind: rx or tx
        :return:
        """
        gw = self._default_gw_data()
        if gw:
            return self._from_metrics_data(kind, gw)
        return None

    def default_gw_tx(self) -> Union[NENetMetric, None]:
        return self._default_gw_rxtx(self.TX_KIND)

    def default_gw_rx(self) -> Union[NENetMetric, None]:
        return self._default_gw_rxtx(self.RX_KIND)

    def _net_rxtx_all(self, kind: str) -> List[NENetMetric]:
        """
        Get Rx or Tx network metrics for all network interfaces.

        :param kind: rx or tx
        :return:
        """
        if not self.get(self.RESOURCES_KEY):
            return []
        self._is_rxtx_kind(kind)
        res: List[NENetMetric] = []
        for v in self[self.RESOURCES_KEY].get(self.NET_STATS_KEY, []):
            res.append(self._from_metrics_data(kind, v))
        return res

    def net_rx_all(self) -> List[NENetMetric]:
        return self._net_rxtx_all(self.RX_KIND)

    def net_tx_all(self) -> List[NENetMetric]:
        return self._net_rxtx_all(self.TX_KIND)

    def _from_metrics_data(self, kind: str, net_metrics: dict) -> NENetMetric:
        return NENetMetric({'id': self['id'],
                            'kind': kind,
                            'iface': net_metrics['interface'],
                            'value': net_metrics[self.NET_RXTX_KEY_MAP[kind]]})

    def ne_version(self) -> tuple:
        ver = self[self.NE_VERSION_KEY]
        if not ver:
            return 0, 0, 0
        return tuple(int(x) for x in ver.split('.'))
