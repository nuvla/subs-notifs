from typing import Union

from nuvla.notifs.log import get_logger


log = get_logger('metric')


EX_MSG_TMPL_KEY_NOT_FOUND = '{} not found under {}'


class MetricNotFound(Exception):
    pass


def key_error_ex_handler(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except KeyError as ex:
            msg = EX_MSG_TMPL_KEY_NOT_FOUND.format(ex, args[1])
            raise MetricNotFound(msg) from ex
    return wrapper


class ResourceMetrics(dict):
    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

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

    def name(self):
        return self['name']

    def description(self):
        return self['description']

    def timestamp(self):
        return self['timestamp']

    def uuid(self):
        return self['id'].split('/')[1]


class NuvlaEdgeResourceMetrics(ResourceMetrics):

    resources = 'RESOURCES'
    resources_prev = 'RESOURCES_PREV'

    net = 'NETWORK'
    net_stats = 'NET-STATS'
    net_tx = 'bytes-transmitted'
    net_rx = 'bytes-received'

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
        return self._load_pct(self.resources)

    def load_pct_prev(self) -> float:
        return self._load_pct(self.resources_prev)

    def ram_pct_curr(self) -> float:
        return self._ram_pct(self.resources)

    def ram_pct_prev(self) -> float:
        return self._ram_pct(self.resources_prev)

    def disk_pct_curr(self, disk_name) -> Union[None, float]:
        return self._disk_pct(self.resources, disk_name)

    def disk_pct_prev(self, disk_name) -> Union[None, float]:
        return self._disk_pct(self.resources_prev, disk_name)

    def default_gw_name(self) -> str:
        return self.get(self.net, {}).get('default_gw')

    def default_gw(self) -> dict:
        gw = self.default_gw_name()
        if gw:
            for v in self[self.resources].get(self.net_stats, []):
                if gw == v.get('interface'):
                    return v
        return {}

    def _default_gw_txrx(self, kind) -> dict:
        gw = self.default_gw()
        if gw:
            return {'interface': gw['interface'],
                    'value': gw[kind]}
        return {}

    def default_gw_tx(self) -> dict:
        return self._default_gw_txrx(self.net_tx)

    def default_gw_rx(self) -> dict:
        return self._default_gw_txrx(self.net_rx)

    def net_rx_all(self) -> list:
        res = []
        for v in self[self.resources].get(self.net_stats, []):
            res.append({'interface': v.get('interface'),
                        'value': v.get(self.net_rx)})
        return res

    def net_tx_all(self) -> list:
        res = []
        for v in self[self.resources].get(self.net_stats, []):
            res.append({'interface': v.get('interface'),
                        'value': v.get(self.net_tx)})
        return res
