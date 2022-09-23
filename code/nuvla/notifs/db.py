from datetime import datetime, timedelta
from typing import Union
import math
import re

from nuvla.notifs.metric import NuvlaEdgeResourceMetrics


def bytes_to_gb(value_bytes: int) -> float:
    return round(value_bytes / math.pow(1024, 3), 2)


def gb_to_bytes(value_gb: float) -> int:
    return int(value_gb * math.pow(1024, 3))


def now() -> datetime:
    return datetime.now()


def next_month_first_day() -> datetime:
    now = datetime.now()
    return (now.replace(day=1) + timedelta(days=32)) \
        .replace(day=1, minute=0, second=0, microsecond=0)


class Window:

    RE_VALID_TIME_WINDOW = re.compile('[0-9].*d$')

    def __init__(self, ts_window='month'):
        """
        :param ts_window: possible values: {month, Nd}; with 'month' working on
                          calendar months level, and Nd is a number of days to
                          roll over on.
        """
        self._ts_window = None
        self.ts_reset = None
        self.ts_window = ts_window

    @classmethod
    def _valid_time_window(cls, window):
        return bool(re.match(cls.RE_VALID_TIME_WINDOW, window))

    @classmethod
    def _next_reset(cls, window):
        if window == 'month':
            return next_month_first_day()
        elif cls._valid_time_window(window):
            days = int(window.replace('d', ''))
            return datetime.now() + timedelta(days=days)
        else:
            raise ValueError(f'Invalid window: {window}')

    def _update_next_reset(self):
        self.ts_reset = self._next_reset(self.ts_window)

    @property
    def ts_window(self):
        return self._ts_window

    @ts_window.setter
    def ts_window(self, window):
        self.ts_reset = self._next_reset(window)
        self._ts_window = window

    def need_update(self):
        return self.ts_reset and now() >= self.ts_reset

    def update(self):
        self._update_next_reset()


class RxTx:

    def __init__(self, window: Union[None, Window] = None):
        self.total: int = 0
        self.prev: int = 0
        self.window = window

    def set_window(self, window: Window):
        self.window = window

    def set(self, current: int):
        """
        :param current: current value of the counter
        :param ts: timestamp
        :return:
        """

        # there might have been zeroing of the counters
        if self.prev > current:
            self.total = self.total + current
        else:
            self.total = self.total + (current - self.prev)
        self.prev = current

        self._apply_window()

    def reset(self):
        """Resting 'total' only. 'prev' is needed to compute next delta.
        """
        self.total = 0

    def _apply_window(self):
        if self.window and self.window.need_update():
            self.reset()
            self.window.update()

    def __repr__(self):
        return f'{self.__class__.__name__}[total={self.total}, prev={self.prev}]'


class RxTxDB:
    def __init__(self):
        self.db = {}

    def set(self, neid, kind, interface=None, value=None):
        if neid in self.db:
            if interface in self.db[neid]:
                if kind in self.db[neid][interface]:
                    self.db[neid][interface][kind].set(value)
                else:
                    val = RxTx()
                    val.set(value)
                    self.db[neid][interface][kind] = val
            else:
                val = RxTx()
                val.set(value)
                self.db[neid][interface] = {kind: val}
        else:
            val = RxTx()
            val.set(value)
            self.db[neid] = {interface: {kind: val}}

    def set_rx(self, neid: str, data: dict):
        """
        :param neid: unique ID of the NE
        :param data: {interface: eth0, value: 0}
        """
        if data:
            self.set(neid, 'rx', **data)

    def set_tx(self, neid: str, data: dict):
        """
        :param neid: unique ID of the NE
        :param data: {interface: eth0, value: 0}
        """
        if data:
            self.set(neid, 'tx', **data)

    def get_data(self, neid, iface, kind) -> Union[None, RxTx]:
        if self.db:
            return self.db[neid][iface][kind]

    def get(self, neid, iface, kind) -> Union[None, int]:
        data = self.get_data(neid, iface, kind)
        if data:
            return data.total

    def get_rx_data(self, neid, iface) -> Union[None, RxTx]:
        return self.get_data(neid, iface, 'rx')

    def get_rx(self, neid, iface) -> Union[None, int]:
        rx_data = self.get_rx_data(neid, iface)
        if rx_data:
            return rx_data.total

    def get_tx_data(self, neid, iface) -> Union[None, RxTx]:
        return self.get_data(neid, iface, 'tx')

    def get_tx(self, neid, iface) -> Union[None, int]:
        tx_data = self.get_tx_data(neid, iface)
        if tx_data:
            return tx_data.total

    def get_tx_gb(self, neid, iface) -> Union[None, float]:
        tx = self.get_tx(neid, iface)
        if tx:
            return bytes_to_gb(tx)

    def get_rx_gb(self, neid, iface) -> Union[None, float]:
        rx = self.get_rx(neid, iface)
        if rx:
            return bytes_to_gb(rx)

    def reset(self, neid, iface, kind):
        if self.db:
            self.db[neid][iface][kind].reset()

    def update(self, metrics: NuvlaEdgeResourceMetrics):
        neid = metrics.get('id')
        if neid:
            for v in metrics.net_rx_all():
                self.set_rx(neid, v)
            for v in metrics.net_tx_all():
                self.set_tx(neid, v)

    def __len__(self):
        return len(self.db)

    def __repr__(self):
        return str(self.db)
