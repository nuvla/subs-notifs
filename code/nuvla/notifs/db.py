from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Union
import math
import pickle
import re
import sqlite3

from nuvla.notifs.metric import NuvlaEdgeResourceMetrics
from nuvla.notifs.log import get_logger

log = get_logger('db')


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
        self._above_thld = {}
        self.window = window

    def set_window(self, window: Window):
        self.window = window

    def set(self, current: int):
        """
        :param current: current value of the counter
        :return:
        """

        # there might have been zeroing of the counters
        if self.prev > current:
            self.total = self.total + current
        else:
            self.total = self.total + (current - self.prev)
        self.prev = current

        self._apply_window()

    def get_above_thld(self, subs_id) -> bool:
        return self._above_thld.get(subs_id, False)

    def set_above_thld(self, subs_id):
        self._above_thld[subs_id] = True

    def reset_above_thld(self, subs_id):
        self._above_thld[subs_id] = False

    def reset_above_thld_all(self):
        self._above_thld = {}

    def reset(self):
        """Resting 'total' only. 'prev' is needed to compute next delta.
        """
        self.total = 0
        self.reset_above_thld_all()

    def _apply_window(self):
        if self.window and self.window.need_update():
            print(f'{self.window} and {self.window.need_update()}')
            self.reset()
            self.window.update()

    def __repr__(self):
        return f'{self.__class__.__name__}[total={self.total}, ' \
               f'prev={self.prev}, above_thld={self._above_thld}, ' \
               f'window={self.window}]'


# TODO: there is a problem with the abstract methods and their signatures
#       when the class is used. So, the class is not used for the moment.
class RxTxDriverABC(ABC):

    @abstractmethod
    def set(self, ne_id, kind, interface=None, value=None):
        """
        TODO: document.
        :param ne_id:
        :param kind:
        :param interface:
        :param value:
        :return:
        """

    @abstractmethod
    def get_data(self, ne_id, iface, kind) -> Union[None, RxTx]:
        """
        TODO: document.
        :param ne_id:
        :param iface:
        :param kind:
        :return:
        """

    @abstractmethod
    def reset(self, ne_id, iface, kind):
        """
        TODO: document.
        :param ne_id:
        :param iface:
        :param kind:
        :return:
        """


class RxTxDriverSqlite:

    TABLE_NAME = 'rxtx'

    def __init__(self, path: str):
        self.con = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.cur = None

    def set(self, ne_id, kind, interface=None, value=None):
        rxtx = self.get_data(ne_id, interface, kind)
        if rxtx:
            rxtx.set(value)
            rxtx_blob = pickle.dumps(rxtx, pickle.HIGHEST_PROTOCOL)
            self.cur.execute(f"UPDATE {self.TABLE_NAME} SET rxtx=? WHERE ne_id=? AND iface=? AND kind=?",
                             (rxtx_blob, ne_id, interface, kind))
            self.con.commit()
        else:
            # Initial data.
            rxtx = RxTx()
            rxtx.set(value)
            rxtx_blob = pickle.dumps(rxtx, pickle.HIGHEST_PROTOCOL)
            self.cur.execute(f"INSERT INTO {self.TABLE_NAME} VALUES (?, ?, ?, ?)",
                             (ne_id, interface, kind, rxtx_blob))
            self.con.commit()

    def get_data(self, ne_id, iface, kind) -> Union[None, RxTx]:
        select = f"SELECT rxtx FROM {self.TABLE_NAME} WHERE ne_id='{ne_id}' AND iface='{iface}' AND kind='{kind}'"
        res = self.cur.execute(select)
        rxtx_res = res.fetchone()
        if rxtx_res:
            rxtx_blob = rxtx_res[0]
            return pickle.loads(rxtx_blob)
        return None

    def reset(self, ne_id, iface, kind):
        rxtx = self.get_data(ne_id, iface, kind)
        if rxtx:
            rxtx.reset()
            self._update_rxtx(ne_id, iface, kind, rxtx)

    def set_above_thld(self, ne_id, iface, kind, subs_id):
        rxtx = self.get_data(ne_id, iface, kind)
        if rxtx:
            rxtx.set_above_thld(subs_id)
            self._update_rxtx(ne_id, iface, kind, rxtx)

    def reset_above_thld(self, ne_id, iface, kind, subs_id):
        rxtx = self.get_data(ne_id, iface, kind)
        if rxtx:
            rxtx.reset_above_thld(subs_id)
            self._update_rxtx(ne_id, iface, kind, rxtx)

    def _update_rxtx(self, ne_id, iface, kind, rxtx: RxTx):
        rxtx_blob = pickle.dumps(rxtx, pickle.HIGHEST_PROTOCOL)
        self.cur.execute(f"UPDATE {self.TABLE_NAME} SET rxtx=? WHERE ne_id=? AND iface=? AND kind=?",
                         (rxtx_blob, ne_id, iface, kind))
        self.con.commit()

    def close(self):
        self.cur.close()
        self.cur = None

    def connect(self):
        self.cur = self.con.cursor()
        self._create_table()

    def _create_table(self):
        self.cur.execute(f'''CREATE TABLE IF NOT EXISTS {self.TABLE_NAME} (
        ne_id TEXT NOT NULL,
        iface TEXT NOT NULL,
        kind TEXT NOT NULL,
        rxtx BLOB NOT NULL
        )''')


class RxTxDBInMem:
    def __init__(self):
        """
        {'nuvlabox/01': {
            'eth0': {'rx': RxTx[total, prev, _above_thld{'subs/xxx': True/False, ...}, window],
                     'tx': RxTx},
            'docker0': {'rx': RxTx,
                        'tx': RxTx}
            ...
            },
         'nuvlabox/02': {
            'eth0': {'rx': RxTx,
                     'tx': RxTx},
            'docker0': {'rx': RxTx,
                        'tx': RxTx}},
         ...
        }
        """
        self._db = {}

    #
    # Driver level method.

    def set(self, ne_id, kind, interface=None, value=None):
        if ne_id in self._db:
            if interface in self._db[ne_id]:
                if kind in self._db[ne_id][interface]:
                    self._db[ne_id][interface][kind].set(value)
                else:
                    val = RxTx()
                    val.set(value)
                    self._db[ne_id][interface][kind] = val
            else:
                val = RxTx()
                val.set(value)
                self._db[ne_id][interface] = {kind: val}
        else:
            val = RxTx()
            val.set(value)
            self._db[ne_id] = {interface: {kind: val}}

    def reset(self, ne_id, iface, kind):
        if self._db and len(self._db) > 0:
            self._db[ne_id][iface][kind].reset()

    def get_data(self, ne_id, iface, kind) -> Union[None, RxTx]:
        if self._db and len(self._db) > 0:
            return self._db[ne_id][iface][kind]
        return None

    def set_above_thld(self, ne_id, iface, kind, subs_id):
        if self._db and len(self._db) > 0:
            self._db[ne_id][iface][kind].set_above_thld(subs_id)

    def reset_above_thld(self, ne_id, iface, kind, subs_id):
        if self._db and len(self._db) > 0:
            self._db[ne_id][iface][kind].reset_above_thld(subs_id)

    def __len__(self):
        return len(self._db)


class RxTxDB:
    def __init__(self, driver=None):
        if driver is None:
            self._db = RxTxDBInMem()
        else:
            self._db = driver

    #
    # Driver level method.

    def set(self, ne_id, kind, interface=None, value=None):
        if self._db is not None:
            self._db.set(ne_id, kind, interface, value)

    def reset(self, ne_id, iface, kind):
        if self._db is not None:
            self._db.reset(ne_id, iface, kind)

    def get_data(self, ne_id, iface, kind) -> Union[None, RxTx]:
        if self._db is not None:
            return self._db.get_data(ne_id, iface, kind)
        return None

    #
    # Common methods.

    def set_rx(self, ne_id: str, data: dict):
        """
        :param ne_id: unique ID of the NE
        :param data: {interface: eth0, value: 0}
        """
        if data:
            self.set(ne_id, 'rx', **data)

    def set_tx(self, ne_id: str, data: dict):
        """
        :param ne_id: unique ID of the NE
        :param data: {interface: eth0, value: 0}
        """
        if data:
            self.set(ne_id, 'tx', **data)

    def get(self, ne_id, iface, kind) -> Union[None, int]:
        data = self.get_data(ne_id, iface, kind)
        if data:
            return data.total

    def get_rx_data(self, ne_id, iface) -> Union[None, RxTx]:
        return self.get_data(ne_id, iface, 'rx')

    def get_rx(self, ne_id, iface) -> Union[None, int]:
        rx_data = self.get_rx_data(ne_id, iface)
        if rx_data:
            return rx_data.total

    def get_tx_data(self, ne_id, iface) -> Union[None, RxTx]:
        return self.get_data(ne_id, iface, 'tx')

    def get_tx(self, ne_id, iface) -> Union[None, int]:
        tx_data = self.get_tx_data(ne_id, iface)
        if tx_data:
            return tx_data.total

    def get_tx_gb(self, ne_id, iface) -> Union[None, float]:
        tx = self.get_tx(ne_id, iface)
        if tx:
            return bytes_to_gb(tx)

    def get_rx_gb(self, ne_id, iface) -> Union[None, float]:
        rx = self.get_rx(ne_id, iface)
        if rx:
            return bytes_to_gb(rx)

    def set_above_thld(self, ne_id, iface, kind, subs_id):
        self._db.set_above_thld(ne_id, iface, kind, subs_id)

    def get_above_thld(self, ne_id, iface, kind, subs_id) -> bool:
        data = self.get_data(ne_id, iface, kind)
        if data:
            return data.get_above_thld(subs_id)

    def reset_above_thld(self, ne_id, iface, kind, subs_id):
        self._db.reset_above_thld(ne_id, iface, kind, subs_id)

    def update(self, metrics: NuvlaEdgeResourceMetrics):
        ne_id = metrics.get('id')
        if ne_id:
            for v in metrics.net_rx_all():
                self.set_rx(ne_id, v)
            for v in metrics.net_tx_all():
                self.set_tx(ne_id, v)

    def __len__(self):
        return len(self._db)

    def __repr__(self):
        return str(self._db)
