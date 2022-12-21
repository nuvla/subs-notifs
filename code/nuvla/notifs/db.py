"""
Set of classes implementing persistence layer and corresponding database drivers.

Schema to hold network consumption accumulating record.

{
"subs-id": "subscription-config/1-2-3-4-5",
"ne-id": "nuvlabox/1-2-3-4-5",
"iface": "eth0",
"rxtx":
  {
    # The Rx and Tx of a network interface are non-monotonic. The values can be
    # zeroed (e.g., by reloading network driver). Thus, to account for the
    # zeroing within a window under question a list of closed intervals is used:
    # [[start, last before zeroing],
    #  [0, last before zeroing],
    #  [0, last value]]
    # `last value` gets always updated with the latest value from telemetry.
    # The detection of the zeroing is done on the fact that the "current value"
    # coming from telemetry is smaller than the "last value". NB! There is a
    # corner case when while there might have been zeroing of the Rx/Tx of the
    # interface on the host, we could have missed it. This can happen when the
    # metrics sampling rate is low (e.g., minutes) but the Rx/Tx rates
    # are high after zeroing and the values going quickly above the value
    # before the zeroing happened.
    # The values of the Rx/Tx are in bytes.
    "value": [[start, end], ..],

    # Shows if the value was already above the threshold to not
    # re-trigger the notifications.
    "above-thld": False
  }
}

rxtx->vals and rxtx->above-thld are reset each time at the end of the user
defined window. rxtx->vals gets reset to [[0, 0]] and rxtx->above-thld gets
reset to False.

To compute current Rx or Tx the following reduction is used

sum(map(lambda x: x[1] - x[0], rxtx->vals))

Example:

sum(map(lambda x: x[1] - x[0], [[1, 2], [1, 4]])) => 4
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Union
import json
import math
import pickle
import re
import sqlite3

import elasticsearch

from nuvla.notifs.metric import NuvlaEdgeResourceMetrics
from nuvla.notifs.log import get_logger, stdout_handler

log = get_logger('db')

import logging

urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.DEBUG)
urllib3_logger.addHandler(stdout_handler)

es_logger = logging.getLogger('elasticsearch')
es_logger.setLevel(logging.DEBUG)
es_logger.addHandler(stdout_handler)


def bytes_to_gb(value_bytes: int) -> float:
    return round(value_bytes / math.pow(1024, 3), 2)


def gb_to_bytes(value_gb: float) -> int:
    return int(value_gb * math.pow(1024, 3))


def now() -> datetime:
    return datetime.now()


def next_month_first_day() -> datetime:
    return (now().replace(day=1) + timedelta(days=32)) \
        .replace(day=1, minute=0, second=0, microsecond=0)


class DBInconsistentStateError(Exception):
    pass


class Window:
    """
    Fixed resettable window within which the accumulation of values happens.
    """

    RE_VALID_TIME_WINDOW = re.compile('[0-9].*d$')

    def __init__(self, ts_window='month'):
        """
        :param ts_window: possible values: {month, Nd}; with 'month' working on
                          calendar months level, and Nd is a number of days to
                          roll over on.
        """
        self._ts_window: str = None
        self.ts_reset: datetime = None
        self.ts_window: str = ts_window

    @classmethod
    def _valid_time_window(cls, window: str) -> bool:
        return bool(re.match(cls.RE_VALID_TIME_WINDOW, window))

    @classmethod
    def _next_reset(cls, window: str) -> datetime:
        if window == 'month':
            return next_month_first_day()
        if cls._valid_time_window(window):
            days = int(window.replace('d', ''))
            return datetime.now() + timedelta(days=days)
        raise ValueError(f'Invalid window: {window}')

    def _update_next_reset(self):
        self.ts_reset = self._next_reset(self.ts_window)

    @property
    def ts_window(self) -> str:
        return self._ts_window

    @ts_window.setter
    def ts_window(self, window: str):
        self.ts_reset = self._next_reset(window)
        self._ts_window = window

    def need_update(self) -> bool:
        return self.ts_reset and now() >= self.ts_reset

    def update(self):
        self._update_next_reset()

    def to_dict(self):
        return {'ts_window': self.ts_window,
                'ts_reset': self.ts_reset.timestamp()}

    @staticmethod
    def from_dict(d: dict):
        if d is None:
            return None
        w = Window()
        w.ts_window = d['ts_window']
        w.ts_reset = datetime.fromtimestamp(d['ts_reset'])
        return w

    def to_json(self):
        return json.dumps(self.to_dict())

    def from_json(self, j: str):
        self.from_dict(json.loads(j))

    def __repr__(self):
        return f'{self.__class__.__name__}[ts_window={self.ts_window}, ' \
               f'ts_reset={self.ts_reset}'


class RxTxValue:
    """
    Implements Rx/Tx value taking into account possibility of zeroing of the
    bytes stream counter of the network interface on the host.
    """

    def __init__(self, init_value=None):
        if init_value is not None:
            self._values = init_value
        else:
            self._values: list = []

    def total(self) -> int:
        """
        Returns the current total value of the Rx or Tx. The implementation
        takes into account possible zeroing of the Rx/Tx values over a period
        of time the accumulation is done.
        :return: int
        """
        return sum(map(lambda x: x and x[1] - x[0] or 0, self._values))

    def set(self, current: int):
        """
        :param current: current value of the counter
        """

        if len(self._values) == 0:
            self._values = [[current, current]]
        else:
            # There was zeroing of the counters. Which we see as the current
            # value being lower than the previous one.
            if self._values[-1][-1] > current:
                self._values.append([0, current])
            else:
                self._values[-1][-1] = current

    def reset(self):
        self._values = []

    def get(self):
        return self._values

    def __repr__(self):
        return f'{self.__class__.__name__}[values={self._values}]'


class RxTx:
    """
    Tracks network Receive/Transmit, above threshold state, and defined window.
    """

    def __init__(self, window: Union[None, Window] = None):
        self.value: RxTxValue = RxTxValue()
        self.above_thld: bool = False
        self.window: Window = window

    def set_window(self, window: Window):
        self.window = window

    def set(self, current: int):
        """
        :param current: current value of the counter
        """

        self.value.set(current)

        self._apply_window()

    def total(self) -> int:
        return self.value.total()

    def get_above_thld(self) -> bool:
        return self.above_thld

    def set_above_thld(self):
        self.above_thld = True

    def reset_above_thld(self):
        self.above_thld = False

    def reset(self):
        """Resting 'total' only. 'prev' is needed to compute next delta.
        """
        self.value.reset()
        self.reset_above_thld()

    def _apply_window(self):
        if self.window and self.window.need_update():
            self.reset()
            self.window.update()

    def to_dict(self):
        return {'value': self.value.get(),
                'above_thld': self.above_thld,
                'window': self.window and self.window.to_dict() or None}

    @staticmethod
    def from_dict(d: dict):
        rxtx = RxTx()
        rxtx.value = RxTxValue(d['value'])
        rxtx.above_thld = d['above_thld']
        rxtx.window = Window.from_dict(d['window'])
        return rxtx

    def __repr__(self):
        return f'{self.__class__.__name__}[total={self.value}, ' \
               f'above_thld={self.above_thld}, ' \
               f'window={self.window}]'


# TODO: there is a problem with the abstract methods and their signatures
#       when the class is used. So, the class is not used for the moment.
class RxTxDriverABC(ABC):
    """
    Interface of DB driver for persisting Rx/Tx.
    """

    @abstractmethod
    def set(self, subs_id, ne_id, kind, interface=None, value=None):
        """
        TODO: document.
        :param subs_id:
        :param ne_id:
        :param kind:
        :param interface:
        :param value:
        :return:
        """

    @abstractmethod
    def get_data(self, subs_id, ne_id, iface, kind) -> Union[None, RxTx]:
        """
        TODO: document.
        :param subs_id:
        :param ne_id:
        :param iface:
        :param kind:
        :return:
        """

    @abstractmethod
    def reset(self, subs_id, ne_id, iface, kind):
        """
        TODO: document.
        :param subs_id:
        :param ne_id:
        :param iface:
        :param kind:
        :return:
        """


class RxTxDriverES:
    """
    Elasticsearch DB driver for persisting Rx/Tx.
    """

    INDEX_NAME = 'subsnotifs-rxtx'
    HOSTS = [{'host': 'localhost', 'port': 9200}]

    def __init__(self, hosts:Union[None, list] = None):
        """
        :param hosts: list of config dicts: {'host': 'localhost', 'port': 9200}
        """
        self.es = elasticsearch.Elasticsearch(hosts=hosts or self.HOSTS)

    def index_create(self, index=None):
        self.es.indices.create(index=index or self.INDEX_NAME)

    def connect(self):
        try:
            self.index_create(self.INDEX_NAME)
        except elasticsearch.exceptions.RequestError as ex:
            if ex.status_code == 400 and \
                    ex.error == 'resource_already_exists_exception':
                log.warning(ex.info)
            else:
                raise ex

    def index_delete(self, index=None):
        self.es.indices.delete(index=index or self.INDEX_NAME)

    def close(self):
        self.es = None

    def _index_content(self) -> dict:
        return self.es.search(index=self.INDEX_NAME,
                              body={'query': {'match_all': {}}})

    def _index_all_docs(self) -> dict:
        return self._index_content()['hits']['hits']

    def _find(self, query) -> dict:
        return self.es.search(index=self.INDEX_NAME, body=query)

    def _data_query(self, subs_id, ne_id, iface, kind) -> dict:
        return {
            'query': {
                'bool': {
                    'filter': [
                        {'term': {'subs_id': subs_id}},
                        {'term': {'ne_id': ne_id}},
                        {'term': {'iface': iface}},
                        {'term': {'kind': kind}}
                    ]
                }
            }
        }

    def _insert(self, doc):
        return self.es.index(index=self.INDEX_NAME, refresh=True, body=doc)

    def _update(self, _id, rxtx: dict):
        self.es.update(self.INDEX_NAME, _id, body={'doc': {'rxtx': rxtx}},
                       refresh=True)

    @staticmethod
    def _doc_base(subs_id, ne_id, iface, kind) -> dict:
        return {'subs_id': subs_id, 'ne_id': ne_id, 'kind': kind, 'iface': iface}

    @staticmethod
    def _rxtx_serialize(rxtx: RxTx) -> dict:
        return rxtx.to_dict()

    @staticmethod
    def _rxtx_deserialize(rxtx: dict) -> RxTx:
        return RxTx.from_dict(rxtx)

    @classmethod
    def _rxtx_from_value_serialized(cls, value: int, rxtx=None) -> dict:
        if None is rxtx:
            rxtx = RxTx()
        rxtx.set(value)
        return cls._rxtx_serialize(rxtx)

    @classmethod
    def _rxtx_from_es_resp(cls, es_resp: dict) -> RxTx:
        hits = es_resp['hits']['hits']
        if 1 == len(hits):
            data = hits[0]['_source']
            log.debug('Found data: %s', data)
            return cls._rxtx_deserialize(data['rxtx'])

    @classmethod
    def _doc_build(cls, subs_id, ne_id: str, kind: str, iface, value, rxtx=None) -> dict:
        return {**cls._doc_base(subs_id, ne_id, iface, kind),
                'rxtx': cls._rxtx_from_value_serialized(value, rxtx)}

    def _total_found(self, es_resp: dict) -> int:
        return es_resp['hits']['total']['value']

    def set(self, subs_id, ne_id: str, kind: str, interface=None, value=None):
        """Set `value` for the `kind` ('rx'/'tx') on the network `interface` of
        NE `ne_id`. Implemented as UPSERT.

        :param subs_id: subscription config id
        :param ne_id: NE id
        :param kind: 'rx' or 'tx'
        :param interface: network interface name of the NE with ne_id
        :param value: current 'rx' or 'tx' value as integer
        """
        query = self._data_query(subs_id, ne_id, interface, kind)
        resp = self._find(query)
        if self._total_found(resp) == 1:
            _id = resp['hits']['hits'][0]['_id']
            rxtx: RxTx = self._rxtx_from_es_resp(resp)
            if rxtx:
                doc = self._doc_build(subs_id, ne_id, kind, interface, value, rxtx)
                log.debug('To update: %s', doc)
                rxtx_ser = self._rxtx_from_value_serialized(value, rxtx)
                self._update(_id, rxtx_ser)
        else:
            log.debug('Adding new: %s, %s, %s, %s', subs_id, ne_id, kind, interface)
            doc = self._doc_build(subs_id, ne_id, kind, interface, value)
            resp = self._insert(doc)
            log.debug(resp)

    def get_data(self, subs_id, ne_id: str, iface: str, kind: str) -> Union[None, RxTx]:
        """Returns RxTx object if found in DB by `ne_id`, `iface` and `kind`.

        Throws InconsistentDBStateError in case more than one object are found
        in DB.

        :param subs_id: subscription configuration id
        :param ne_id: NE id
        :param iface: network interface name of the NE with ne_id
        :param kind: 'rx' or 'tx'
        :return: RxTx object if found in DB, None otherwise.
        """
        query = self._data_query(subs_id, ne_id, iface, kind)
        resp = self._find(query)
        log.debug('Search resp: %s', resp)
        num_found = resp['hits']['total']['value']
        log.debug('Found num: %s', num_found)
        if num_found == 0:
            return None
        if num_found == 1:
            return self._rxtx_from_es_resp(resp)
        if num_found > 1:
            raise DBInconsistentStateError(f'More than one object for {query}')

    def reset(self, subs_id, ne_id: str, iface: str, kind: str):
        """Resets corresponding RxTx object in DB by `ne_id`, `iface` and `kind`.

        :param subs_id: subscription configuration id
        :param ne_id: NE id
        :param iface: network interface name of the NE with ne_id
        :param kind: 'rx' or 'tx'
        """
        resp = self._find(self._data_query(subs_id, ne_id, iface, kind))
        if self._total_found(resp) == 1:
            rxtx: RxTx = self._rxtx_from_es_resp(resp)
            if rxtx:
                rxtx.reset()
                log.debug('To update: %s', rxtx)
                self._update(resp['hits']['hits'][0]['_id'],
                             self._rxtx_serialize(rxtx))

    def set_above_thld(self, subs_id: str, ne_id: str, iface: str, kind: str):
        """Sets above_thld in the corresponding RxTx object for `subs_id` in
        DB by `ne_id`, `iface` and `kind`.

        :param subs_id: notification subscription id
        :param ne_id: NE id
        :param iface: network interface name of the NE with ne_id
        :param kind: 'rx' or 'tx'
        """
        resp = self._find(self._data_query(subs_id, ne_id, iface, kind))
        if self._total_found(resp) == 1:
            rxtx: RxTx = self._rxtx_from_es_resp(resp)
            if rxtx:
                rxtx.set_above_thld()
                log.debug('To set above thld: %s', rxtx)
                self._update(resp['hits']['hits'][0]['_id'],
                             self._rxtx_serialize(rxtx))

    def reset_above_thld(self, subs_id, ne_id, iface, kind):
        resp = self._find(self._data_query(subs_id, ne_id, iface, kind))
        if self._total_found(resp) == 1:
            rxtx: RxTx = self._rxtx_from_es_resp(resp)
            if rxtx:
                rxtx.reset_above_thld()
                log.debug('To reset above thld: %s', rxtx)
                self._update(resp['hits']['hits'][0]['_id'],
                             self._rxtx_serialize(rxtx))


class RxTxDriverSqlite:
    """
    Sqlite DB driver for persisting Rx/Tx.
    """

    TABLE_NAME = 'rxtx'

    INSERT = f"INSERT INTO {TABLE_NAME} VALUES (?, ?, ?, ?)"
    UPDATE = f"UPDATE {TABLE_NAME} SET rxtx=? WHERE ne_id=? AND iface=? AND kind=?"
    SELECT = f"SELECT rxtx FROM {TABLE_NAME} WHERE ne_id=? AND iface=? AND kind=?"

    def __init__(self, path: str):
        self.con = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
        self.cur = None

    def set(self, ne_id, kind, interface=None, value=None):
        rxtx = self.get_data(ne_id, interface, kind)
        if rxtx:
            rxtx.set(value)
            rxtx_blob = pickle.dumps(rxtx, pickle.HIGHEST_PROTOCOL)
            self.cur.execute(self.UPDATE, (rxtx_blob, ne_id, interface, kind))
            self.con.commit()
        else:
            # Initial data.
            rxtx = RxTx()
            rxtx.set(value)
            rxtx_blob = pickle.dumps(rxtx, pickle.HIGHEST_PROTOCOL)
            self.cur.execute(self.INSERT, (ne_id, interface, kind, rxtx_blob))
            self.con.commit()

    def get_data(self, ne_id, iface, kind) -> Union[None, RxTx]:
        res = self.cur.execute(self.SELECT, (ne_id, iface, kind))
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


class RxTxDriverInMem:
    """
    Driver for in-memory persistence of Rx/Tx.
    """

    def __init__(self):
        """
        {'subscription/01':
            'nuvlabox/01': {
               'eth0': {'rx': RxTx[RxTxValue[values], above_thld, window],
                        'tx': RxTx},
               'docker0': {'rx': RxTx,
                           'tx': RxTx}
               ...
               },
            'nuvlabox/02: {},
         'subscription/02':
            'nuvlabox/01: {
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

    def set(self, subs_id, ne_id, kind, interface=None, value=None):
        if subs_id in self._db:
            if ne_id in self._db[subs_id]:
                if interface in self._db[subs_id][ne_id]:
                    if kind in self._db[subs_id][ne_id][interface]:
                        self._db[subs_id][ne_id][interface][kind].set(value)
                    else:
                        val = RxTx()
                        val.set(value)
                        self._db[subs_id][ne_id][interface][kind] = val
            else:
                val = RxTx()
                val.set(value)
                self._db[subs_id][ne_id][interface] = {kind: val}
        else:
            val = RxTx()
            val.set(value)
            self._db[subs_id] = {ne_id: {interface: {kind: val}}}

    def reset(self, ne_id, iface, kind):
        if self._db and len(self._db) > 0:
            self._db[ne_id][iface][kind].reset()

    def get_data(self, subs_id, ne_id, iface, kind) -> Union[None, RxTx]:
        if self._db and len(self._db) > 0:
            return self._db[subs_id][ne_id][iface][kind]
        return None

    def set_above_thld(self, subs_id, ne_id, iface, kind):
        if self._db and len(self._db) > 0:
            self._db[subs_id][ne_id][iface][kind].set_above_thld()

    def reset_above_thld(self, subs_id, ne_id, iface, kind):
        if self._db and len(self._db) > 0:
            self._db[subs_id][ne_id][iface][kind].reset_above_thld()

    def __len__(self):
        return len(self._db)

    def __repr__(self):
        return str(self._db)


class RxTxDB:
    """
    Manipulations with Rx/Tx over DB via a settable driver.
    """

    def __init__(self, driver=None):
        if driver is None:
            self._db = RxTxDriverInMem()
        else:
            self._db = driver

    #
    # Driver level methods.

    def set(self, subs_id, ne_id, kind, interface=None, value=None):
        if self._db is not None:
            self._db.set(subs_id, ne_id, kind, interface, value)

    def reset(self, subs_id, ne_id, iface, kind):
        if self._db is not None:
            self._db.reset(subs_id, ne_id, iface, kind)

    def get_data(self, subs_id, ne_id, iface, kind) -> Union[None, RxTx]:
        if self._db is not None:
            return self._db.get_data(subs_id, ne_id, iface, kind)
        return None

    #
    # Common methods.

    def set_rx(self, subs_id: str, ne_id: str, data: dict):
        """
        :param subs_id: unique ID of the subscription configuration
        :param ne_id: unique ID of the NE
        :param data: {interface: eth0, value: 0}
        """
        if data:
            self.set(subs_id, ne_id, 'rx', **data)

    def set_tx(self, subs_id, ne_id: str, data: dict):
        """
        :param subs_id: unique ID of the subscription configuration
        :param ne_id: unique ID of the NE
        :param data: {interface: eth0, value: 0}
        """
        if data:
            self.set(subs_id, ne_id, 'tx', **data)

    def get(self, subs_id, ne_id, iface, kind) -> Union[None, int]:
        data = self.get_data(subs_id, ne_id, iface, kind)
        if data:
            return data.total()

    def get_rx_data(self, subs_id, ne_id, iface) -> Union[None, RxTx]:
        return self.get_data(subs_id, ne_id, iface, 'rx')

    def get_rx(self, subs_id, ne_id, iface) -> Union[None, int]:
        rx_data = self.get_rx_data(subs_id, ne_id, iface)
        if rx_data:
            return rx_data.total()

    def get_tx_data(self, subs_id, ne_id, iface) -> Union[None, RxTx]:
        return self.get_data(subs_id, ne_id, iface, 'tx')

    def get_tx(self, subs_id, ne_id, iface) -> Union[None, int]:
        tx_data = self.get_tx_data(subs_id, ne_id, iface)
        if tx_data:
            return tx_data.total()

    def get_tx_gb(self, subs_id, ne_id, iface) -> Union[None, float]:
        tx = self.get_tx(subs_id, ne_id, iface)
        if tx:
            return bytes_to_gb(tx)

    def get_rx_gb(self, subs_id, ne_id, iface) -> Union[None, float]:
        rx = self.get_rx(subs_id, ne_id, iface)
        if rx:
            return bytes_to_gb(rx)

    def set_above_thld(self, ne_id, iface, kind):
        self._db.set_above_thld(ne_id, iface, kind, kind)

    def get_above_thld(self, subs_id, ne_id, iface, kind) -> bool:
        data = self.get_data(subs_id, ne_id, iface, kind)
        if data:
            return data.get_above_thld()

    def reset_above_thld(self, subs_id, ne_id, iface, kind):
        self._db.reset_above_thld(subs_id, ne_id, iface, kind)

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
