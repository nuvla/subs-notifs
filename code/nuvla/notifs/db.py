"""
Set of classes implementing persistence layer and corresponding database drivers.

Schema to hold network consumption accumulating record.

{
"subs_id": "subscription-config/1-2-3-4-5",
"ne_id": "nuvlaedge/1-2-3-4-5",
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
    "above_thld": False
  }
}

rxtx->value and rxtx->above_thld are reset each time at the end of the user
defined window. rxtx->vals gets reset to [[0, 0]] and rxtx->above_thld gets
reset to False.

To compute current Rx or Tx the following reduction is used

sum(map(lambda x: x[1] - x[0], rxtx->vals))

Example:

sum(map(lambda x: x[1] - x[0], [[1, 2], [0, 4]])) => 5
"""

import math
from typing import Union, List

import elasticsearch

from nuvla.notifs.log import get_logger, stdout_handler
from nuvla.notifs.metric import NuvlaEdgeMetrics
from nuvla.notifs.schema.rxtx import RxTx, RxTxEntry
from nuvla.notifs.subscription import SubscriptionCfg

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


class DBInconsistentStateError(Exception):
    pass


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

    def is_connected(self) -> bool:
        try:
            res = self.es.info()
        except Exception as ex:
            log.warning('Driver is not connected to DB: %s', ex)
            return False
        return 200 == res.get('status')

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
    def _rxtx_from_value_serialized(cls, rxtx_entry: RxTxEntry, rxtx=None) -> dict:
        if None is rxtx:
            rxtx = RxTx(rxtx_entry.window)
        rxtx.set(rxtx_entry.value)
        return cls._rxtx_serialize(rxtx)

    @classmethod
    def _rxtx_from_es_resp(cls, es_resp: dict) -> RxTx:
        hits = es_resp['hits']['hits']
        if 1 == len(hits):
            data = hits[0]['_source']
            log.debug('Found data: %s', data)
            return cls._rxtx_deserialize(data['rxtx'])

    @classmethod
    def _doc_build(cls, rxtx_entry: RxTxEntry, rxtx=None) -> dict:
        return {**cls._doc_base(rxtx_entry.subs_id, rxtx_entry.ne_id,
                                rxtx_entry.iface, rxtx_entry.kind),
                'rxtx': cls._rxtx_from_value_serialized(rxtx_entry, rxtx)}

    def _total_found(self, es_resp: dict) -> int:
        return es_resp['hits']['total']['value']

    def set(self, rxtx_entry: RxTxEntry):
        """Set `value` for the `kind` ('rx'/'tx') on the network `interface` of
        NE `ne_id`. Implemented as UPSERT.

        :param rxtx_entry: object containing details on subscription, NE network
                           metric and time window.
        """
        query = self._data_query(rxtx_entry.subs_id, rxtx_entry.ne_id,
                                 rxtx_entry.iface, rxtx_entry.kind)
        resp = self._find(query)
        if self._total_found(resp) == 1:
            # update
            _id = resp['hits']['hits'][0]['_id']
            rxtx: RxTx = self._rxtx_from_es_resp(resp)
            if rxtx:
                doc = self._doc_build(rxtx_entry, rxtx)
                log.debug('To update: %s', doc)
                rxtx_ser = self._rxtx_from_value_serialized(rxtx_entry, rxtx)
                self._update(_id, rxtx_ser)
        else:
            # insert
            log.debug('Adding new: %s', rxtx_entry)
            doc = self._doc_build(rxtx_entry)
            resp = self._insert(doc)
            log.debug(resp)

    def get_data(self, subs_id, ne_id: str, iface: str, kind: str) -> \
            Union[None, RxTx]:
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

    def reset(self, subs_id: str, ne_id: str, iface: str, kind: str):
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

    def __len__(self) -> int:
        return len(self._index_all_docs())


class RxTxDB:
    """
    Manipulations with Rx/Tx over DB via a settable driver.
    """

    def __init__(self, driver=None):
        if driver is None:
            self._db = RxTxDriverES()
        else:
            self._db = driver
        self.connect()

    def connect(self):
        self._db.connect()

    def is_connected(self) -> bool:
        return self._db.is_connected()

    #
    # Driver level methods.

    def set(self, rxtx_entry: RxTxEntry):
        if self._db is not None:
            self._db.set(rxtx_entry)

    def reset(self, rxtx_entry: RxTxEntry):
        if self._db is not None:
            self._db.reset(rxtx_entry.subs_id,
                           rxtx_entry.ne_id,
                           rxtx_entry.iface,
                           rxtx_entry.kind)

    def get_data(self, subs_id, ne_id, iface, kind) -> Union[None, RxTx]:
        if self._db is not None:
            return self._db.get_data(subs_id, ne_id, iface, kind)
        return None

    #
    # Common methods.

    def set_rx(self, rx_entry: RxTxEntry):
        """
        :param rx_entry: Rx network metric with subscription related info to
        be persisted in DB.
        """
        if rx_entry.kind == 'rx' and rx_entry.value:
            self.set(rx_entry)

    def set_tx(self, tx_entry: RxTxEntry):
        """
        :param tx_entry: Tx network metric with subscription related info to
        be persisted in DB.
        :return:
        """
        if tx_entry.kind == 'rx' and tx_entry.value:
            self.set(tx_entry)

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

    def set_above_thld(self, subs_id, ne_id, iface, kind):
        self._db.set_above_thld(subs_id, ne_id, iface, kind)

    def get_above_thld(self, subs_id, ne_id, iface, kind) -> bool:
        data = self.get_data(subs_id, ne_id, iface, kind)
        if data:
            return data.get_above_thld()

    def reset_above_thld(self, subs_id, ne_id, iface, kind):
        self._db.reset_above_thld(subs_id, ne_id, iface, kind)

    def update(self, metrics: NuvlaEdgeMetrics,
               subs_cfgs: List[SubscriptionCfg]):
        """
        Updates Rx/Tx metrics of the corresponding NuvlaEdge when it has an
        associated subscription. The underlying implementation works in UPSERT
        mode.

        The caller of the method must ensure that each subscription from the
        `subs_ids` list is actually subscribed to the NuvlaEdge from which the
        `metrics` are coming.

        :param metrics: telemetry of a NuvlaEdge
        :param subs_cfgs: list of subscription configs to the NuvlaEdge
        """
        if 'id' in metrics:
            for subs_cfg in subs_cfgs:
                for metric in metrics.net_rx_all():
                    self.set(RxTxEntry(subs_cfg, metric))
                for metric in metrics.net_tx_all():
                    self.set(RxTxEntry(subs_cfg, metric))

    def __len__(self):
        return len(self._db)

    def __repr__(self):
        return str(self._db)
