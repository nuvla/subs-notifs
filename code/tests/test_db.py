import os
import unittest

from datetime import datetime, timedelta
from mock import Mock

import elasticsearch

from es_patch import es_update, es_index_create
import elasticmock.fake_elasticsearch
elasticmock.fake_elasticsearch.FakeElasticsearch.update = es_update
import elasticmock.fake_indices
elasticmock.fake_indices.FakeIndicesClient.create = es_index_create

from elasticmock import elasticmock

import nuvla.notifs.db as db
from nuvla.notifs.db import RxTxValue, RxTx, RxTxDB, RxTxDriverES, Window, \
    bytes_to_gb, gb_to_bytes, next_month_first_day, _next_month_same_day, \
    next_month_this_day, DBInconsistentStateError

db_now_orig = db.now


class TestNextMonthConversion(unittest.TestCase):

    def test_month_same_day(self):

        assert datetime.fromisoformat('2023-03-15T00:00:00') == \
               _next_month_same_day(
                   datetime.fromisoformat('2023-02-15T00:00:00'))

        for i in range(28, 32):
            assert datetime.fromisoformat('2023-02-28T00:00:00') == \
                   _next_month_same_day(
                       datetime.fromisoformat(f'2023-01-{i}T00:00:00'))

    def test_month_this_day(self):
        res = next_month_this_day(28)
        assert 28 == res.day
        assert (datetime.today().month + 1) % 12 == res.month


class TestDBUtils(unittest.TestCase):

    def test_bytes_to_gb(self):
        assert 0 == bytes_to_gb(1)
        assert 0 == bytes_to_gb(1024**1)
        assert 0 == bytes_to_gb(1024**2)
        assert 1 == bytes_to_gb(1024**3)
        assert 3 == bytes_to_gb(3*1024**3)
        assert 1.5 == bytes_to_gb(1024**3 + 510*1024**2)

    def test_gb_to_bytes(self):
        assert 0 == gb_to_bytes(0.0)
        assert 1 * 1024 ** 3 == gb_to_bytes(1.)
        assert 1288490188 == gb_to_bytes(1.2)
        assert 5057323991 == gb_to_bytes(4.71)

    def test_gb_to_bytes_to_gb(self):
        assert 0 == bytes_to_gb(gb_to_bytes(0.0))
        assert 1.0 == bytes_to_gb(gb_to_bytes(1.0))
        assert 1.2 == bytes_to_gb(gb_to_bytes(1.2))
        assert 4.71 == bytes_to_gb(gb_to_bytes(4.71))


class TestValue(unittest.TestCase):

    def test_init(self):
        v = RxTxValue()
        assert 0 == v.total()

    def test_set(self):
        v = RxTxValue()
        v.set(0)
        assert 0 == v.total()

        v = RxTxValue()
        v.set(1)
        assert 0 == v.total()

        # monotonic growth
        v = RxTxValue()
        v.set(1)
        assert 0 == v.total()
        v.set(2)
        assert 1 == v.total()
        v.set(3)
        assert 2 == v.total()
        v.set(3)
        assert 2 == v.total()
        v.set(5)
        assert 4 == v.total()

        # non-monotonic growth with "zeroing"
        v = RxTxValue()
        v.set(1)
        assert 0 == v.total()
        assert [[1, 1]] == v._values
        v.set(2)
        assert 1 == v.total()
        assert [[1, 2]] == v._values
        v.set(5)
        assert 4 == v.total()
        assert [[1, 5]] == v._values
        v.set(2)
        assert 6 == v.total()
        assert [[1, 5], [0, 2]] == v._values
        v.set(4)
        assert 8 == v.total()
        assert [[1, 5], [0, 4]] == v._values
        v.set(3)
        assert 11 == v.total()
        assert [[1, 5], [0, 4], [0, 3]] == v._values

    def test_reset(self):
        v = RxTxValue()
        v.set(1)
        assert 0 == v.total()
        v.reset()
        assert 0 == v.total()

        v = RxTxValue()
        v.set(1)
        assert 0 == v.total()
        v.set(3)
        assert 2 == v.total()
        v.reset()
        assert 0 == v.total()


class TestRxTx(unittest.TestCase):

    def test_init(self):
        rx = RxTx()
        assert 0 == rx.total()

    def test_load_pct(self):
        rx = RxTx()

        rx.set(1)
        assert 0 == rx.total()

        rx.set(2)
        assert 1 == rx.total()

        rx.set(2)
        assert 1 == rx.total()

        rx.set(0)
        assert 1 == rx.total()

        rx.set(1)
        assert 2 == rx.total()

    def test_to_dict(self):
        rx = RxTx()
        rx.set(1)
        assert {'value': [[1, 1]],
                'above_thld': False,
                'window': None} == rx.to_dict()

        rx.reset()
        assert {'value': [],
                'above_thld': False,
                'window': None} == rx.to_dict()

        rx.set_above_thld()
        assert {'value': [],
                'above_thld': True,
                'window': None} == rx.to_dict()

        rx.reset()
        assert {'value': [],
                'above_thld': False,
                'window': None} == rx.to_dict()

        rx.set_window(Window())
        assert {'value': [],
                'above_thld': False,
                'window': {'ts_window': 'month',
                           'month_day': 1,
                           'ts_reset': next_month_this_day(1).timestamp()}} == \
               rx.to_dict()

        d = 25
        rx.set_window(Window('month', month_day=d))
        assert {'value': [],
                'above_thld': False,
                'window': {'ts_window': 'month',
                           'month_day': d,
                           'ts_reset': next_month_this_day(d).timestamp()}} == \
               rx.to_dict()


def rx_workflow_test(rxtx: RxTxDB):
    rxtx.set_rx('foo', 'bar', {'interface': 'eth0', 'value': 0})
    assert 0 == rxtx.get_rx('foo', 'bar', 'eth0')

    rxtx.set_rx('foo', 'bar', {'interface': 'eth0', 'value': 1})
    assert 1 == rxtx.get_rx('foo', 'bar', 'eth0')

    rxtx.set_rx('foo', 'bar', {'interface': 'eth0', 'value': 3})
    assert 3 == rxtx.get_rx('foo', 'bar', 'eth0')

    rxtx.set_rx('foo', 'bar', {'interface': 'eth0', 'value': 0})
    assert 3 == rxtx.get_rx('foo', 'bar', 'eth0')

    rxtx.set_rx('foo', 'bar', {'interface': 'eth0', 'value': 2})
    assert 5 == rxtx.get_rx('foo', 'bar', 'eth0')

    rxtx.set_tx('foo', 'bar', {'interface': 'eth0', 'value': 2})
    assert 0 == rxtx.get_tx('foo', 'bar', 'eth0')

    rxtx.set_rx('foo', 'bar', {'interface': 'eth0', 'value': 5})
    assert 8 == rxtx.get_rx('foo', 'bar', 'eth0')

    rxtx.set_tx('foo', 'bar', {'interface': 'eth0', 'value': 5})
    assert 3 == rxtx.get_tx('foo', 'bar', 'eth0')


class TestNetworkDBInMem(unittest.TestCase):

    def test_init(self):
        ndb = RxTxDB()
        assert ndb._db is not None

    def test_rx_workflow(self):
        ndb = RxTxDB()
        assert ndb._db is not None

        rx_workflow_test(ndb)


class TestRxTxDriverESDataMgmt(unittest.TestCase):

    def test_data_mgmt(self):
        data_base = RxTxDriverES._doc_base('subs/01', 'ne/01', 'eth0', 'rx')
        assert {'subs_id': 'subs/01',
                'ne_id': 'ne/01',
                'iface': 'eth0',
                'kind': 'rx'} == data_base

        data = RxTxDriverES._doc_build('subs/01', 'ne/01', 'eth0', 'rx', 1)
        assert 'rxtx' in data
        assert data.get('rxtx') is not None

        rxtx = RxTx()
        rxtx.set(3)
        data = RxTxDriverES._doc_build('subs/01', 'ne/01', 'eth0', 'rx', 1,
                                       rxtx=rxtx)
        assert 'rxtx' in data
        assert data.get('rxtx') is not None
        rxtx_updated = RxTxDriverES._rxtx_deserialize(data.get('rxtx'))
        assert 1 == rxtx_updated.total()


class TestRxTxDriverESMockedBase(unittest.TestCase):

    @elasticmock
    def setUp(self):
        self.driver: RxTxDriverES = RxTxDriverES()
        self.driver.connect()

    @elasticmock
    def tearDown(self):
        self.driver.index_delete()
        self.driver.close()
        self.driver = None


class TestRxTxDriverESMockedCreateIndex(unittest.TestCase):

    @elasticmock
    def test_successive_create_index_throws(self):
        self.driver = RxTxDriverES()
        self.driver.index_create(RxTxDriverES.INDEX_NAME)
        with self.assertRaises(elasticsearch.exceptions.RequestError) as cm:
            self.driver.index_create(RxTxDriverES.INDEX_NAME)
        ex = cm.exception
        self.assertEqual(ex.status_code, 400)
        self.assertEqual(ex.error, 'resource_already_exists_exception')

    @elasticmock
    def test_successive_connect_succeeds(self):
        self.driver = RxTxDriverES()
        self.driver.connect()
        self.driver.connect()


class TestRxTxDriverESMocked(TestRxTxDriverESMockedBase):

    def test_init(self):
        assert self.driver.es is not None
        assert [] == self.driver._index_all_docs()

    def test_set_get_basic(self):
        subs_id = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'
        value = 1

        self.driver.set(subs_id, ne_id, kind, iface, value)

        assert 1 == len(self.driver._index_all_docs())

        rxtx = self.driver.get_data(subs_id, ne_id, 'foo', 'bar')
        assert rxtx is None
        rxtx = self.driver.get_data(subs_id, ne_id, iface, 'bar')
        assert rxtx is None
        rxtx = self.driver.get_data(subs_id, ne_id, 'foo', kind)
        assert rxtx is None

        rxtx = self.driver.get_data(subs_id, ne_id, iface, kind)
        assert rxtx is not None
        assert 0 == rxtx.total()
        assert False is rxtx.above_thld
        assert None is rxtx.window

    def test_inconsistent_db(self):
        subs_id = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'
        value = 1

        self.driver.set(subs_id, ne_id, kind, iface, value)
        assert 1 == len(self.driver._index_all_docs())
        rxtx = self.driver.get_data(subs_id, ne_id, iface, kind)
        assert rxtx is not None
        assert 0 == rxtx.total()
        assert False is rxtx.above_thld
        assert None is rxtx.window

        doc = self.driver._doc_build(subs_id, ne_id, kind, iface, value)
        self.driver._insert(doc)
        assert 2 == len(self.driver._index_all_docs())
        self.assertRaises(DBInconsistentStateError, self.driver.get_data,
                          *(subs_id, ne_id, iface, kind))

    def test_set_get_reset_workflow(self):
        def rx_validate(rx, total):
            assert rx is not None
            assert total == rx.total()
            assert False is rx.above_thld

        subs_id = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'

        self.driver.set(subs_id, ne_id, kind, iface, 1)
        assert 1 == len(self.driver._index_all_docs())

        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)

        self.driver.set(subs_id, ne_id, kind, iface, 2)
        assert 1 == len(self.driver._index_all_docs())

        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 1)

        self.driver.reset(subs_id, ne_id, iface, kind)
        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)

        self.driver.set(subs_id, ne_id, kind, iface, 10)
        assert 1 == len(self.driver._index_all_docs())

        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)

        self.driver.reset(subs_id, ne_id, iface, kind)
        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)


class TestWindow(unittest.TestCase):

    def test_init(self):

        wrtx = Window()
        assert 'month' == wrtx.ts_window
        assert 1 == wrtx.month_day

        wrtx = Window('month', 1)
        assert 'month' == wrtx.ts_window
        assert 1 == wrtx.month_day

        wrtx = Window('month', 31)
        assert 'month' == wrtx.ts_window
        assert 31 == wrtx.month_day

        self.assertRaises(AssertionError, Window, 'month', 0)
        self.assertRaises(AssertionError, Window, 'month', 32)

        wrtx = Window('1d')
        assert '1d' == wrtx.ts_window
        assert None is wrtx.month_day

    def test_valid_window(self):

        wrtx = Window()

        assert True is wrtx._valid_time_window('1d')
        assert True is wrtx._valid_time_window('123d')
        assert False is wrtx._valid_time_window('123s')
        assert False is wrtx._valid_time_window('d')
        assert False is wrtx._valid_time_window('abcd')
        assert False is wrtx._valid_time_window('foo')
        assert False is wrtx._valid_time_window('')

    def test_next_reset(self):
        assert (datetime.today() + timedelta(days=1)).date() == \
               Window._next_reset('1d').date()
        assert (datetime.today() + timedelta(days=42)).date() == \
               Window._next_reset('42d').date()
        self.assertRaises(TypeError, Window._next_reset, None)
        self.assertRaises(ValueError, Window._next_reset, '')
        self.assertRaises(ValueError, Window._next_reset, '42')

    def test_to_from_dict(self):
        assert {'ts_window': 'month',
                'month_day': 1,
                'ts_reset': next_month_this_day(1).timestamp()} == \
               Window().to_dict()

        win_dict = {'ts_window': '1d',
                    'month_day': None,
                    'ts_reset': (datetime.today() + timedelta(days=1)).timestamp()}
        assert win_dict == Window.from_dict(win_dict).to_dict()


class TestRxTxWithWindow(unittest.TestCase):

    def test_init(self):
        rx = RxTx()
        assert rx.window is None
        rx.set_window(Window())
        assert rx.window is not None
        assert 'month' == rx.window.ts_window
        rx.set(1)
        assert 0 == rx.total()

    def test_reset_window_1d(self):
        rx = RxTx()
        rx.set_window(Window('1d'))
        assert '1d' == rx.window.ts_window
        rx.set(1)
        assert False is rx.window.need_update()
        assert 0 == rx.total()
        rx.set(3)
        assert False is rx.window.need_update()
        assert 2 == rx.total()
        # let's jump to the future
        db.now = Mock(return_value=datetime.now() + timedelta(days=2))
        assert True is rx.window.need_update()
        rx.set(5)
        assert 0 == rx.total()
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        assert tomorrow == rx.window.ts_reset.date()

        db.now = db_now_orig

    def test_reset_window_month(self):
        rx = RxTx()
        rx.set_window(Window())
        assert 'month' == rx.window.ts_window
        assert 1 == rx.window.month_day
        rx.set(1)
        assert 0 == rx.total()
        # let's jump to the future
        db.now = Mock(return_value=datetime.now() + timedelta(days=32))
        assert True is rx.window.need_update()
        rx.set(50)
        assert 0 == rx.total()
        assert next_month_first_day().date() == rx.window.ts_reset.date()
