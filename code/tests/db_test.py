import unittest

from datetime import datetime, timedelta
from mock import Mock

import nuvla.notifs.db as db
from nuvla.notifs.db import RxTx, RxTxDB, Window, bytes_to_gb, gb_to_bytes, \
    next_month_first_day


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


class TestRxTx(unittest.TestCase):

    def test_init(self):
        rx = RxTx()
        assert 0 == rx.total
        assert 0 == rx.prev

    def test_load_pers(self):
        rx = RxTx()

        rx.set(1)
        assert 1 == rx.prev
        assert 1 == rx.total

        rx.set(2)
        assert 2 == rx.prev
        assert 2 == rx.total

        rx.set(2)
        assert 2 == rx.prev
        assert 2 == rx.total

        rx.set(0)
        assert 0 == rx.prev
        assert 2 == rx.total

        rx.set(1)
        assert 1 == rx.prev
        assert 3 == rx.total


class TestNetworkDB(unittest.TestCase):

    def test_init(self):
        ndb = RxTxDB()
        assert ndb._db is not None

    def test_set_get(self):
        ndb = RxTxDB()
        assert ndb._db is not None

        ndb.set_rx('foo', {'interface': 'eth0', 'value': 0})
        assert 0 == ndb.get_rx('foo', 'eth0')
        assert 0 == ndb.get_rx_data('foo', 'eth0').prev

        ndb.set_rx('foo', {'interface': 'eth0', 'value': 1})
        assert 1 == ndb.get_rx('foo', 'eth0')
        assert 1 == ndb.get_rx_data('foo', 'eth0').prev

        ndb.set_rx('foo', {'interface': 'eth0', 'value': 3})
        assert 3 == ndb.get_rx('foo', 'eth0')
        assert 3 == ndb.get_rx_data('foo', 'eth0').prev

        ndb.set_rx('foo', {'interface': 'eth0', 'value': 0})
        assert 3 == ndb.get_rx('foo', 'eth0')
        assert 0 == ndb.get_rx_data('foo', 'eth0').prev

        ndb.set_rx('foo', {'interface': 'eth0', 'value': 2})
        assert 5 == ndb.get_rx('foo', 'eth0')
        assert 2 == ndb.get_rx_data('foo', 'eth0').prev

        ndb.set_tx('foo', {'interface': 'eth0', 'value': 2})
        assert 2 == ndb.get_tx('foo', 'eth0')
        assert 2 == ndb.get_tx_data('foo', 'eth0').prev

        ndb.set_rx('foo', {'interface': 'eth0', 'value': 5})
        assert 8 == ndb.get_rx('foo', 'eth0')
        assert 5 == ndb.get_rx_data('foo', 'eth0').prev

        ndb.set_tx('foo', {'interface': 'eth0', 'value': 5})
        assert 5 == ndb.get_tx('foo', 'eth0')
        assert 5 == ndb.get_tx_data('foo', 'eth0').prev


class TestWindow(unittest.TestCase):

    def test_init(self):
        wrtx = Window()
        assert 'month' == wrtx.ts_window
        wrtx = Window('1d')
        assert '1d' == wrtx.ts_window

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


class TestRxTxWithWindow(unittest.TestCase):

    def test_init(self):
        rx = RxTx()
        assert rx.window is None
        rx.set_window(Window())
        assert rx.window is not None
        assert 'month' == rx.window.ts_window
        rx.set(1)
        assert 1 == rx.total
        assert 1 == rx.prev

    def test_reset(self):
        rx = RxTx()
        rx.set_window(Window('1d'))
        assert '1d' == rx.window.ts_window
        rx.set(1)
        assert False is rx.window.need_update()
        assert 1 == rx.total
        assert 1 == rx.prev
        rx.set(3)
        assert False is rx.window.need_update()
        assert 3 == rx.total
        assert 3 == rx.prev
        # let's jump to the future
        db.now = Mock(return_value=datetime.now() + timedelta(days=2))
        assert True is rx.window.need_update()
        rx.set(5)
        assert 0 == rx.total
        assert 5 == rx.prev
        tomorrow = (datetime.now() + timedelta(days=1)).date()
        assert tomorrow == rx.window.ts_reset.date()

        rx = RxTx()
        rx.set_window(Window())
        assert 'month' == rx.window.ts_window
        rx.set(1)
        assert 1 == rx.total
        assert 1 == rx.prev
        # let's jump to the future
        db.now = Mock(return_value=datetime.now() + timedelta(days=32))
        assert True is rx.window.need_update()
        rx.set(50)
        assert 0 == rx.total
        assert 50 == rx.prev
        assert next_month_first_day().date() == rx.window.ts_reset.date()
