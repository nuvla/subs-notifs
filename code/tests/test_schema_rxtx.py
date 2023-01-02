import unittest
from datetime import datetime, timedelta

from mock import Mock

from nuvla.notifs import window
from nuvla.notifs.schema.rxtx import _RxTxValue, RxTx
from nuvla.notifs.window import Window, next_month_this_day, \
    next_month_first_day, current_month_this_day
from test_db import window_now_orig


class TestRxTxValue(unittest.TestCase):

    def test_init(self):
        v = _RxTxValue()
        assert 0 == v.total()

    def test_set(self):
        v = _RxTxValue()
        v.set(0)
        assert 0 == v.total()

        v = _RxTxValue()
        v.set(1)
        assert 0 == v.total()

        # monotonic growth
        v = _RxTxValue()
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
        v = _RxTxValue()
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
        v = _RxTxValue()
        v.set(1)
        assert 0 == v.total()
        v.reset()
        assert 0 == v.total()

        v = _RxTxValue()
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
                           'ts_reset': rx.window._this_day(1).timestamp()}} == \
               rx.to_dict()

        d = 25
        rx.set_window(Window('month', month_day=d))
        assert {'value': [],
                'above_thld': False,
                'window': {'ts_window': 'month',
                           'month_day': d,
                           'ts_reset': rx.window._this_day(d).timestamp()}} == \
               rx.to_dict()


class TestRxTxWithWindow(unittest.TestCase):

    def tearDown(self) -> None:
        window.now = window_now_orig

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
        window.now = Mock(return_value=datetime.now() + timedelta(days=1))
        assert True is rx.window.need_update()
        rx.set(5)
        assert 0 == rx.total()
        next_reset_date = (window.now() + timedelta(days=1)).date()
        assert next_reset_date == rx.window.ts_reset.date()

    def test_reset_window_15d(self):
        rx = RxTx()
        rx.set_window(Window('15d'))
        assert '15d' == rx.window.ts_window
        rx.set(1)
        assert False is rx.window.need_update()
        assert 0 == rx.total()
        rx.set(3)
        assert False is rx.window.need_update()
        assert 2 == rx.total()
        # let's jump to the future
        window.now = Mock(return_value=datetime.now() + timedelta(days=15))
        assert True is rx.window.need_update()
        rx.set(5)
        assert 0 == rx.total()
        next_reset_date = (window.now() + timedelta(days=15)).date()
        assert next_reset_date == rx.window.ts_reset.date()

    def test_reset_window_month(self):
        rx = RxTx()
        rx.set_window(Window())
        assert 'month' == rx.window.ts_window
        assert 1 == rx.window.month_day
        rx.set(1)
        assert 0 == rx.total()
        # let's jump to the future
        window.now = Mock(return_value=datetime.now() + timedelta(days=32))
        assert True is rx.window.need_update()
        rx.set(50)
        assert 0 == rx.total()
        assert next_month_first_day().date() == rx.window.ts_reset.date()