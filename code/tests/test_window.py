import unittest
from datetime import datetime, timedelta

from nuvla.notifs.db.window import Window
from nuvla.notifs.db.window import _next_month_this_day, _next_month_same_day


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
        res = _next_month_this_day(28)
        assert 28 == res.day
        assert (datetime.today().month + 1) % 12 == res.month


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
                'ts_reset': _next_month_this_day(1).timestamp()} == \
               Window().to_dict()

        win_dict = {'ts_window': '1d',
                    'month_day': None,
                    'ts_reset': (datetime.today() + timedelta(days=1)).timestamp()}
        assert win_dict == Window.from_dict(win_dict).to_dict()