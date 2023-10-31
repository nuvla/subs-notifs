import json
import re
from typing import Optional
from datetime import datetime, timedelta, timezone
from dateutil import relativedelta


def now(tz=None) -> datetime:
    return datetime.now(tz)


def next_month_first_day() -> datetime:
    return (now().replace(day=1) + timedelta(days=32)) \
        .replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _next_month_same_day(date: datetime) -> datetime:
    """
    Given `date`, returns same day of the next month relative to the `date`.
    :param date: datetime
    """
    return date + relativedelta.relativedelta(months=1)


def _next_month_this_day(day: int) -> datetime:
    """
    Given `day` of month, returns same day of the next month.
    :param day: int (range [1, 31])
    """
    assert 1 <= day <= 31
    dt = now(tz=timezone.utc)
    return _next_month_same_day(datetime(dt.year, dt.month, day))


def _last_day_of_month(any_day) -> int:
    # The day 28 exists in every month. 4 days later, it's always next month
    next_month = any_day.replace(day=28) + timedelta(days=4)
    # subtracting the number of the current day brings us back one month
    return (next_month - timedelta(days=next_month.day)).day


def _current_month_this_day(day: int) -> datetime:
    """
    Given `day` of month, returns the `day` of the current month.
    :param day: int (range [1, 31])
    """
    assert 1 <= day <= 31
    dt = now(tz=timezone.utc)
    last_day = _last_day_of_month(dt)
    return datetime(dt.year, dt.month, min(day, last_day))


class Window:
    """
    Fixed resettable window within which the accumulation of values happens.
    """

    RE_VALID_TIME_WINDOW = re.compile(r'\d.*d$')

    def __init__(self, ts_window='month', month_day=1):
        """
        :param ts_window: possible values: {month, Nd}; with 'month' working on
                          calendar months level, and Nd is a number of days to
                          roll over on.
        :param month_day: int (range [1, 31]), day of the month for reset.
        """
        if ts_window == 'month':
            assert 1 <= month_day <= 31
            self.month_day = month_day
        else:
            self.month_day = None
        self._ts_window: Optional[str]
        self.ts_reset: Optional[datetime]
        self.ts_window: str = ts_window

    @classmethod
    def _valid_time_window(cls, window: str) -> bool:
        return bool(re.match(cls.RE_VALID_TIME_WINDOW, window))

    @staticmethod
    def _this_day(month_day: int) -> datetime:
        if now().day < month_day:
            return _current_month_this_day(month_day)
        return _next_month_this_day(month_day)

    @classmethod
    def _next_reset(cls, window: str, month_day: int = 1) -> datetime:
        if window == 'month':
            return cls._this_day(month_day)
        if cls._valid_time_window(window):
            days = int(window.replace('d', ''))
            return now() + timedelta(days=days)
        raise ValueError(f'Invalid window: {window}')

    def _update_next_reset(self):
        self.ts_reset = self._next_reset(self.ts_window, self.month_day)

    @property
    def ts_window(self) -> str:
        return self._ts_window

    @ts_window.setter
    def ts_window(self, window: str):
        self.ts_reset = self._next_reset(window, self.month_day)
        self._ts_window = window

    def need_update(self) -> bool:
        return self.ts_reset and now() >= self.ts_reset

    def update(self):
        self._update_next_reset()

    def to_dict(self):
        return {'ts_window': self.ts_window,
                'ts_reset': self.ts_reset.timestamp(),
                'month_day': self.month_day}

    @staticmethod
    def from_dict(d: dict):
        if d is None:
            return None
        w = Window()
        w.ts_window = d['ts_window']
        w.ts_reset = datetime.fromtimestamp(d['ts_reset'])
        w.month_day = d['month_day']
        return w

    def to_json(self):
        return json.dumps(self.to_dict())

    def from_json(self, j: str):
        self.from_dict(json.loads(j))

    def __repr__(self):
        return f'{self.__class__.__name__}[ts_window={self.ts_window}, ' \
               f'month_day={self.month_day}, ' \
               f'ts_reset={self.ts_reset}]'
