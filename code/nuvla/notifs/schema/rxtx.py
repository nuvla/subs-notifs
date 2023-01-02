"""
Set of classes defining the NuvlaEdge network metrics objects to be persisted in
the DB.
"""

from typing import Union

from nuvla.notifs.log import get_logger
from nuvla.notifs.metric import NENetMetric
from nuvla.notifs.window import Window
from nuvla.notifs.subscription import SubscriptionCfg

log = get_logger('rxtx')


class _RxTxValue:
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
        self.value: _RxTxValue = _RxTxValue()
        self.above_thld: bool = False
        self.window: Window = window

    def set_window(self, window: Window):
        self.window = window

    def set(self, current: int):
        """
        :param current: current value of the counter
        """

        self._apply_window()

        self.value.set(current)

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
            log.debug('RxTx got reset: %s', self)

    def to_dict(self):
        return {'value': self.value.get(),
                'above_thld': self.above_thld,
                'window': self.window and self.window.to_dict() or None}

    @staticmethod
    def from_dict(d: dict):
        rxtx = RxTx()
        rxtx.value = _RxTxValue(d['value'])
        rxtx.above_thld = d['above_thld']
        rxtx.window = Window.from_dict(d['window'])
        return rxtx

    def __repr__(self):
        return f'{self.__class__.__name__}[value={self.value}, ' \
               f'above_thld={self.above_thld}, ' \
               f'window={self.window}]'


class RxTxEntry:
    """
    High-level class to be provided to the DB layer for persisting accumulating
    values of Rx or Tx metrics, tracking reset window and subscription ID.
    """

    def __init__(self, subs_cfg: SubscriptionCfg, ne_net_metric: NENetMetric):
        self.subs_id: str = subs_cfg['id']
        self.window: Window = self.subs_cfg_criteria_to_window(subs_cfg)
        self.ne_id: str = ne_net_metric.id()
        self.iface: str = ne_net_metric.iface()
        self.kind: str = ne_net_metric.kind()
        self.value: int = ne_net_metric.value()

    @staticmethod
    def subs_cfg_criteria_to_window(subs_cfg: SubscriptionCfg) -> Window:
        if subs_cfg.criteria_reset_interval():
            if subs_cfg.criteria_reset_interval().startswith('month'):
                reset_date = subs_cfg.criteria_reset_start_date() or 1
                return Window('month', reset_date)
            else:
                return Window(subs_cfg.criteria_reset_interval())
        return Window()

    def __repr__(self):
        return f'{self.__class__.__name__}[' \
               f'subs_id={self.subs_id}, ' \
               f'window={self.window}, ' \
               f'ne_id={self.ne_id}, ' \
               f'iface={self.iface}, ' \
               f'kind={self.kind}, ' \
               f'value={self.value}' \
               f']'
