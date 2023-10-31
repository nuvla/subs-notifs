import unittest

from nuvla.notifs.models.subscription import SubscriptionCfg
from nuvla.notifs.db.window import Window
from nuvla.notifs.db.schema.rxtx import RxTxEntry


class TestRxTxEntry(unittest.TestCase):

    def test_subs_cfg_criteria_to_window(self):
        assert Window().to_dict() == RxTxEntry.subs_cfg_criteria_to_window(
            SubscriptionCfg({'criteria': {}})).to_dict()

        w = RxTxEntry.subs_cfg_criteria_to_window(
            SubscriptionCfg(
                {'criteria': {SubscriptionCfg.KEY_RESET_INTERVAL: 'month'}}))
        wctl = Window('month')
        assert wctl.ts_window == w.ts_window
        assert wctl.month_day == w.month_day

        w = RxTxEntry.subs_cfg_criteria_to_window(
            SubscriptionCfg({
                'criteria':
                    {SubscriptionCfg.KEY_RESET_INTERVAL: 'month',
                     SubscriptionCfg.KEY_RESET_START_DAY: 15}}))
        wctl = Window('month', 15)
        assert wctl.ts_window == w.ts_window
        assert wctl.month_day == w.month_day

        w = RxTxEntry.subs_cfg_criteria_to_window(
            SubscriptionCfg({
                'criteria':
                    {SubscriptionCfg.KEY_RESET_INTERVAL: '7d'}}))
        wctl = Window('7d')
        assert wctl.ts_window == w.ts_window
        assert None is wctl.month_day is w.month_day
