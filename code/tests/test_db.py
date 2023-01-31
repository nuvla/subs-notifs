from datetime import datetime, timedelta

from mock import Mock
import unittest

import elasticsearch

from nuvla.notifs.metric import NENetMetric
from nuvla.notifs.subscription import SubscriptionCfg

from es_patch import es_update
import elasticmock.fake_elasticsearch
elasticmock.fake_elasticsearch.FakeElasticsearch.update = es_update

from es_patch import es_index_create
import elasticmock.fake_indices
elasticmock.fake_indices.FakeIndicesClient.create = es_index_create

from es_patch import es_index_put_mapping
import elasticmock.fake_indices
elasticmock.fake_indices.FakeIndicesClient.put_mapping = es_index_put_mapping

from es_patch import get_elasticmock
elasticmock = get_elasticmock()

import nuvla.notifs.window as window
window_now_orig = datetime.now

from nuvla.notifs.db import RxTxDriverES, gb_to_bytes, DBInconsistentStateError
from nuvla.notifs.schema.rxtx import RxTx, RxTxEntry


class TestDBUtils(unittest.TestCase):

    def test_gb_to_bytes(self):
        assert 0 == gb_to_bytes(0.0)
        assert 1 * 1024 ** 3 == gb_to_bytes(1.)
        assert 1288490188 == gb_to_bytes(1.2)
        assert 5057323991 == gb_to_bytes(4.71)
        assert 41875931 == gb_to_bytes(0.039)


class TestRxTxDriverESDataMgmt(unittest.TestCase):

    def test_data_mgmt(self):
        data_base = RxTxDriverES._doc_base('subs/01', 'ne/01', 'eth0', 'rx')
        assert {'subs_id': 'subs/01',
                'ne_id': 'ne/01',
                'iface': 'eth0',
                'kind': 'rx'} == data_base

        rx_entry = RxTxEntry(SubscriptionCfg({'id': 'subs/01',
                                              'criteria': {}}),
                             NENetMetric({'id': 'ne/01',
                                          'iface': 'eth0',
                                          'kind': 'rx',
                                          'value': 1}))
        data = RxTxDriverES._doc_build(rx_entry)
        assert 'rxtx' in data
        assert data.get('rxtx') is not None

        rxtx = RxTx()
        rxtx.set(3)
        data = RxTxDriverES._doc_build(rx_entry, rxtx=rxtx)
        assert 'rxtx' in data
        assert data.get('rxtx') is not None
        rxtx_updated = RxTxDriverES._rxtx_deserialize(data.get('rxtx'))
        assert 1 == rxtx_updated.total()


class TestRxTxDriverESMockedBase(unittest.TestCase):

    @elasticmock
    def setUp(self):
        self.driver: RxTxDriverES = RxTxDriverES()
        self.driver.index_delete()
        try:
            self.driver.connect()
        except Exception as ex:
            self.driver.index_delete()
            raise ex

    @elasticmock
    def tearDown(self):
        self.driver.index_delete()
        self.driver.close()
        self.driver = None

        window.now = window_now_orig


class TestRxTxDriverESMockedCreateIndex(unittest.TestCase):

    @elasticmock
    def setUp(self):
        RxTxDriverES().index_delete()

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

    @elasticmock
    def test_init(self):
        assert self.driver.es is not None
        assert [] == self.driver.index_all_docs()

    def test_set_get_basic(self):
        subs_id = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'
        value = 1

        rx_entry = RxTxEntry(SubscriptionCfg({'id': subs_id,
                                              'criteria': {}}),
                             NENetMetric({'id': ne_id,
                                          'iface': iface,
                                          'kind': kind,
                                          'value': value}))
        self.driver.set(rx_entry)

        assert 1 == self.driver.index_count()

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
        assert 'month' == rxtx.window.ts_window

    def test_set_get_custom_window(self):
        subs_id_1 = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'
        value = 1

        rx_entry = RxTxEntry(
            SubscriptionCfg(
                {'id': subs_id_1,
                 'criteria': {SubscriptionCfg.KEY_RESET_INTERVAL: 'month'}}),
            NENetMetric({'id': ne_id,
                         'iface': iface,
                         'kind': kind,
                         'value': value}))
        self.driver.set(rx_entry)
        assert 1 == self.driver.index_count()

        rxtx = self.driver.get_data(subs_id_1, ne_id, iface, kind)
        assert rxtx is not None
        assert 0 == rxtx.total()
        assert False is rxtx.above_thld
        assert 'month' == rxtx.window.ts_window
        assert 1 == rxtx.window.month_day

        subs_id_2 = 'subscription/02'
        rx_entry = RxTxEntry(
            SubscriptionCfg(
                {'id': subs_id_2,
                 'criteria': {SubscriptionCfg.KEY_RESET_INTERVAL: 'month',
                              SubscriptionCfg.KEY_RESET_START_DAY: 25}}),
            NENetMetric({'id': ne_id,
                         'iface': iface,
                         'kind': kind,
                         'value': value}))
        self.driver.set(rx_entry)
        assert 2 == self.driver.index_count()

        rxtx = self.driver.get_data(subs_id_2, ne_id, iface, kind)
        assert rxtx is not None
        assert 0 == rxtx.total()
        assert False is rxtx.above_thld
        assert 'month' == rxtx.window.ts_window
        assert 25 == rxtx.window.month_day

    def test_inconsistent_db(self):
        subs_id = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'
        value = 1

        rx_entry = RxTxEntry(SubscriptionCfg({'id': subs_id,
                                              'criteria': {}}),
                             NENetMetric({'id': ne_id,
                                          'iface': iface,
                                          'kind': kind,
                                          'value': value}))
        self.driver.set(rx_entry)
        assert 1 == self.driver.index_count()
        rxtx = self.driver.get_data(subs_id, ne_id, iface, kind)
        assert rxtx is not None
        assert 0 == rxtx.total()
        assert False is rxtx.above_thld
        assert 'month' == rxtx.window.ts_window

        doc = self.driver._doc_build(rx_entry)
        self.driver._insert(doc)
        assert 2 == self.driver.index_count()
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

        rx_entry = RxTxEntry(SubscriptionCfg({'id': subs_id,
                                              'criteria': {}}),
                             NENetMetric({'id': ne_id,
                                          'iface': iface,
                                          'kind': kind,
                                          'value': 1}))
        self.driver.set(rx_entry)
        assert 1 == self.driver.index_count()

        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)

        rx_entry = RxTxEntry(SubscriptionCfg({'id': subs_id,
                                              'criteria': {}}),
                             NENetMetric({'id': ne_id,
                                          'iface': iface,
                                          'kind': kind,
                                          'value': 2}))
        self.driver.set(rx_entry)
        assert 1 == self.driver.index_count()

        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 1)

        self.driver.reset(subs_id, ne_id, iface, kind)
        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)

        rx_entry = RxTxEntry(SubscriptionCfg({'id': subs_id,
                                              'criteria': {}}),
                             NENetMetric({'id': ne_id,
                                          'iface': iface,
                                          'kind': kind,
                                          'value': 10}))
        self.driver.set(rx_entry)
        assert 1 == self.driver.index_count()

        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)

        self.driver.reset(subs_id, ne_id, iface, kind)
        rx = self.driver.get_data(subs_id, ne_id, iface, kind)
        rx_validate(rx, 0)


class TestRxTxDriverESWithWindow(TestRxTxDriverESMockedBase):

    def resets_workflow(self, reset_day, current_day):
        def rxtx_entry_from_value(value):
            return RxTxEntry(
                SubscriptionCfg(
                    {'id': subs_id,
                     'criteria': {SubscriptionCfg.KEY_RESET_INTERVAL: 'month',
                                  SubscriptionCfg.KEY_RESET_START_DAY: reset_day}}),
                NENetMetric({'id': ne_id,
                             'iface': iface,
                             'kind': kind,
                             'value': value}))

        subs_id = 'subscription/01'
        ne_id = 'nuvlaedge/01'
        iface = 'eth0'
        kind = 'rx'

        window.now = Mock(return_value=datetime.now().replace(day=current_day))

        self.driver.set(rxtx_entry_from_value(1))
        assert 1 == self.driver.index_count()

        rxtx = self.driver.get_data(subs_id, ne_id, iface, kind)
        assert rxtx is not None
        assert 0 == rxtx.total()
        assert [[1, 1]] == rxtx.value.get()
        assert False is rxtx.above_thld
        assert 'month' == rxtx.window.ts_window
        assert reset_day == rxtx.window.month_day

        self.driver.set(rxtx_entry_from_value(2))
        assert 1 == self.driver.index_count()

        rxtx = self.driver.get_data(subs_id, ne_id, iface, kind)
        assert rxtx is not None
        assert 1 == rxtx.total()
        assert [[1, 2]] == rxtx.value.get()
        assert False is rxtx.above_thld
        assert 'month' == rxtx.window.ts_window
        assert reset_day == rxtx.window.month_day

        # jump to the future to the next day after 'reset_day'
        days_delta = reset_day - window.now().day + 1
        window.now = Mock(return_value=window.now() + timedelta(days=days_delta))
        assert reset_day + 1 == window.now().day

        self.driver.set(rxtx_entry_from_value(3))
        assert 1 == self.driver.index_count()

        rxtx = self.driver.get_data(subs_id, ne_id, iface, kind)
        assert rxtx is not None
        if current_day >= reset_day:
            assert 2 == rxtx.total()
            assert [[1, 3]] == rxtx.value.get()
        else:
            assert 0 == rxtx.total()
            assert [[3, 3]] == rxtx.value.get()
        assert False is rxtx.above_thld
        assert 'month' == rxtx.window.ts_window
        assert reset_day == rxtx.window.month_day

    def test_current_day_before_reset_day(self):
        reset_day = 15
        current_day = 10
        self.resets_workflow(reset_day, current_day)

    def test_current_day_equal_reset_day(self):
        reset_day = 15
        current_day = 15
        self.resets_workflow(reset_day, current_day)

    def test_current_day_after_reset_day(self):
        reset_day = 15
        current_day = 20
        self.resets_workflow(reset_day, current_day)
