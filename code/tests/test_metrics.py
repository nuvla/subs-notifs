import unittest

from nuvla.notifs.metric import NuvlaEdgeMetrics, MetricNotFound, \
    EX_MSG_TMPL_KEY_NOT_FOUND


class TestNuvlaEdgeResourceMetrics(unittest.TestCase):

    def test_init(self):
        assert 0 == len(NuvlaEdgeMetrics({}))

    def test_metric_not_found(self):
        nerm = NuvlaEdgeMetrics({'RESOURCES': {}, 'RESOURCES_PREV': {}})

        with self.assertRaises(MetricNotFound) as context_mgr:
            nerm._load_pct(nerm.RESOURCES_KEY)
        self.assertEqual(str(context_mgr.exception),
                         EX_MSG_TMPL_KEY_NOT_FOUND.format("'CPU'", 'RESOURCES'))

        self.assertRaises(MetricNotFound, nerm._load_pct, nerm.RESOURCES_KEY)
        self.assertRaises(MetricNotFound, nerm.load_pct_curr)
        self.assertRaises(MetricNotFound, nerm.load_pct_prev)
        self.assertRaises(MetricNotFound, nerm._ram_pct, nerm.RESOURCES_KEY)
        self.assertRaises(MetricNotFound, nerm.ram_pct_curr)
        self.assertRaises(MetricNotFound, nerm.ram_pct_prev)
        self.assertRaises(MetricNotFound, nerm._disk_pct, nerm.RESOURCES_KEY, 'foo')
        self.assertRaises(MetricNotFound, nerm.disk_pct_curr, 'foo')
        self.assertRaises(MetricNotFound, nerm.disk_pct_prev, 'foo')

    def test_load_pct(self):
        nerm = NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}}})
        assert 100 == nerm._load_pct(nerm.RESOURCES_KEY)
        assert 100 == nerm.load_pct_curr()
        assert 50 == nerm._load_pct(nerm.RESOURCES_PREV_KEY)
        assert 50 == nerm.load_pct_prev()

    def test_ram_pct(self):
        nerm = NuvlaEdgeMetrics({
            'RESOURCES': {'RAM': {'used': 1000, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {'RAM': {'used': 500, 'capacity': 1000, 'topic': 'ram'}}})
        assert 100 == nerm._ram_pct(nerm.RESOURCES_KEY)
        assert 100 == nerm.ram_pct_curr()
        assert 50 == nerm._ram_pct(nerm.RESOURCES_PREV_KEY)
        assert 50 == nerm.ram_pct_prev()

    def test_disk_pct(self):
        nerm = NuvlaEdgeMetrics({
            'RESOURCES': {'DISKS': [{'used': 10, 'capacity': 10, 'device': 'C:'}]},
            'RESOURCES_PREV': {'DISKS': [{'used': 5, 'capacity': 10, 'device': 'C:'}]}})
        assert None is nerm._disk_pct(nerm.RESOURCES_KEY, 'A:')
        assert None is nerm._disk_pct(nerm.RESOURCES_PREV_KEY, 'D:')
        assert 100 == nerm._disk_pct(nerm.RESOURCES_KEY, 'C:')
        assert 100 == nerm.disk_pct_curr('C:')
        assert 50 == nerm._disk_pct(nerm.RESOURCES_PREV_KEY, 'C:')
        assert 50 == nerm.disk_pct_prev('C:')

    def test_default_gw_name(self):
        nerm = NuvlaEdgeMetrics({'NETWORK': None})
        assert None is nerm.default_gw_name()

        nerm = NuvlaEdgeMetrics({'NETWORK': {}})
        assert None is nerm.default_gw_name()

        nerm = NuvlaEdgeMetrics({'NETWORK': {'default-gw': None}})
        assert None is nerm.default_gw_name()

        nerm = NuvlaEdgeMetrics({'NETWORK': {'default-gw': 'foo'}})
        assert 'foo' == nerm.default_gw_name()

    def test_default_gw(self):
        nerm = NuvlaEdgeMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert {} == nerm.default_gw_data()

        nerm = NuvlaEdgeMetrics({
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'foo'},
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert {} == nerm.default_gw_data()

        gw = {'interface': 'eth0',
              'bytes-transmitted': 1,
              'bytes-received': 2}
        nerm = NuvlaEdgeMetrics({
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: 'foo'},
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              gw,
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert {} == nerm.default_gw_data()

        gw_name = 'eth0'
        gw = {'interface': gw_name,
              'bytes-transmitted': 1,
              'bytes-received': 2}
        nerm = NuvlaEdgeMetrics({
            'NETWORK': {NuvlaEdgeMetrics.DEFAULT_GW_KEY: gw_name},
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'net-stats': [
                              gw,
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert gw == nerm.default_gw_data()
        assert {'interface': gw_name, 'value': 1} == nerm.default_gw_tx()
        assert {'interface': gw_name, 'value': 2} == nerm.default_gw_rx()
