import unittest

from nuvla.notifs.metric import NuvlaEdgeResourceMetrics, MetricNotFound, \
    EX_MSG_TMPL_KEY_NOT_FOUND


class TestNuvlaEdgeResourceMetrics(unittest.TestCase):

    def test_init(self):
        assert 0 == len(NuvlaEdgeResourceMetrics({}))

    def test_metric_not_found(self):
        nerm = NuvlaEdgeResourceMetrics({'RESOURCES': {}, 'RESOURCES_PREV': {}})

        with self.assertRaises(MetricNotFound) as context_mgr:
            nerm._load_pct(nerm.resources)
        self.assertEqual(str(context_mgr.exception),
                         EX_MSG_TMPL_KEY_NOT_FOUND.format("'CPU'", 'RESOURCES'))

        self.assertRaises(MetricNotFound, nerm._load_pct, nerm.resources)
        self.assertRaises(MetricNotFound, nerm.load_pct_curr)
        self.assertRaises(MetricNotFound, nerm.load_pct_prev)
        self.assertRaises(MetricNotFound, nerm._ram_pct, nerm.resources)
        self.assertRaises(MetricNotFound, nerm.ram_pct_curr)
        self.assertRaises(MetricNotFound, nerm.ram_pct_prev)
        self.assertRaises(MetricNotFound, nerm._disk_pct, nerm.resources, 'foo')
        self.assertRaises(MetricNotFound, nerm.disk_pct_curr, 'foo')
        self.assertRaises(MetricNotFound, nerm.disk_pct_prev, 'foo')

    def test_load_pct(self):
        nerm = NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'}},
            'RESOURCES_PREV': {'CPU': {'load': 2.0, 'capacity': 4, 'topic': 'cpu'}}})
        assert 100 == nerm._load_pct(nerm.resources)
        assert 100 == nerm.load_pct_curr()
        assert 50 == nerm._load_pct(nerm.resources_prev)
        assert 50 == nerm.load_pct_prev()

    def test_ram_pct(self):
        nerm = NuvlaEdgeResourceMetrics({
            'RESOURCES': {'RAM': {'used': 1000, 'capacity': 1000, 'topic': 'ram'}},
            'RESOURCES_PREV': {'RAM': {'used': 500, 'capacity': 1000, 'topic': 'ram'}}})
        assert 100 == nerm._ram_pct(nerm.resources)
        assert 100 == nerm.ram_pct_curr()
        assert 50 == nerm._ram_pct(nerm.resources_prev)
        assert 50 == nerm.ram_pct_prev()

    def test_disk_pct(self):
        nerm = NuvlaEdgeResourceMetrics({
            'RESOURCES': {'DISKS': [{'used': 10, 'capacity': 10, 'device': 'C:'}]},
            'RESOURCES_PREV': {'DISKS': [{'used': 5, 'capacity': 10, 'device': 'C:'}]}})
        assert None is nerm._disk_pct(nerm.resources, 'A:')
        assert None is nerm._disk_pct(nerm.resources_prev, 'D:')
        assert 100 == nerm._disk_pct(nerm.resources, 'C:')
        assert 100 == nerm.disk_pct_curr('C:')
        assert 50 == nerm._disk_pct(nerm.resources_prev, 'C:')
        assert 50 == nerm.disk_pct_prev('C:')

    def test_default_gw(self):
        nerm = NuvlaEdgeResourceMetrics({
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert {} == nerm.default_gw()

        nerm = NuvlaEdgeResourceMetrics({
            'NETWORK': {'default_gw': 'foo'},
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert {} == nerm.default_gw()

        gw = {'interface': 'eth0',
              'bytes-transmitted': 1,
              'bytes-received': 2}
        nerm = NuvlaEdgeResourceMetrics({
            'NETWORK': {'default_gw': 'foo'},
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              gw,
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert {} == nerm.default_gw()

        gw_name = 'eth0'
        gw = {'interface': gw_name,
              'bytes-transmitted': 1,
              'bytes-received': 2}
        nerm = NuvlaEdgeResourceMetrics({
            'NETWORK': {'default_gw': gw_name},
            'RESOURCES': {'CPU': {'load': 4.0, 'capacity': 4, 'topic': 'cpu'},
                          'NET-STATS': [
                              gw,
                              {'interface': 'lo',
                               'bytes-transmitted': 1,
                               'bytes-received': 2}]}})
        assert gw == nerm.default_gw()
        assert {'interface': gw_name, 'value': 1} == nerm.default_gw_tx()
        assert {'interface': gw_name, 'value': 2} == nerm.default_gw_rx()
