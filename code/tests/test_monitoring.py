import unittest
from unittest.mock import patch, MagicMock

from nuvla.notifs.monitoring import search_if_present, act_on_deleted_subscriptions


class TestMonitoring(unittest.TestCase):
    @patch('nuvla.notifs.monitoring.get_all_es_records')
    def test_search_if_present(self, mock_get_all_records):
        mock_get_all_records.side_effect = [['1111', '1111'], ['2222', '3333', '1111', '2222']]
        elastic_instance = None
        deleted_subscription_or_nuvlaedge_ids = ['nuvlabox/1', 'subscription-config/1']
        ids_rxtx_to_be_deleted = search_if_present(elastic_instance, deleted_subscription_or_nuvlaedge_ids)
        self.assertEqual({'1111', '2222', '3333'}, ids_rxtx_to_be_deleted)

    @patch('nuvla.notifs.monitoring.search_if_present')
    def test_act_on_deleted_subscriptions(self, mock_search_if_present):
        elastic_instance = MagicMock()
        elastic_instance.search.side_effect = [{
            'hits': {'hits': [{'_id': '1111'}, {'_id': '2222'}, {'_id': '3333'}]}},
            {'hits': {'hits': []}}]
        mock_search_if_present.return_value = {'1111', '2222', '3333'}
        act_on_deleted_subscriptions(elastic_instance)


if __name__ == '__main__':
    unittest.main()
