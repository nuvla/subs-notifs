import unittest

from nuvla.notifs.models.subscription import SubscriptionCfg
from nuvla.notifs.models.resource import collection_all_owners


class TestResource(unittest.TestCase):

    def test_all_owners(self):
        """Extraction of the owners of the subscription configurations."""
        assert set() == collection_all_owners([])

        me = SubscriptionCfg({'acl': {'owners': ['me']}})
        assert {'me'} == collection_all_owners([me])

        you = SubscriptionCfg({'acl': {'owners': ['you']}})
        assert {'me', 'you'} == collection_all_owners([me, you])

        me_and_you = SubscriptionCfg({'acl': {'owners': ['me', 'you']}})
        assert {'me', 'you'} == collection_all_owners([me_and_you])

        assert {'me', 'you'} == collection_all_owners([me, you, me_and_you])
