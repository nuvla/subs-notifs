"""
Generic class to hold Nuvla resource as dict. Contains helper methods to access
resource attributes.
"""

from typing import List, Set, Union
from itertools import chain

from .subscription import SubscriptionCfg


class Resource(dict):
    """
    Base class for holding resource metrics as dictionary.
    """

    def __init__(self, *args, **kwargs):
        dict.__init__(self, *args, **kwargs)

    def __getitem__(self, key):
        """Try with keys in upper case to account for ksqlDB key transformation.
        """
        try:
            return dict.__getitem__(self, key)
        except KeyError as ex:
            try:
                return dict.__getitem__(self, key.upper())
            except KeyError:
                raise ex

    def name(self):
        return self['name']

    def id(self):
        return self['id']

    def description(self):
        return self['description']

    def _canonical_timestamp(self, name):
        if self[name] and '.' in self[name]:
            return self[name].split('.')[0] + 'Z'
        return self[name]

    def timestamp(self):
        return self._canonical_timestamp('timestamp')

    def nuvla_timestamp(self):
        try:
            return self._canonical_timestamp('nuvla_timestamp')
        except KeyError:
            return self.timestamp()

    def uuid(self):
        return self['id'].split('/')[1]


def collection_all_owners(coll: Union[List[Resource], List[SubscriptionCfg]]) -> Set[str]:
    """Given a list of resources, returns a set of all owners."""
    return set(chain.from_iterable([x['acl']['owners'] for x in coll]))
