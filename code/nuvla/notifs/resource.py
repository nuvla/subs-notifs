"""
Generic class to hold Nuvla resource as dict. Contains helper methods to access
resource attributes.
"""


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

    def description(self):
        return self['description']

    def timestamp(self):
        return self['timestamp']

    def uuid(self):
        return self['id'].split('/')[1]