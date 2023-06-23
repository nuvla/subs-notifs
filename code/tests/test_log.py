import logging
import os
import unittest

from nuvla.notifs.log import loglevel_from_env


class TestLog(unittest.TestCase):

    def setUp(self) -> None:
        for v in ('FOO_LOGLEVEL',
                  'FOO_BAR_BAZ1_LOGLEVEL'):
            if v in os.environ:
                os.environ.pop(v)

    def test_no_env(self):
        assert None is loglevel_from_env('foo')

    def test_level_from_env(self):
        os.environ['FOO_LOGLEVEL'] = 'DEBUG'
        assert logging.DEBUG == loglevel_from_env('foo')

        os.environ['FOO_LOGLEVEL'] = 'bad value'
        assert None is loglevel_from_env('foo')

        os.environ['FOO_BAR_BAZ1_LOGLEVEL'] = 'INFO'
        assert logging.INFO == loglevel_from_env('foo-bar-baz1')
