import os
import unittest

from nuvla.notifs.main import es_hosts, ES_HOSTS


class TestEsHosts(unittest.TestCase):

    def test_no_env(self):
        assert ES_HOSTS == es_hosts()

    def test_with_env(self):
        os.environ['ES_HOSTS'] = 'es:9200'
        assert [{'host': 'es', 'port': 9200}] == es_hosts()

        os.environ['ES_HOSTS'] = 'es1:9201,es2:9202'
        assert [{'host': 'es1', 'port': 9201},
                {'host': 'es2', 'port': 9202}] == es_hosts()
