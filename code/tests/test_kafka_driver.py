import json
import unittest

import mock

from nuvla.notifs.kafka_driver import key_deserializer, value_deserializer, \
    KafkaUpdater


class TestKafkaDriver(unittest.TestCase):

    def test_key_deserializer(self):
        self.assertRaises(AttributeError, key_deserializer, 'a')
        assert 'a' == key_deserializer(b'a')

    def test_value_deserializer(self):
        assert None is value_deserializer(None)
        assert {} == value_deserializer(b'{}')
        assert {'a': 1} == value_deserializer(b'{"a": 1}')
        assert {'a': True} == value_deserializer(b'{"a": true}')
        assert '' == value_deserializer('{"a": 1}')
        assert '' == value_deserializer(b'{"a": }')


class TestKafkaUpdater(unittest.TestCase):

    def test_hello(self):
        d = {}
        ku = KafkaUpdater('test')
        with mock.patch('nuvla.notifs.kafka_driver.kafka_consumer') as kc:
            r_list = []
            for k, v in zip(['a', 'b'], [1, 2]):
                msg = mock.MagicMock()
                msg.key = k
                msg.value = v
                r_list.append(msg)
            kc.return_value = r_list
            ku.run(d).join()
        assert d == {'a': 1, 'b': 2}
