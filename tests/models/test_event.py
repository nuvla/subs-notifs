import unittest

from nuvla.notifs.models.event import Event


class TestModelsEvent(unittest.TestCase):

    def test_is_methods(self):
        assert Event({'name': 'foo'}).is_name('foo') is True
        assert Event({'name': 'foo'}).is_name('bar') is False

        assert Event({'success': True}).is_successful() is True
        assert Event({'success': False}).is_successful() is False

        assert Event({'category': 'foo'}).is_category('foo') is True
        assert Event({'category': 'foo'}).is_category('bar') is False

    def test_tags(self):
        assert Event({'tags': ['foo']}).tags_contains('foo') is True
        assert Event({'tags': ['foo']}).tags_contains('bar') is False
        assert Event({'tags': ['foo', 'bar']}).tags_contains('bar') is True
