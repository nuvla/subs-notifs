"""
Event resource class.
"""
import re

from nuvla.notifs.models.resource import Resource


class Event(Resource):
    """
    Event resource class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def content_match_href(self, href_regex: str) -> bool:
        return re.search(href_regex,
                         self['content']['resource']['href']) is not None

    def content_is_state(self, state):
        return self['content']['state'] == state

    def resource_id(self):
        return self['content']['resource']['href']

    def resource_content(self):
        return self['content']['resource']['content']

    def is_name(self, name: str):
        return name == self['name']

    def is_successful(self):
        return True is self['success']

    def is_category(self, category):
        return category == self['category']
