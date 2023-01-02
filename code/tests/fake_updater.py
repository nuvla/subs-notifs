from typing import Dict

from nuvla.notifs.updater import DictUpdater


class FakeUpdaterEmpty(DictUpdater):
    def do_yield(self) -> Dict[str, dict]:
        yield {None: None}


class FakeUpdater(DictUpdater):

    def __init__(self, data):
        self.data = data

    def do_yield(self) -> Dict[str, dict]:
        for k, v in self.data:
            yield {k: v}


def get_updater(data=None):
    if data:
        return FakeUpdater(data)
    return FakeUpdaterEmpty()
