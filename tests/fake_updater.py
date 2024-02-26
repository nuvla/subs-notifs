from typing import Dict, List, Tuple

from nuvla.notifs.dictupdater import DictUpdater


class FakeUpdaterEmpty(DictUpdater):
    def do_yield(self) -> Dict[str, dict]:
        yield {None: None}


class FakeUpdater(DictUpdater):

    def __init__(self, data: List[Tuple[str, dict]]):
        self.data = data

    def do_yield(self) -> Dict[str, dict]:
        for k, v in self.data:
            yield {k: v}


def get_updater(data=None):
    if data:
        return FakeUpdater(data)
    return FakeUpdaterEmpty()
