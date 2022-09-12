import threading
from abc import ABC, abstractmethod
from typing import Dict

from nuvla.notifs.log import get_logger

log = get_logger('dict-updater')


class DictUpdater(ABC):

    def run(self, obj: Dict):
        t1 = threading.Thread(target=self._run, args=(obj,), daemon=True)
        t1.start()
        return t1

    def _run(self, obj: Dict):
        for r in self.do_yield():
            for k, v in r.items():
                obj[k] = v

    @abstractmethod
    def do_yield(self) -> Dict[dict, str]:
        pass
