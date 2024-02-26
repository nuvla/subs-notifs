import logging
import threading
from abc import ABC, abstractmethod
from typing import Dict

from .log import get_logger

log = get_logger('dict-updater')


class DictUpdater(ABC):

    def run(self, obj: Dict):
        t = threading.Thread(target=self._run, args=(obj,), daemon=True)
        t.start()
        return t

    def _run(self, obj: Dict):
        for r in self.do_yield():
            for k, v in r.items():
                obj[k] = v
                if log.level == logging.DEBUG:
                    log.debug('%s %s', k, v)

    @abstractmethod
    def do_yield(self) -> Dict[dict, str]:
        pass
