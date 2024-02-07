import threading
import time
from nuvla.notifs.stats.metrics import PROCESSING_TIME


class ThreadStats(threading.Thread):
    def __init__(self, target, args, daemon, _type=None):
        super().__init__(target=target, args=args, daemon=daemon)
        self.type = _type

    def run(self):
        start = time.time()
        super().run()
        PROCESSING_TIME.labels(self.type).set(time.time() - start)
