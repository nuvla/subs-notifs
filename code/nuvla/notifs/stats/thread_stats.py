import threading
import time
from nuvla.notifs.stats.metrics import PROCESSING_TIME
from nuvla.notifs.log import get_logger

log = get_logger('thread_stats')


class ThreadStats(threading.Thread):
    def __init__(self, target, args, daemon, _type=None):
        super().__init__(target=target, args=args, daemon=daemon)
        self.type = _type

    def run(self):
        log.info('ThreadStats run %s', self.type)
        start = time.time()
        super().run()
        log.info(f'ThreadStats done {time.time() - start} secs')
        PROCESSING_TIME.labels(self.type).set(time.time() - start)
