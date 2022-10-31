import logging
import sys

log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_formatter)

logger = logging.getLogger('kafka')
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)


def get_logger(who, level=logging.DEBUG):
    global stdout_handler
    log = logging.getLogger(who)
    log.addHandler(stdout_handler)
    log.setLevel(level)
    return log
