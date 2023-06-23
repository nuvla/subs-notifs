import logging
import os
import sys

log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_formatter)

logger = logging.getLogger('kafka')
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)


def loglevel_from_env(who):
    who_env = f'{who.upper().replace("-", "_")}_LOGLEVEL'
    if who_env not in os.environ:
        return
    try:
        return logging._checkLevel(os.environ[who_env])
    except ValueError as ex:
        logging.info('Wrong log level provided for %s: %s', who_env, ex)
        return


def get_logger(who: str, level=logging.INFO):
    global stdout_handler
    log = logging.getLogger(who)
    log.addHandler(stdout_handler)
    log.setLevel(loglevel_from_env(who) or level)
    return log
