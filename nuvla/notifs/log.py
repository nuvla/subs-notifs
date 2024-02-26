import logging
import os
import sys
from typing import Union

log_formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(log_formatter)

logger = logging.getLogger('kafka')
logger.addHandler(stdout_handler)
logger.setLevel(logging.INFO)


def loglevel_from_env(who: str) -> Union[int, None]:
    """
    In case `who`_LOGLEVEL is not set, ALL_LOGLEVEL is tried.

    :param who: component name
    :return: log level as int or None
    """
    who_env = f'{who.upper().replace("-", "_")}_LOGLEVEL'
    all_env = 'ALL_LOGLEVEL'
    for env in (who_env, all_env):
        try:
            if env in os.environ:
                return logging._checkLevel(os.environ[env])
        except ValueError as ex:
            logging.warning('Wrong log level provided for %s: %s', env, ex)


def get_logger(who: str, level=logging.INFO):
    global stdout_handler
    log = logging.getLogger(who)
    log.addHandler(stdout_handler)
    log.setLevel(loglevel_from_env(who) or level)
    return log
