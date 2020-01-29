"""
This modules provides generic utility functions of the SCIA FEED package
"""
from datetime import datetime
import logging
import os.path

from sciafeed import config


def hello_world():
    """
    Example function just for testing
    """
    ret_value = 'Hello world!'
    return ret_value


def setup_log(func):  # pragma: no cover
    """
    Configure logging:
    - main formatting of all log messages
    - standard output prints at level logging.WARNING (or higher);
    - loggers of names config.LOG_NAMES are setup to write on a file inside the config.LOG_FOLDER
    - logs messages in the file are written with config.LOG_LEVEL (or higher)
    The function is managed to be used as a python decorator.

    :param func: function that triggers the setup of the log
    """
    logformat = '%(asctime)s [%(name).7s] %(levelname)s: %(message)s'
    datefmt = '%d %b %Y %H:%M:%S'
    logging.basicConfig(
        level=logging.DEBUG,
        format=logformat,
        datefmt=datefmt)
    main_logger = logging.getLogger()
    main_logger.handlers[0].setLevel(logging.WARNING)
    logfilename = datetime.now().strftime("%y%b%d-%H%M%S") + '.log'

    def inner(*args, **kwargs):
        log_filepath = os.path.join(config.LOG_FOLDER, logfilename)
        for log_name in config.LOG_NAMES:
            logger = logging.getLogger(log_name)
            if not logger.handlers:
                fh = logging.FileHandler(log_filepath)
                fh.setLevel(config.LOG_LEVEL)
                formatter = logging.Formatter(logformat, datefmt=datefmt)
                fh.setFormatter(formatter)
                logger.addHandler(fh)
        return func(*args, **kwargs)
    return inner
