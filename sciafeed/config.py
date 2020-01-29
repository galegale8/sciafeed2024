
import logging
import os.path

from sciafeed import this_path

LOG_LEVEL = logging.DEBUG
LOG_FOLDER = os.path.join(this_path, 'logs')
LOG_NAMES = (
    'VALIDATOR',
    'DOWNLOADER',
    'PROCESSOR',
    'TESTING'
)
