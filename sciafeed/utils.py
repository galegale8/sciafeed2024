"""
This modules provides generic utility functions of the SCIA FEED package
"""
from datetime import datetime
import logging
import os.path

import xlrd

from sciafeed import this_path


LOG_LEVEL = logging.DEBUG
LOG_FOLDER = os.path.join(this_path, 'logs')
LOG_NAMES = (
    'VALIDATOR',
    'DOWNLOADER',
    'PROCESSOR',
    'TESTING'
)


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
        log_filepath = os.path.join(LOG_FOLDER, logfilename)
        for log_name in LOG_NAMES:
            logger = logging.getLogger(log_name)
            if not logger.handlers:
                fh = logging.FileHandler(log_filepath)
                fh.setLevel(LOG_LEVEL)
                formatter = logging.Formatter(logformat, datefmt=datefmt)
                fh.setFormatter(formatter)
                logger.addHandler(fh)
        return func(*args, **kwargs)
    return inner


def cell_str(cell, datemode, datepattern='%d.%m.%Y'):
    """
    Try to get the raw value of an Excel cell as string/unicode

    :param cell: xlrd Cell object of xlrd
    :param datemode: attribute of the worksheet to help parsing date cells
    :param datepattern: pattern used by cells containing dates (as used by datetime.strftime)
    :return: the unicode string of the value of the cell
    """
    if cell.ctype == xlrd.XL_CELL_NUMBER:
        if cell.value == int(cell.value):
            # parse integer
            return str(int(cell.value))
        else:
            # parse float
            return str(cell.value)
    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        # parse boolean
        return str(int(cell.value))
    if cell.ctype == xlrd.XL_CELL_DATE:
        # parse date
        # WARNING: libreoffice is not compliant with Excel for dates!
        dtime = xlrd.xldate.xldate_as_datetime(cell.value, datemode)
        dtime_str = dtime.strftime(datepattern)
        return dtime_str
    return cell.value


def load_excel(filepath, sheet_index=0, sheet_name=None):
    """
    Try to parse the EXCEL file content, returning a list of rows, each row a list of cells.

    :param filepath: path to the Excel file
    :param sheet_index: the sheet index to read (starts from 0). Ignored if `sheet_name` != None
    :param sheet_name: the name of the sheet to read.
    :return: (header, rows, errors)
    """
    workbook = xlrd.open_workbook(filepath)
    if sheet_name:
        sheet = workbook.sheet_by_name(sheet_name)
    else:
        sheet = workbook.sheet_by_index(sheet_index)
    rows = []
    for row_index in range(sheet.nrows):
        row = [cell_str(sheet.cell(row_index, i), workbook.datemode) for i in range(sheet.ncols)]
        rows.append(row)
    return rows
