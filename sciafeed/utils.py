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


def string2lambda(thestring, variable_label='X', round_precision=4):
    """
    Convert a string to a lambda function.
    For example, 'X+1' -> lambda X: X+1.
    The string is checked: no chars [A-Z,a-z] allowed.

    :param thestring: the string defining the lambda function
    :param variable_label: the string used for the function variable
    :param round_precision: precision from the decimal point
    :return: the lambda function
    """
    # some checks
    assert len(variable_label) == 1
    value = thestring.lower().replace('x', '')
    for c_ord in range(ord('a'), ord('z')+1):
        assert chr(c_ord) not in value

    function_txt = 'lambda %s: round(%s, %s)' % (variable_label, thestring, round_precision)
    retvalue = eval(function_txt)
    return retvalue


def is_same_list(list1, list2, any_marker):
    """
    Return True if each element of list1 is the same of list2.
    The comparison of one element of the list return True if the element=`any_marker` for the
    first list.
    """
    if len(list1) != len(list2):
        return False
    for i, elem in enumerate(list1):
        if elem == list2[i] or str(elem) == any_marker:
            continue
        else:
            return False
    return True


def folder2props(folder_name):
    """
    Extract some station/network properties from a folder name,
    according to agreed conventions.

    :param folder_name: the name of the folder
    :return: a dictionary of properties extracted
    """
    ret_value = dict()
    folder_name_tokens = folder_name.split('_')
    cod_rete = folder_name_tokens[0]
    if cod_rete.isdigit():
        ret_value['cod_rete'] = cod_rete
    # case HISCENTRAL
    cod_utente_prefix = folder_name_tokens[-1]
    if ret_value.get('cod_rete') == '15' and cod_utente_prefix.isdigit():
        ret_value['cod_utente_prefix'] = cod_utente_prefix
    return ret_value
