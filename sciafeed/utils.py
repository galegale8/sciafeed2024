"""
This modules provides generic utility functions of the SCIA FEED package
"""
import csv
from datetime import datetime, timedelta
import gzip
import itertools
import logging
import os
import os.path
import random
import shutil

import xlrd

from sciafeed import LOG_NAME


def is_float(value):
    try:
        float(value)
        return True
    except:
        return False


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


def parse_date(thedate, patterns):
    """
    Try to extract the date object using the input patterns.

    :param thedate: the date string
    :param patterns: list of date patterns
    :return: the date obj or None
    """
    ret_value = None
    for pattern in patterns:
        try:
            ret_value = datetime.strptime(thedate, pattern)
        except ValueError:
            continue
    return ret_value


def create_random_samples(par_code, hour_step=1, values_range=(-100, 100)):  # pragma: no cover
    metadata = {'a metadata': 'a value'}
    start_time = datetime(2020, 1, 1, 0, 0)
    ret_value = []
    for thehour in range(0, 24, hour_step):
        par_value = round(random.uniform(*values_range), 4)
        valid = random.choice([True, False])
        record = (metadata, start_time+timedelta(hours=thehour), par_code, par_value, valid)
        ret_value.append(record)
    return ret_value


def different_data_record_info(data_record):
    """
    Used to group all the records with the same station and date
    """
    metadata, row_date, par_code, par_value, par_flag = data_record
    station_id = (metadata.get('cod_utente'), metadata.get('cod_rete'))
    return station_id, row_date


def open_csv_writers(parent_folder, tables_map):
    writers = dict()
    for table, columns in tables_map.items():
        csv_path = os.path.join(parent_folder, '%s.csv' % table)
        if not os.path.exists(csv_path):
            fp = open(csv_path, 'w')
            writer = csv.DictWriter(fp, fieldnames=columns, delimiter=';')
            writer.writeheader()
        else:
            fp = open(csv_path, 'a')
            writer = csv.DictWriter(fp, fieldnames=columns, delimiter=';')
        writers[table] = writer, fp
    return writers


def close_csv_writers(writers):
    for _, fp in writers.values():
        fp.close()


def extract_gz(input_path, output_path, rm_source=False):
    """
    Unzip a .gz file located in `input_path` into a file located at `output_path`

    :param input_path: gz archive file
    :param output_path: unzipped archive
    :param rm_source: if True, remove input file when done
    """
    with gzip.open(input_path, 'rb') as f_in:
        with open(output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    if rm_source and input_path != output_path:
        os.remove(input_path)


def gettime(thefunction):
    """decorator to print start and ending time"""
    name = thefunction.__name__

    def wrapper(*args, **kwargs):
        start = datetime.now()
        print('start time of %s: %r' % (name, start))
        res = thefunction(*args, **kwargs)
        end = datetime.now()
        print('end time of %s: %r' % (name, end))
        elapsed_sec = (end-start).seconds
        print('seconds occurred for %s: %s' % (name, elapsed_sec))
        return res

    return wrapper


def setup_log(report_path=None, log_format='%(asctime)s %(levelname)s: %(message)s',
              log_datefmt='%d-%m-%Y %H:%M:%S'):
    """
    Function to setup global logging on standard output and file.

    :param report_path: if not None, the path of a logging file
    :param log_format: template schema for reporting lines
    :param log_datefmt: format for datetimes in the reporting lines
    :return: the logging object
    """
    # sometimes it needs
    [logging.root.removeHandler(handler) for handler in logging.root.handlers[:]]
    log_name = LOG_NAME + datetime.now().isoformat()
    level_for_standard_output = logging.INFO
    level_for_report_file = logging.INFO

    logger = logging.getLogger(log_name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(log_format, datefmt=log_datefmt)

    # setup standard output
    std_output_handler = logging.StreamHandler()
    std_output_handler.setLevel(level_for_standard_output)
    std_output_handler.setFormatter(formatter)
    logger.addHandler(std_output_handler)

    if not report_path:
        return logger

    # setup report file
    log_filepath = os.path.abspath(report_path)
    file_handler = logging.FileHandler(log_filepath)
    file_handler.setLevel(level_for_report_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    return logger


def chunked_iterable(iterable, size):
    """
    Divide `iterable` in chunks of size `size`. Return iterable of chunks

    :param iterable: input iterable
    :param size: size of each chunk
    :return: iterable of chunks
    """
    it = iter(iterable)
    while True:
        chunk = tuple(itertools.islice(it, size))
        if not chunk:
            break
        yield chunk
