"""
This module contains the functions and utilities to parse an ARPA file with 21 variables.
"""
import csv
from datetime import datetime
from os.path import abspath, basename, join, splitext
from pathlib import PurePath

from sciafeed import TEMPLATES_PATH
from sciafeed import utils

MISSING_VALUE_MARKER = '32767'
PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'arpa21_params.csv')
LIMITING_PARAMETERS = {
    'Tmedia': ('Tmin', 'Tmax'),
    'UR media': ('UR min', 'UR max'),
    'P': ('Pmin', 'Pmax'),
}
FORMAT_LABEL = 'ARPA-21'


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the arpa21 stored parameters.
    Return a dictionary of type:
    ::

        {   1: dictionary of properties of parameter stored at position 1,
            index i: dictionary of properties of parameter stored at position i,
            19: dictionary of properties of parameter stored at position 19
        }

    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param delimiter: CSV delimiter
    :return: dictionary of positions with parameters information
    """
    csv_file = open(parameters_filepath, 'r')
    csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        position = int(row['position'])
        ret_value[position] = dict()
        for prop in row.keys():
            ret_value[position][prop] = row[prop].strip()
        ret_value[position]['convertion'] = utils.string2lambda(ret_value[position]['convertion'])
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the arpa21 stored parameters.
    Return a dictionary of type:
    ::

        {   param_code: [min_value, max_value]
        }

    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param delimiter: CSV delimiter
    :return: dictionary of parameters with their ranges
    """
    csv_file = open(parameters_filepath, 'r')
    csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        par_code = row['par_code']
        try:
            min_threshold, max_threshold = map(float, [row['min'], row['max']])
            ret_value[par_code] = [min_threshold, max_threshold]
        except (KeyError, TypeError, ValueError):
            continue
    return ret_value


def parse_filename(filename: str):
    """
    Return (<code of the station>, <start datetime object>, <end datetime object>)
    corresponding to the arpa21 input file named `filename`.
    The function assumes the filename is validated (see `validate_filename`).

    :param filename: the name of the arpa21 file
    :return: the tuple (<code>, <start date>, <end date>)
    """
    name, ext = splitext(filename)
    assert ext.lower() == '.dat'
    tokens = name.split('_')
    assert len(tokens) == 4
    code = tokens[1].zfill(5)
    assert len(code) == 5
    start_str = tokens[2]
    end_str = tokens[3]
    start_obj = datetime.strptime(start_str, '%Y%m%d%H%M')
    end_obj = datetime.strptime(end_str, '%Y%m%d%H%M')
    assert start_obj <= end_obj
    ret_value = (code, start_obj, end_obj)
    return ret_value


def validate_filename(filename: str):
    """
    Check the name of the input arpa21 file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the arpa21 file
    :return: the string describing the error
    """
    err_msg = ''
    name, ext = splitext(filename)
    if ext.lower() != '.dat':
        err_msg = 'Extension expected must be .dat, found %s' % ext
        return err_msg
    tokens = name.split('_')
    if len(tokens) != 4:
        err_msg = 'File name %r is not standard' % filename
        return err_msg
    code = tokens[1].zfill(5)
    if len(code) != 5:
        err_msg = 'Station code %r is too long' % code
        return err_msg
    start_str = tokens[2]
    end_str = tokens[3]
    try:
        start_obj = datetime.strptime(start_str, '%Y%m%d%H%M')
    except ValueError:
        err_msg = 'Start date in file name %r is not standard' % filename
        return err_msg
    try:
        end_obj = datetime.strptime(end_str, '%Y%m%d%H%M')
    except ValueError:
        err_msg = 'End date in file name %r is not standard' % filename
        return err_msg
    if start_obj > end_obj:
        err_msg = 'The time interval in file name %r is not valid' % filename
    return err_msg


def extract_metadata(filepath, parameters_filepath):
    """
    Extract generic metadata information from a file `filepath` of format arpa21.
    Return the dictionary of the metadata extracted.
    The function assumes the file name is validated against the format (see function
    `validate_filename`).

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: dictionary of metadata extracted
    """
    source = join(*PurePath(abspath(filepath)).parts[-2:])
    filename = basename(filepath)
    code, start_obj, end_obj = parse_filename(filename)
    ret_value = {'cod_utente': code, 'start_date': start_obj, 'end_date': end_obj,
                 'source': source, 'format': FORMAT_LABEL}
    return ret_value


def parse_row(row, parameters_map, only_valid=False, missing_value_marker=MISSING_VALUE_MARKER,
              metadata=None):
    """
    Parse a row of a arpa21 file, and return the parsed data. Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row of the arpa21 file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param only_valid: parse only values flagged as valid (default: False)
    :param missing_value_marker: the string used as a marker for missing value
    :param metadata: default metadata if not provided in the row
    :return: [(metadata, datetime object, par_code, par_value, flag), ...]
    """
    if metadata is None:
        metadata = dict()
    else:
        metadata = metadata.copy()
    tokens = row.split()
    date_str = tokens[0]
    date_obj = datetime.strptime(date_str, '%Y%m%d%H%M')
    metadata['lat'] = float(tokens[1])
    par_values = tokens[2:23]
    par_flags = tokens[23:]
    data = []
    for i, (param_i_value_str, flag_num) in enumerate(zip(par_values, par_flags)):
        props = parameters_map[i + 1]
        param_i_code = props['par_code']
        if param_i_value_str == missing_value_marker:
            param_i_value = None
        else:
            param_i_value = props['convertion'](float(param_i_value_str))
        flag = float(flag_num) <= 1
        if not flag and only_valid:
            continue
        measure = [metadata, date_obj, param_i_code, param_i_value, flag]
        data.append(measure)
    return data


def validate_row_format(row):
    """
    It checks a row of an arpa21 file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: the arpa21 file row to validate
    :return: the string describing the error
    """
    err_msg = ''
    if not row.strip():  # no errors on empty rows
        return err_msg
    tokens = row.split()
    if len(tokens) != 44:
        err_msg = "The number of components in the row is wrong"
        return err_msg
    try:
        datetime.strptime(tokens[0], '%Y%m%d%H%M')
    except ValueError:
        err_msg = "The date format in the row is wrong"
        return err_msg
    for value in tokens[1:]:
        try:
            float(value)
        except (ValueError, TypeError):
            err_msg = "The row contains not numeric values"
            return err_msg

    # characters spacing
    if row[:12] != tokens[0]:
        err_msg = "The date length in the row is wrong"
        return err_msg
    if row[13:22] != tokens[1]:
        err_msg = "The latitude length in the row is wrong"
        return err_msg
    par_row = row[22:].rstrip('\n')
    for token_index, i in enumerate(range(0, len(par_row), 7)):
        if par_row[i:i+7].lstrip() != tokens[token_index+2]:
            err_msg = "The spacing in the row is wrong"
            return err_msg
    return err_msg


def rows_generator(filepath, parameters_map, metadata):
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            yield i, row


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH, only_valid=False,
          missing_value_marker=MISSING_VALUE_MARKER):
    """
    Read an arpa21 file located at `filepath` and returns the data stored inside. 
    Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]
    
    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.
    
    :param filepath: path to the arpa21 file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param only_valid: parse only values flagged as valid (default: False)
    :param missing_value_marker: the string used as a marker for missing value
    :return: [(stat_props, datetime object, par_code, par_value, flag), ...]
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    metadata = extract_metadata(filepath, parameters_filepath)
    data = []
    for i, row in rows_generator(filepath, parameters_map, metadata):
        parsed_row = parse_row(
            row, parameters_map, only_valid=only_valid,
            missing_value_marker=missing_value_marker, metadata=metadata)
        data.extend(parsed_row)
    return data


def validate_format(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Open an arpa21 file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the arpa21 file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)
    if err_msg:
        return [(0, err_msg)]
    found_errors = []
    metadata = extract_metadata(filepath, parameters_filepath)
    start, end = metadata['start_date'], metadata['end_date']
    parameters_map = load_parameter_file(parameters_filepath)
    with open(filepath) as fp:
        last_row_date = None
        last_row = None
        official_lat = None
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            err_msg = validate_row_format(row)
            if err_msg:
                found_errors.append((i, err_msg))
                continue
            row_measures = parse_row(row, parameters_map, metadata=metadata)
            if not row_measures:
                continue
            current_row_date = row_measures[0][1]
            if not official_lat:
                # NOTE: assuming the official latitude is the one in the first row
                official_lat = row_measures[0][0].get('lat')
            current_row_lat = row_measures[0][0].get('lat')
            if last_row_date and last_row_date > current_row_date:
                err_msg = "it is not strictly after the previous"
                found_errors.append((i, err_msg))
            elif official_lat and official_lat != current_row_lat:
                err_msg = "the latitude changes"
                found_errors.append((i, err_msg))
            elif last_row and last_row_date and last_row_date == current_row_date and \
                    row != last_row:
                err_msg = "duplication of rows with different data"
                found_errors.append((i, err_msg))
            elif not start <= current_row_date <= end:
                err_msg = "the time is not coherent with the filename"
                found_errors.append((i, err_msg))
            last_row_date = current_row_date
            last_row = row
    return found_errors


def is_format_compliant(filepath):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :return: True if the file is compliant, False otherwise
    """
    filename = basename(filepath)
    if validate_filename(filename):
        return False
    # check first row
    with open(filepath) as fp:
        first_row = fp.readline()
        if len(first_row.split()) != 44:
            return False
    return True
