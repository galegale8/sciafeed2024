"""
This module contains the functions and utilities to parse an ARPA file with 19 variables.
"""
import csv
from datetime import datetime
from os.path import splitext, join

from sciafeed import this_path

MISSING_VALUE_MARKER = '32767'
PARAMETERS_FILEPATH = join(this_path, 'arpa19_params.csv')


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the arpa19 stored parameters.
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
    csv_reader = csv.DictReader(
        csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        position = int(row['position'])
        ret_value[position] = dict()
        for prop in row.keys():
            ret_value[position][prop] = row[prop].strip()
    return ret_value


def parse_filename(filename: str):
    """
    Return the tuple (<code of the station>, <start datetime object>, <end datetime object>)
    corresponding to the arpa19 input file named `filename`.
    The function assumes the filename is validated (see `validate_filename`).

    :param filename: the name of the arpa19 file
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
    Check the name of the input arpa19 file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the arpa19 file
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


def parse_row(row, parameters_map, only_valid=False, missing_value_marker=MISSING_VALUE_MARKER,
              code_for_lat='lat'):
    """
    Parse a row of a arpa19 file, and return the parsed data as a dictionary:
      {datetime object:
        { ....
          param_i_code: (param_i_value, flag),
          ...
        }
    The function assumes the row as validated. Flag is True (valid data) or False (not valid).

    :param row: a row of the arpa19 file
    :param parameters_map: dictionary containing information about stored parameters at each
        position
    :param only_valid: parse only values flagged as valid (default: False)
    :param missing_value_marker: the string used as a marker for missing value
    :param code_for_lat: code to be used for the latitude value
    :return: dictionary with key=time obj, and value=dict of the parameters values
    """
    tokens = row.split()
    assert len(tokens) == 40
    date_str = tokens[0]
    date_obj = datetime.strptime(date_str, '%Y%m%d%H%M')
    ret_value = {date_obj: dict()}
    ret_value[date_obj][code_for_lat] = float(tokens[1])
    par_values = tokens[2:21]
    par_flags = tokens[21:]
    for i, (param_i_value_str, flag_num) in enumerate(zip(par_values, par_flags)):
        param_i_code = parameters_map[i+1]['par_code']
        if param_i_value_str == missing_value_marker:
            param_i_value = None
        else:
            param_i_value = float(param_i_value_str)
        flag = float(flag_num) <= 1
        if not flag and only_valid:
            continue
        ret_value[date_obj][param_i_code] = (param_i_value, flag)
    return ret_value
