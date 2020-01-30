"""
This module contains the functions and utilities to parse an ARPA file with 19 variables.
"""
import csv
from datetime import datetime
from os.path import basename, join, splitext

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
    Return (<code of the station>, <start datetime object>, <end datetime object>)
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


def parse_row(row, parameters_map, only_valid=False, missing_value_marker=MISSING_VALUE_MARKER):
    """
    Parse a row of a arpa19 file, and return the parsed data as a tuple of kind:
    ::

      (datetime object, latitude, prop_dict)

    where prop_dict is:
    ::

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
    :return: (datetime object, latitude, prop_dict)
    """
    tokens = row.split()
    assert len(tokens) == 40
    date_str = tokens[0]
    date_obj = datetime.strptime(date_str, '%Y%m%d%H%M')
    ret_value1 = date_obj
    ret_value2 = float(tokens[1])
    ret_value3 = dict()
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
        ret_value3[param_i_code] = (param_i_value, flag)
    ret_value = (ret_value1, ret_value2, ret_value3)
    return ret_value


def validate_row(row, strict=False):
    """
    It checks a row of an arpa19 file for validation and returns the description
    string of the error (if found).

    :param row: the arpa19 file row to validate
    :param strict: if True, check also the length of the spaces in the row (default False)
    :return: the string describing the error
    """
    err_msg = ''
    if not row.strip():  # no errors on empty rows
        return err_msg
    tokens = row.split()
    if len(tokens) != 40:
        err_msg = "The number of components in the row %r is wrong" % row
        return err_msg
    try:
        datetime.strptime(tokens[0], '%Y%m%d%H%M')
    except ValueError:
        err_msg = "The date format in the row %r is wrong" % row
        return err_msg
    for value in tokens[1:]:
        try:
            float(value)
        except (ValueError, TypeError):
            err_msg = "The row %r contains not numeric values" % row
            return err_msg
    if not strict:
        return err_msg

    # strict check on spacing of characters
    if row[:12] != tokens[0]:
        err_msg = "The date length in the row %r is wrong" % row
        return err_msg
    if row[13:22] != tokens[1]:
        err_msg = "The latitude length in the row %r is wrong" % row
        return err_msg
    par_row = row[22:].rstrip('\n')
    for token_index, i in enumerate(range(0, len(par_row), 7)):
        if par_row[i:i+7].lstrip() != tokens[token_index+2]:
            err_msg = "The spacing in the row %r is wrong" % row
    return err_msg


def parse_arpa19(filepath, parameters_filepath=PARAMETERS_FILEPATH, only_valid=False,
                 missing_value_marker=MISSING_VALUE_MARKER):
    """
    Read an arpa19 file located at `filepath` and returns the data stored inside. Value
    returned is a tuple (station_code, station_latitude, data) where data is a dictionary of type:
    :: 
    
        {   timeA: { par1_name: (par1_value,flag), ....},
            timeB: { par1_name: (par1_value,flag), ....},
            ...
        }
        
    :param filepath: path to the arpa19 file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param only_valid: parse only values flagged as valid (default: False)
    :param missing_value_marker: the string used as a marker for missing value
    :return: (station_code, station_latitude, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    code, _, _ = parse_filename(basename(filepath))
    data = dict()
    with open(filepath) as fp:
        for row in fp:
            if not row.strip():
                continue
            row_date, lat, props = parse_row(
                row, parameters_map, only_valid=only_valid,
                missing_value_marker=missing_value_marker)
            data[row_date] = props
    ret_value = (code, lat, data)
    return ret_value


def validate_arpa19(filepath, strict=False):
    """
    Open an arpa19 file and check it. Return an error string if an error is found.

    :param filepath: path to the arpa19 file
    :param strict: if True, check also the length of the spaces in the row (default False)
    :return: the string describing the error
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)

    if err_msg:
        return err_msg
    code, start, end = parse_filename(filename)
    parameters_map = load_parameter_file()
    with open(filepath) as fp:
        last_lat = None
        last_row_date = None
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            err_msg = validate_row(row, strict=strict)
            if err_msg:
                return "Row %i: " % i + err_msg
            current_row_date, current_lat, props = parse_row(row, parameters_map)
            if not start <= current_row_date <= end:
                err_msg = "Row %i: the times are not coherent with the filename" % i
                return err_msg
            if last_lat and last_lat != current_lat:
                err_msg = "Row %i: the latitude changes" % i
                return err_msg
            if last_row_date and last_row_date > current_row_date:
                err_msg = "Row %i: it is not strictly after the previous" % i
                return err_msg
            last_lat = current_lat
            last_row_date = current_row_date
    return err_msg
