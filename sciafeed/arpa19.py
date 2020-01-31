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
    csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        position = int(row['position'])
        ret_value[position] = dict()
        for prop in row.keys():
            ret_value[position][prop] = row[prop].strip()
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the arpa19 stored parameters.
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

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row of the arpa19 file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param only_valid: parse only values flagged as valid (default: False)
    :param missing_value_marker: the string used as a marker for missing value
    :return: (datetime object, latitude, prop_dict)
    """
    tokens = row.split()
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


def validate_row_format(row):
    """
    It checks a row of an arpa19 file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: the arpa19 file row to validate
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

    # characters spacing
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
    return err_msg


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH, only_valid=False,
          missing_value_marker=MISSING_VALUE_MARKER):
    """
    Read an arpa19 file located at `filepath` and returns the data stored inside. Value
    returned is a tuple (station_code, station_latitude, data) where data is a dictionary of type:
    :: 
    
        {   timeA: { par1_name: (par1_value,flag), ....},
            timeB: { par1_name: (par1_value,flag), ....},
            ...
        }
    
    The function assumes the file as validated (see function `validate_format`).
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


def validate_format(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Open an arpa19 file and validate each row against the format.
    Return the list of error strings.
    This validation is needed to be able to parse the file with the function `parse`.

    :param filepath: path to the arpa19 file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: the list of strings describing the errors found
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)
    if err_msg:
        return [err_msg]

    found_errors = []
    code, start, end = parse_filename(filename)
    parameters_map = load_parameter_file(parameters_filepath)
    with open(filepath) as fp:
        last_lat = None
        last_row_date = None
        last_row = None
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            err_msg = validate_row_format(row)
            if err_msg:
                found_errors.append("Row %i: " % i + err_msg)
                continue
            current_row_date, current_lat, props = parse_row(row, parameters_map)
            if not start <= current_row_date <= end:
                err_msg = "Row %i: the time is not coherent with the filename" % i
                found_errors.append(err_msg)
                continue
            if last_lat and last_lat != current_lat:
                err_msg = "Row %i: the latitude changes" % i
                found_errors.append(err_msg)
                continue
            if last_row_date and last_row_date > current_row_date:
                err_msg = "Row %i: it is not strictly after the previous" % i
                found_errors.append(err_msg)
                continue
            if last_row and last_row_date and last_row_date == current_row_date and \
                    row != last_row:
                err_msg = "Row %i: duplication of rows with different data" % i
                found_errors.append(err_msg)
                continue
            last_lat = current_lat
            last_row_date = current_row_date
            last_row = row
    return found_errors


def row_weak_climatologic_check(row, parameters_map, parameters_thresholds=None):
    """
    Get the weak climatologic check for a row of an arpa19 file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the row is validated against the format (see `validate_row_format`).
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param row: the row of a arpa19 file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    row_date, lat, props = parse_row(row, parameters_map)
    err_msgs = []
    ret_props = props.copy()
    for par_code, (par_value, par_flag) in props.items():
        if par_code not in parameters_thresholds or not par_flag:
            # no check if limiting parameters are flagged invalid
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            ret_props[par_code] = (par_value, False)
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append(err_msg)
    parsed_row_updated = (row_date, lat, ret_props)
    return err_msgs, parsed_row_updated


def row_internal_consistence_check(row, parameters_map, limiting_params=None):
    """
    Get the internal consistent check for a row of a arpa19 file.
    It assumes that the row is parsable. Return the list of error
    messages, and the data parsed (eventually partially flagged as invalid).
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param row: the row of a arpa19 file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    row_date, lat, props = parse_row(row, parameters_map)
    err_msgs = []
    ret_props = props.copy()
    for par_code, (par_value, par_flag) in props.items():
        if par_code not in limiting_params or not par_flag:
            # no check if the parameter is floagged invalid or no in the limiting_params
            continue
        par_code_min, par_code_max = limiting_params[par_code]
        par_code_min_value, par_code_min_flag = props[par_code_min]
        par_code_max_value, par_code_max_flag = props[par_code_max]
        if not par_code_min_flag or not par_code_max_flag:
            # no check if limiting parameters are flagged invalid
            continue
        par_code_min_value, par_code_max_value = map(
                float, [par_code_min_value, par_code_max_value])
        if not (par_code_min_value <= par_value <= par_code_max_value):
            ret_props[par_code] = (par_value, False)
            err_msg = "The values of %r, %r and %r are not consistent" \
                      % (par_code, par_code_min, par_code_max)
            err_msgs.append(err_msg)
    return err_msgs, (row_date, lat, ret_props)


def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for an arpa19 file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the file is validated against the format (see `validate_format`).
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the arpa19 file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (err_msgs, data_parsed)
    """
    code, _, _ = parse_filename(basename(filepath))
    parameters_map = load_parameter_file(parameters_filepath)
    parameters_thresholds = load_parameter_thresholds(parameters_filepath)
    err_msgs = []
    data = dict()
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            err_msgs_row, parsed_row = row_weak_climatologic_check(
                row, parameters_map, parameters_thresholds)
            for err_msg_row in err_msgs_row:
                err_msgs.append("Row %s: %s" % (i, err_msg_row))
            row_date, lat, props = parsed_row
            data[row_date] = props
    ret_value = err_msgs, (code, lat, data)
    return ret_value


def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    """
    Get the internal consistent check for an arpa19 file.
    It assumes that the file is validated against the format (see `validate_format`).
    Return the list of error messages, and the resulting data with flags updated.

    :param filepath: path to the arpa19 file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    code, _, _ = parse_filename(basename(filepath))
    parameters_map = load_parameter_file(parameters_filepath)
    err_msgs = []
    data = dict()
    with open(filepath) as fp:
        for i, row in enumerate(fp):
            if not row.strip():
                continue
            err_msgs_row, parsed_row = row_internal_consistence_check(
                row, parameters_map, limiting_params)
            for err_msg_row in err_msgs_row:
                err_msgs.append("Row %s: %s" % (i, err_msg_row))
            row_date, lat, props = parsed_row
            data[row_date] = props
    ret_value = err_msgs, (code, lat, data)
    return ret_value
