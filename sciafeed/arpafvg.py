"""
This module contains the functions and utilities to parse an ARPA of Friuli-Venezia Giulia
"""
import csv
from datetime import datetime
from os.path import basename, join, splitext

from sciafeed import TEMPLATES_PATH

PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'arpafvg_params.csv')
LIMITING_PARAMETERS = dict()


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the arpafvg stored parameters.
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
    Load a CSV file containing thresholds of the arpafvg stored parameters.
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
    corresponding to the arpafvg input file named `filename`.
    The function assumes the filename is validated (see `validate_filename`).

    :param filename: the name of the arpafvg file
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
    start_obj = datetime.strptime(start_str, '%Y%m%d%H')
    end_obj = datetime.strptime(end_str, '%Y%m%d%H')
    assert start_obj <= end_obj
    ret_value = (code, start_obj, end_obj)
    return ret_value


def validate_filename(filename: str):
    """
    Check the name of the input arpafvg file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the arpafvg file
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
        start_obj = datetime.strptime(start_str, '%Y%m%d%H')
    except ValueError:
        err_msg = 'Start date in file name %r is not standard' % filename
        return err_msg
    try:
        end_obj = datetime.strptime(end_str, '%Y%m%d%H')
    except ValueError:
        err_msg = 'End date in file name %r is not standard' % filename
        return err_msg
    if start_obj > end_obj:
        err_msg = 'The time interval in file name %r is not valid' % filename
    return err_msg


def parse_row(row, parameters_map):
    """
    Parse a row of a arpafvg file, and return the parsed data as a tuple of kind:
    ::

      (datetime object, latitude, prop_dict)

    where prop_dict is:
    ::

        { ....
          param_i_code: (param_i_value, flag),
          ...
        }

    The function assumes the row as validated (see function `validate_row_format`).

    :param row: a row of the arpafvg file
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: (datetime object, latitude, prop_dict)
    """
    tokens = row.split()
    date_str = ''.join(tokens[:4])
    date_obj = datetime.strptime(date_str, '%y%m%d%H.%M')
    ret_value1 = date_obj
    ret_value2 = float(tokens[14])
    ret_value3 = dict()
    par_values = tokens[5:14]
    for i, param_i_value_str in enumerate(par_values):
        param_i_code = parameters_map[i + 1]['par_code']
        param_i_value = float(param_i_value_str)
        ret_value3[param_i_code] = (param_i_value, True)
    ret_value = (ret_value1, ret_value2, ret_value3)
    return ret_value


def validate_row_format(row):
    """
    It checks a row of an arpafvg file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: the arpafvg file row to validate
    :return: the string describing the error
    """
    err_msg = ''
    if not row.strip():  # no errors on empty rows
        return err_msg
    tokens = row.split()
    if len(tokens) != 15:
        err_msg = "The number of components in the row is wrong"
        return err_msg
    try:
        datetime.strptime(''.join(tokens[:4]), '%y%m%d%H.%M')
    except ValueError:
        err_msg = "The date format in the row is wrong"
        return err_msg
    for value in tokens[5:]:
        try:
            float(value)
        except (ValueError, TypeError):
            err_msg = "The row contains not numeric values"
            return err_msg

    return err_msg


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read an arpafvg file located at `filepath` and returns the data stored inside. Value
    returned is a tuple (station_code, station_latitude, data) where data is a dictionary of type:
    :: 

        {   timeA: { par1_name: (par1_value,flag), ....},
            timeB: { par1_name: (par1_value,flag), ....},
            ...
        }

    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the arpafvg file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, station_latitude, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    code, _, _ = parse_filename(basename(filepath))
    data = dict()
    with open(filepath) as fp:
        for row in fp:
            if not row.strip():
                continue
            row_date, lat, props = parse_row(row, parameters_map)
            data[row_date] = props
    ret_value = (code, lat, data)
    return ret_value


def write_data(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` of an arpafvg file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the output of the function `parse`.

    :param data: arpafvg file data
    :param out_filepath: output file where to write the data
    :param omit_parameters: list of the parameters to omit
    :param omit_missing: if False, include also values marked as missing
    """
    fieldnames = ['station', 'latitude', 'date', 'parameter', 'value', 'valid']
    code, lat, time_data = data
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for current_date in sorted(time_data):
            base_row = {
                'station': code,
                'latitude': lat,
                'date': current_date.isoformat()
            }
            current_data = time_data[current_date]
            for parameter in current_data:
                if parameter in omit_parameters:
                    continue
                row = base_row.copy()
                row['value'], row['valid'] = current_data[parameter]
                if omit_missing and row['value'] is None:
                    continue
                row['parameter'] = parameter
                row['valid'] = row['valid'] and '1' or '0'
                writer.writerow(row)


def validate_format(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Open an arpafvg file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the arpafvg file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)
    if err_msg:
        return [(0, err_msg)]
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
                found_errors.append((i, err_msg))
                continue
            current_row_date, current_lat, props = parse_row(row, parameters_map)
            if not start <= current_row_date <= end:
                err_msg = "the time is not coherent with the filename"
                found_errors.append((i, err_msg))
                continue
            if last_lat and last_lat != current_lat:
                err_msg = "the latitude changes"
                found_errors.append((i, err_msg))
                continue
            if last_row_date and last_row_date > current_row_date:
                err_msg = "it is not strictly after the previous"
                found_errors.append((i, err_msg))
                continue
            if last_row and last_row_date and last_row_date == current_row_date and \
                    row != last_row:
                err_msg = "duplication of rows with different data"
                found_errors.append((i, err_msg))
                continue
            last_lat = current_lat
            last_row_date = current_row_date
            last_row = row
    return found_errors


def row_weak_climatologic_check(parsed_row, parameters_thresholds=None):
    """
    Get the weak climatologic check for a parsed row of an arpafvg file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param parsed_row: the row of a arpafvg file
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    row_date, lat, props = parsed_row
    err_msgs = []
    ret_props = props.copy()
    for par_code, (par_value, par_flag) in props.items():
        if par_code not in parameters_thresholds or not par_flag or par_value is None:
            # no check if limiting parameters are flagged invalid or value is None
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            ret_props[par_code] = (par_value, False)
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append(err_msg)
    parsed_row_updated = (row_date, lat, ret_props)
    return err_msgs, parsed_row_updated


def row_internal_consistence_check(parsed_row, limiting_params=None):
    """
    Get the internal consistent check for a parsed row of a arpafvg file.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the parsed_row modified.
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param parsed_row: the row of a arpafvg file
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if limiting_params is None:
        limiting_params = dict()
    row_date, lat, props = parsed_row
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


# entry point candidate
def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for an arpafvg file, i.e. it flags
    as invalid a value is out of a defined range.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the arpafvg file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    fmt_errors = validate_format(filepath, parameters_filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    code, _, _ = parse_filename(basename(filepath))
    parameters_map = load_parameter_file(parameters_filepath)
    parameters_thresholds = load_parameter_thresholds(parameters_filepath)
    err_msgs = []
    data = dict()
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_errors_dict:
                continue
            parsed_row = parse_row(row, parameters_map=parameters_map)
            err_msgs_row, parsed_row = row_weak_climatologic_check(
                parsed_row, parameters_thresholds)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            row_date, lat, props = parsed_row
            data[row_date] = props
    ret_value = err_msgs, (code, lat, data)
    return ret_value


# entry point candidate
def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    """
    Get the internal consistent check for an arpafvg file.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the arpafvg file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    fmt_errors = validate_format(filepath, parameters_filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    if limiting_params is None:
        limiting_params = dict()
    code, _, _ = parse_filename(basename(filepath))
    parameters_map = load_parameter_file(parameters_filepath)
    err_msgs = []
    data = dict()
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_errors_dict:
                continue
            parsed_row = parse_row(row, parameters_map)
            err_msgs_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            row_date, lat, props = parsed_row
            data[row_date] = props
    ret_value = err_msgs, (code, lat, data)
    return ret_value


def parse_and_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                    limiting_params=LIMITING_PARAMETERS):
    """
    Read an arpafvg file located at `filepath`, and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the arpafvg file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    filename = basename(filepath)
    par_map = load_parameter_file(parameters_filepath)
    par_thresholds = load_parameter_thresholds(parameters_filepath)
    err_msgs = []
    data = dict()
    fmt_err_msgs = validate_format(filepath, parameters_filepath)
    err_msgs.extend(fmt_err_msgs)
    fmt_err_indexes_dict = dict(fmt_err_msgs)
    if 0 in fmt_err_indexes_dict:
        # global error, no parsing
        return err_msgs, (None, None, data)

    code, _, _ = parse_filename(filename)
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_err_indexes_dict:
                continue
            parsed_row = parse_row(row, par_map)
            err_msgs1_row, parsed_row = row_weak_climatologic_check(parsed_row, par_thresholds)
            err_msgs2_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
            row_date, lat, props = parsed_row
            data[row_date] = props
            err_msgs.extend([(i, err_msg1_row) for err_msg1_row in err_msgs1_row])
            err_msgs.extend([(i, err_msg2_row) for err_msg2_row in err_msgs2_row])
    ret_value = err_msgs, (code, lat, data)
    return ret_value


def is_format_compliant(filepath):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :return: True if the file is compliant, False otherwise
    """
    filename = basename(filepath)
    if validate_filename(filename):
        return False
    # check first 2 rows
    with open(filepath) as fp:
        row1 = fp.readline()
        row2 = fp.readline()
        if len(row1.split()) != 15 and len(row2.split()) != 15:
            return False
    return True
