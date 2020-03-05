"""
This module contains the functions and utilities to parse a NOAA file
"""
import csv
from datetime import datetime
import operator
from os.path import join, splitext

from sciafeed import TEMPLATES_PATH

MISSING_VALUE_MARKERS = dict((
    ('TEMP', '9999.9'),
    ('DEWP', '9999.9'),
    ('SLP', '9999.9'),
    ('STP', '9999.9'),
    ('VISIB', '999.9'),
    ('WDSP', '999.9'),
    ('MXSPD', '999.9'),
    ('GUST', '999.9'),
    ('MAX', '9999.9'),
    ('MIN', '9999.9'),
    ('PRCP', '99.99'),
    ('SNDP', '999.9')
))
PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'noaa_params.csv')
LIMITING_PARAMETERS = {
    'Tmedia': ('Tmin', 'Tmax'),
}


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the NOAA stored parameters.
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
        code = row['NOAA_CODE']
        ret_value[code] = dict()
        for prop in row.keys():
            ret_value[code][prop] = row[prop].strip()
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the NOAA stored parameters.
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


def parse_row(row, parameters_map, missing_value_markers=MISSING_VALUE_MARKERS):
    """
    Parse a row of a NOAA file, and return the parsed data as a tuple of kind:
    ::

      (stat_props, datetime object, prop_dict)

    The output is the tuple (stat_props, date, measures), where:
    - stat_props is a dictionary of properties of the station
    - date is the reference date for the measures
    - prop_dict is a dictionary of kind:
    ::

        { ....
          param_i_code: (param_i_value, flag),
          ...
        }

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row of the NOAA file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param missing_value_markers: the map of the strings used as a marker for missing value
    :return: (datetime object, prop_dict)
    """
    date_str = row[14:22]
    stat_props = {
        'code': row[0:6].strip(),
        'wban': row[7:12].strip()
    }
    prop_dict_raw = {
        'TEMP': row[24:30],
        'DEWP': row[35:41],
        'SLP': row[46:52],
        'STP': row[57:63],
        'VISIB': row[68:73],
        'WDSP': row[78:83],
        'MXSPD': row[88:93],
        'GUST': row[95:100],
        'MAX': row[102:108],
        'MIN': row[110:116],
        'PRCP': row[118:123],
        'SNDP': row[125:130],
    }
    date_obj = datetime.strptime(date_str, '%Y%m%d')
    prop_dict = dict()
    for noaa_code, par_props in parameters_map.items():
        par_value_str = prop_dict_raw[noaa_code].strip()
        if par_value_str in (missing_value_markers.get(noaa_code), ''):
            par_value = None
        else:
            par_value = float(par_value_str.replace('*', ''))
        prop_dict[par_props['par_code']] = (par_value, True)
    ret_value = (stat_props, date_obj, prop_dict)
    return ret_value


def validate_row_format(row):
    """
    It checks a row of a NOAA file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: a row of the NOAA file
    :return: the string describing the error
    """
    err_msg = ''
    if not row.strip():  # no errors on empty rows
        return err_msg
    if len(row.strip()) != 138:
        err_msg = 'the length of the row is not standard'
        return err_msg
    try:
        date_str = row[14:22]
        _ = datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        err_msg = 'the reference time for the row is not parsable'
        return err_msg
    if row[123] not in ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', ' ']:
        err_msg = 'the precipitation flag is not parsable'
        return err_msg

    tokens = row.split()
    if len(tokens) != 22:
        err_msg = "The number of components in the row is wrong"
        return err_msg

    for token in (tokens[3:19] + tokens[20:]):
        try:
            float(token.replace('*', ''))
        except (ValueError, TypeError):
            err_msg = "The row contains not numeric values"
            return err_msg

    return err_msg


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH,
          missing_value_markers=MISSING_VALUE_MARKERS):
    """
    Read a NOAA file located at `filepath` and returns the data stored inside. 
    Data returned is a list of results of the function `parse_row`.
    
    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the NOAA file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param missing_value_markers: the map of the strings used as a marker for missing value
    :return: list of parsed rows
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    data = []
    with open(filepath) as fp:
        for _ in fp:
            break  # avoid first line!
        for row in fp:
            if not row.strip():
                continue
            parsed_row = parse_row(row, parameters_map,
                                   missing_value_markers=missing_value_markers)
            data.append(parsed_row)
    return data


def export(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` of a NOAA file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the output of the function `parse`.

    :param data: NOAA file data
    :param out_filepath: output file where to write the data
    :param omit_parameters: list of the parameters to omit
    :param omit_missing: if False, include also values marked as missing
    """
    fieldnames = ['station', 'latitude', 'date', 'parameter', 'value', 'valid']
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for data_item in sorted(data, key=operator.itemgetter(1)):  # sort by date
            stat_props, date_obj, prop_dict = data_item
            base_row = {
                'station': stat_props['code'],
                'latitude': '',
                'date': date_obj.isoformat(),
            }
            for parameter, (value, is_valid) in prop_dict.items():
                if parameter in omit_parameters:
                    continue
                if value is None and omit_missing:
                    continue
                row = base_row.copy()
                row['parameter'] = parameter
                row['value'] = value
                row['valid'] = is_valid and '1' or '0'
                writer.writerow(row)


def validate_format(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Open a NOAA file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the NOAA file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    HEADER = "STN--- WBAN   YEARMODA    TEMP       DEWP      SLP        STP       VISIB" \
             "      WDSP     MXSPD   GUST    MAX     MIN   PRCP   SNDP   FRSHTT"
    _, ext = splitext(filepath)
    if ext != '.op':
        return [(0, 'file extension must be .op')]
    found_errors = []
    parameters_map = load_parameter_file(parameters_filepath)
    with open(filepath) as fp:
        last_row_date = None
        last_row = None
        for i, row in enumerate(fp, 1):
            if i == 1 and row.strip() != HEADER:
                return [(0, "file doesn't include a correct header")]
            if not row.strip() or i == 1:
                continue
            err_msg = validate_row_format(row)
            if err_msg:
                found_errors.append((i, err_msg))
                continue
            stat_props, current_row_date, prop_dict = parse_row(row, parameters_map)
            if last_row_date and last_row_date > current_row_date:
                err_msg = "it is not strictly after the previous"
                found_errors.append((i, err_msg))
                continue
            if last_row and last_row_date and last_row_date == current_row_date and \
                    row != last_row:
                err_msg = "duplication of rows with different data"
                found_errors.append((i, err_msg))
                continue
            last_row_date = current_row_date
            last_row = row
    return found_errors


def row_weak_climatologic_check(parsed_row, parameters_thresholds=None):
    """
    Get the weak climatologic check for a parsed row of a NOAA file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param parsed_row: the row of a NOAA file
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    stat_props, date_obj, prop_dict = parsed_row
    err_msgs = []
    ret_props = prop_dict.copy()
    for par_code, (par_value, par_flag) in prop_dict.items():
        if par_code not in parameters_thresholds or not par_flag or par_value is None:
            # no check if limiting parameters are flagged invalid or value is None
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            ret_props[par_code] = (par_value, False)
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append(err_msg)
    parsed_row_updated = (stat_props, date_obj, ret_props)
    return err_msgs, parsed_row_updated


def row_internal_consistence_check(parsed_row, limiting_params=None):
    """
    Get the internal consistent check for a parsed row of a NOAA file.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the parsed_row modified.
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param parsed_row: the row of a NOAA file
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if limiting_params is None:
        limiting_params = dict()
    stat_props, date_obj, props = parsed_row
    err_msgs = []
    ret_props = props.copy()
    for par_code, (par_value, par_flag) in props.items():
        if par_code not in limiting_params or not par_flag or par_value is None:
            # no check if the parameter is floagged invalid or no in the limiting_params
            continue
        par_code_min, par_code_max = limiting_params[par_code]
        par_code_min_value, par_code_min_flag = props[par_code_min]
        par_code_max_value, par_code_max_flag = props[par_code_max]
        # check minimum
        if par_code_min_flag and par_code_min_value is not None:
            par_code_min_value = float(par_code_min_value)
            if par_value < par_code_min_value:
                ret_props[par_code] = (par_value, False)
                err_msg = "The values of %r and %r are not consistent" % (par_code, par_code_min)
                err_msgs.append(err_msg)
        # check maximum
        if par_code_max_flag and par_code_max_value is not None:
            par_code_max_value = float(par_code_max_value)
            if par_value > par_code_max_value:
                ret_props[par_code] = (par_value, False)
                err_msg = "The values of %r and %r are not consistent" % (par_code, par_code_max)
                err_msgs.append(err_msg)
    return err_msgs, (stat_props, date_obj, ret_props)


# entry point candidate
def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for a NOAA file, i.e. it flags
    as invalid a value is out of a defined range.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the NOAA file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    fmt_errors = validate_format(filepath, parameters_filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    parameters_map = load_parameter_file(parameters_filepath)
    parameters_thresholds = load_parameter_thresholds(parameters_filepath)
    err_msgs = []
    data = []
    with open(filepath) as fp:
        for _ in fp:
            break  # avoid first line!
        for i, row in enumerate(fp, 2):
            if not row.strip() or i in fmt_errors_dict:
                continue
            parsed_row = parse_row(row, parameters_map=parameters_map)
            err_msgs_row, parsed_row = row_weak_climatologic_check(
                parsed_row, parameters_thresholds)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            data.append(parsed_row)
    ret_value = err_msgs, data
    return ret_value


# entry point candidate
def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    """
    Get the internal consistent check for a NOAA file.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the NOAA file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    fmt_errors = validate_format(filepath, parameters_filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    parameters_map = load_parameter_file(parameters_filepath)
    err_msgs = []
    data = []
    with open(filepath) as fp:
        for _ in fp:
            break  # avoid first line!
        for i, row in enumerate(fp, 2):
            if not row.strip() or i in fmt_errors_dict:
                continue
            parsed_row = parse_row(row, parameters_map=parameters_map)
            err_msgs_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            data.append(parsed_row)
    ret_value = err_msgs, data
    return ret_value


def parse_and_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                    limiting_params=LIMITING_PARAMETERS):
    """
    Read a NOAA file located at `filepath`, and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the NOAA file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
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
    data = []
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_err_indexes_dict or i == 1:
                continue
            parsed_row = parse_row(row, par_map)
            err_msgs1_row, parsed_row = row_weak_climatologic_check(parsed_row, par_thresholds)
            err_msgs2_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
            data.append(parsed_row)
            err_msgs.extend([(i, err_msg1_row) for err_msg1_row in err_msgs1_row])
            err_msgs.extend([(i, err_msg2_row) for err_msg2_row in err_msgs2_row])
    ret_value = err_msgs, data
    return ret_value


def is_format_compliant(filepath):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :return: True if the file is compliant, False otherwise
    """
    header = "STN--- WBAN   YEARMODA    TEMP       DEWP      SLP        STP       VISIB" \
             "      WDSP     MXSPD   GUST    MAX     MIN   PRCP   SNDP   FRSHTT"
    _, ext = splitext(filepath)
    if ext != '.op':
        return False
    with open(filepath) as fp:
        first_row = fp.readline()
        if first_row.strip() != header:
            return False
    return True
