"""
This module contains functions and utilities to parse a BOLZANO file
"""
import csv
from datetime import datetime
from os.path import basename, join

from sciafeed import TEMPLATES_PATH
from sciafeed import utils

PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'bolzano_params.csv')
LIMITING_PARAMETERS = {}


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the BOLZANO stored parameters.
    Return a dictionary of type:
    ::

        {   i: dictionary of properties of parameter stored at the i-eth column (starting from 1),
        ...
        }

    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param delimiter: CSV delimiter
    :return: dictionary of positions with parameters information
    """
    csv_file = open(parameters_filepath, 'r')
    csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        csv_code = row['column']
        ret_value[csv_code] = dict()
        for prop in row.keys():
            ret_value[csv_code][prop] = row[prop].strip()
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the BOLZANO stored parameters.
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


def get_station_props(filepath):
    """
    Parse a BOLZANO file to guess some station properties.
    Station properties is a dictionary witk keys ['code', 'desc', 'utmx', 'utmy', height'].

    :param filepath: path to the input BOLZANO file
    :return: the list [station properties, column_index]
    """
    rows = utils.load_excel(filepath)
    stat_props = dict()
    # check only first 20 rows
    for i, row in enumerate(rows[:20]):
        for j, cell in enumerate(row):
            if 'stazione' in cell and not stat_props:
                stat_props['desc'] = row[j+1].strip()
                stat_props['code'] = rows[i+1][j+1].strip()
                stat_props['utmx'] = rows[i+2][j+1].strip()
                stat_props['utmy'] = rows[i+3][j+1].strip()
                stat_props['height'] = rows[i+4][j+1].strip()
                return stat_props
    raise ValueError('BOLZANO file not compliant')


def parse_row(row, parameters_map):
    """
    Parse a row of a BOLZANO file, and return the parsed data as a tuple of kind:
    ::

      (datetime object, prop_dict)

    where prop_dict is:
    ::

        { ....
          param_i_code: (param_i_value, flag),
          ...
        }

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a list of values for each cell of the original Excel file
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: (datetime object, prop_dict)
    """
    # NOTE: assuming the column with the date is the second one
    the_time = datetime.strptime(row[1].strip(), "%d.%m.%Y")
    prop_dict = dict()
    for col_indx, par_props in parameters_map.items():
        par_code = par_props['par_code']
        par_value = str(row[col_indx-1]).strip().replace(',', '.')
        if par_value == '':
            par_value = None
        else:
            par_value = float(par_value)
        prop_dict[par_code] = (par_value, True)
    return the_time, prop_dict


def validate_row_format(row):
    """
    It checks a row of a BOLZANO file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: a row dictionary of the BOLZANO file as parsed by csv.DictReader
    :return: the string describing the error
    """
    err_msg = ''
    try:
        # NOTE: assuming the date is always on the second column
        datetime.strptime(row[1], "%d.%m.%Y")
    except ValueError:
        err_msg = 'the date format is wrong'
        return err_msg
    for cell in row[2:]:
        cell = str(cell).strip().replace(',', '')
        if cell != '':
            try:
                float(cell)
            except (TypeError, ValueError):
                err_msg = 'the value %s is not numeric' % cell
                return err_msg
    return err_msg


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read a BOLZANO file located at `filepath` and returns the data stored inside. Value
    returned is a tuple (station_code, data) where data is a dictionary of type:
    :: 

        {   timeA: { par1_name: (par1_value,flag), ....},
            timeB: { par1_name: (par1_value,flag), ....},
            ...
        }

    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the BOLZANO file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    # NOTE: assuming the column with the date is the second one
    date_column_indx = 1
    station_code = get_station_props(filepath)['code']
    data = dict()
    for row in utils.load_excel(filepath):
        try:
            datetime.strptime(row[date_column_indx], "%d.%m.%Y")
        except ValueError:
            continue
        # now there is data
        the_time, prop_dict = parse_row(row, parameters_map)
        if prop_dict:
            data[the_time] = prop_dict
    return station_code, data


def export(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` of a BOLZANO file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the second output of the function `parse_and_check`.

    :param data: BOLZANO file data
    :param out_filepath: output file where to write the data
    :param omit_parameters: list of the parameters to omit
    :param omit_missing: if False, include also values marked as missing
    """
    fieldnames = ['station', 'latitude', 'date', 'parameter', 'value', 'valid']
    code, time_data = data
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for current_date in sorted(time_data):
            base_row = {
                'station': code,
                'latitude': '',
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
    Open a BOLZANO file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the BOLZANO file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    try:
        _ = get_station_props(filepath)
    except ValueError as err:
        return [(0, str(err))]
    parameters_map = load_parameter_file(parameters_filepath)
    rows = utils.load_excel(filepath)
    if not rows:
        return []
    j = 0
    for j, row in enumerate(rows):
        date_cell = [cell for cell in row if 'Data' in str(cell)]
        if date_cell:
            break
    found_errors = []
    last_time = None
    last_row = None
    for i, row in enumerate(rows[j+2:], j+3):
        err_msg = validate_row_format(row)
        if err_msg:
            found_errors.append((i, err_msg))
            continue
        cur_time, _ = parse_row(row, parameters_map)
        if last_time and cur_time == last_time and last_row != row:
            err_msg = 'the row is duplicated with different values'
            found_errors.append((i, err_msg))
            continue
        if last_time and cur_time < last_time:
            err_msg = 'the row is not strictly after the previous'
            found_errors.append((i, err_msg))
            continue
        last_time = cur_time
        last_row = row
    return found_errors


def row_weak_climatologic_check(parsed_row, parameters_thresholds=None):
    """
    Get the weak climatologic check for a parsed row of a BOLZANO file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param parsed_row: the row of a BOLZANO file
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    row_date, props = parsed_row
    err_msgs = []
    ret_props = props.copy()
    for par_code, (par_value, par_flag) in props.items():
        if par_code not in parameters_thresholds or not par_flag or par_value is None:
            # no check if limiting parameters are flagged invalid or the value is None
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            ret_props[par_code] = (par_value, False)
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append(err_msg)
    parsed_row_updated = (row_date, ret_props)
    return err_msgs, parsed_row_updated


def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for a BOLZANO file, i.e. it flags
    as invalid a value is out of a defined range.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the BOLZANO file
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
    code = get_station_props(filepath)['code']
    err_msgs = []
    data = dict()
    rows = utils.load_excel(filepath)
    if not rows:
        return [], None
    j = 0
    for j, row in enumerate(rows):
        date_cell = [cell for cell in row if 'Data' in str(cell)]
        if date_cell:
            break
    for i, row in enumerate(rows[j+2:], j+3):
        if i in fmt_errors_dict:
            continue
        parsed_row = parse_row(row, parameters_map=parameters_map)
        err_msgs_row, parsed_row = row_weak_climatologic_check(
            parsed_row, parameters_thresholds)
        for err_msg_row in err_msgs_row:
            err_msgs.append((i, err_msg_row))
        row_date, props = parsed_row
        data[row_date] = props
    ret_value = err_msgs, (code, data)
    return ret_value


def row_internal_consistence_check(parsed_row, limiting_params=None):
    """
    Get the internal consistent check for a parsed row of a BOLZANO file.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the parsed_row modified.
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param parsed_row: the row of a BOLZANO file
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if limiting_params is None:
        limiting_params = dict()
    row_date, props = parsed_row
    err_msgs = []
    ret_props = props.copy()
    for par_code, (par_value, par_flag) in props.items():
        if par_code not in limiting_params or not par_flag:
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
    return err_msgs, (row_date, ret_props)


def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    """
    Get the internal consistent check for a BOLZANO file.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the BOLZANO file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    fmt_errors = validate_format(filepath, parameters_filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    code = get_station_props(filepath)['code']
    parameters_map = load_parameter_file(parameters_filepath)
    err_msgs = []
    data = dict()
    rows = utils.load_excel(filepath)
    if not rows:
        return err_msgs, (code, data)
    j = 0
    for j, row in enumerate(rows):
        date_cell = [cell for cell in row if 'Data' in str(cell)]
        if date_cell:
            break
    for i, row in enumerate(rows[j+2:], j+3):
        if i in fmt_errors_dict:
            continue
        parsed_row = parse_row(row, parameters_map=parameters_map)
        err_msgs_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
        for err_msg_row in err_msgs_row:
            err_msgs.append((i, err_msg_row))
        row_date, props = parsed_row
        data[row_date] = props
    ret_value = err_msgs, (code, data)
    return ret_value


def parse_and_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                    limiting_params=LIMITING_PARAMETERS):
    """
    Read a BOLZANO file located at `filepath`, and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the BOLZANO file
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
        return err_msgs, (None, data)
    code = get_station_props(filepath)['code']
    rows = utils.load_excel(filepath)
    if not rows:
        return err_msgs, (code, data)
    j = 0
    for j, row in enumerate(rows):
        date_cell = [cell for cell in row if 'Data' in str(cell)]
        if date_cell:
            break
    for i, row in enumerate(rows[j+2:], j+3):
        if i in fmt_err_indexes_dict:
            continue
        parsed_row = parse_row(row, par_map)
        err_msgs1_row, parsed_row = row_weak_climatologic_check(parsed_row, par_thresholds)
        err_msgs2_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
        row_date, props = parsed_row
        data[row_date] = props
        err_msgs.extend([(i, err_msg1_row) for err_msg1_row in err_msgs1_row])
        err_msgs.extend([(i, err_msg2_row) for err_msg2_row in err_msgs2_row])
    ret_value = err_msgs, (code, data)
    return ret_value


def is_format_compliant(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: True if the file is compliant, False otherwise
    """
    try:
        station = get_station_props(filepath)
        if not station:
            return False
    except:
        return False
    return True
