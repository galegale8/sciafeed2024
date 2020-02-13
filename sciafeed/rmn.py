"""
This module contains the functions and utilities to parse a RMN file
(Rete Mareografica Nazionale)
"""
import csv
from datetime import datetime
from os.path import join

from sciafeed import this_path

PARAMETERS_FILEPATH = join(this_path, 'rmn_params.csv')
LIMITING_PARAMETERS = {}


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the rmn stored parameters.
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
    csv_file = open(parameters_filepath, 'r', encoding='unicode_escape')
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
    Load a CSV file containing thresholds of the rmn stored parameters.
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


def guess_fieldnames(filepath, parameters_map):
    """
    Parse a rmn file to guess the right CSV header and the station name.
    The measured parameters are taken from the parameters_map dictionary.

    :param filepath: path to the input RMN file
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: the tuple (list of fieldnames, station)
    """
    fieldnames = []
    station = None
    with open(filepath, 'r', encoding='unicode_escape') as csv_file:
        for line in csv_file:
            if 'DATA' in line and 'ORA' in line:
                break
        else:  # never gone on break: not found header
            raise ValueError('RMN header not found')
    tokens = line.split(';')
    for token in tokens:
        token = token.replace('À', 'A').replace('Ã\x80', 'A')
        if token == 'DATA' or token == 'ORA':
            fieldnames.append(token)
            continue
        for par_indx, par_props in parameters_map.items():
            par_code = par_props['par_code']
            par_descr = par_props['description']
            if par_descr in token:
                fieldnames.append(par_code)
                station = token[:token.index(par_descr)].strip()
                break
        else:  # never gone on break: unknown header token
            raise ValueError('Unknown column on header: %r' % token)
    return fieldnames, station


def parse_row(row, parameters_map):
    """
    Parse a row of a rmn file, and return the parsed data as a tuple of kind:
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

    :param row: a row dictionary of the rmn file as parsed by csv.DictReader
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: (datetime object, prop_dict)
    """
    time_str = "%s %s" % (row['DATA'], row['ORA'])
    the_time = datetime.strptime(time_str, "%Y%m%d %H:%M")
    prop_dict = dict()
    for par_indx in parameters_map:
        param_code = parameters_map[par_indx]['par_code']
        try:
            prop_dict[param_code] = (float(row[param_code].replace(',', '.')), True)
        except (IndexError, ValueError):
            continue
    return the_time, prop_dict


def validate_row_format(row):
    """
    It checks a row of a rmn file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: a row dictionary of the rmn file as parsed by csv.DictReader
    :return: the string describing the error
    """
    err_msg = None
    try:
        time_str = "%s %s" % (row['DATA'], row['ORA'])
        datetime.strptime(time_str, "%Y%m%d %H:%M")
    except ValueError:
        err_msg = 'the reference time for the row is not parsable'
        return err_msg
    for param_code, param_value in row.items():
        if param_code in ['DATA', 'ORA']:
            continue
        if not param_value:
            continue
        try:
            float(param_value.replace(',', '.'))
        except ValueError:
            err_msg = 'the value %r is not numeric' % param_value
            return err_msg
    return err_msg


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read an rmn file located at `filepath` and returns the data stored inside. Value
    returned is a tuple (station_code, data) where data is a dictionary of type:
    :: 

        {   timeA: { par1_name: (par1_value,flag), ....},
            timeB: { par1_name: (par1_value,flag), ....},
            ...
        }

    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the rmn file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    fieldnames, station = guess_fieldnames(filepath, parameters_map)
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';', fieldnames=fieldnames)
    data = dict()
    for row in csv_reader:
        if (row['DATA'], row['ORA']) != ('DATA', 'ORA'):
            continue
        break
    for row in csv_reader:
        the_time, prop_dict = parse_row(row, parameters_map)
        if prop_dict:
            data[the_time] = prop_dict
    return station, data


def write_data(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` of an rmn file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the output of the function `parse`.

    :param data: rmn file data
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
    Open an rmn file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the rmn file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    parameters_map = load_parameter_file(parameters_filepath)
    try:
        fieldnames, station = guess_fieldnames(filepath, parameters_map)
        if not station:
            raise ValueError('not found station name')
    except ValueError as err:
        return [(0, str(err))]

    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';', fieldnames=fieldnames)
    j = 0
    for j, row in enumerate(csv_reader, 1):
        if (row['DATA'], row['ORA']) != ('DATA', 'ORA'):
            continue
        break
    found_errors = []
    last_time = None
    last_row = None
    for i, row in enumerate(csv_reader, j+1):
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
    Get the weak climatologic check for a parsed row of a RMN file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param parsed_row: the row of a arpa19 file
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    row_date, props = parsed_row
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
    parsed_row_updated = (row_date, ret_props)
    return err_msgs, parsed_row_updated


def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for an rmn file, i.e. it flags
    as invalid a value is out of a defined range.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the rmn file
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
    fieldnames, code = guess_fieldnames(filepath, parameters_map)
    err_msgs = []
    data = dict()
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';', fieldnames=fieldnames)
    for i, row in enumerate(csv_reader, 1):
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
    Get the internal consistent check for a parsed row of a arpa19 file.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the parsed_row modified.
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param parsed_row: the row of a arpa19 file
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
    return err_msgs, (row_date, ret_props)


def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    """
    Get the internal consistent check for an rmn file.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the rmn file
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
    fieldnames, code = guess_fieldnames(filepath, parameters_map)
    err_msgs = []
    data = dict()
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';', fieldnames=fieldnames)
    for i, row in enumerate(csv_reader, 1):
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
    Read an rmn file located at `filepath`, and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the rmn file
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

    fieldnames, code = guess_fieldnames(filepath, par_map)
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';', fieldnames=fieldnames)
    j = 0
    for j, row in enumerate(csv_reader, 1):
        if (row['DATA'], row['ORA']) != ('DATA', 'ORA'):
            continue
        break
    for i, row in enumerate(csv_reader, j+1):
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


def make_report(in_filepath, out_filepath=None, outdata_filepath=None,
                parameters_filepath=PARAMETERS_FILEPATH, limiting_params=LIMITING_PARAMETERS):
    """
    Read an rmn file located at `in_filepath` and generate a report on the parsing.
    If `out_filepath` is defined, the report string is written on a file.
    If the path `outdata_filepath` is defined, a file with the data parsed is created at the path.
    Return the list of report strings and the data parsed.

    :param in_filepath: rmn input file
    :param out_filepath: path of the output report
    :param outdata_filepath: path of the output file containing data
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (report_strings, data_parsed)
    """
    msgs = []
    msg = "START OF ANALYSIS OF rmn FILE %r" % in_filepath
    msgs.append(msg)
    msgs.append('=' * len(msg))
    msgs.append('')

    err_msgs, data_parsed = parse_and_check(
        in_filepath, parameters_filepath=parameters_filepath, limiting_params=limiting_params)
    if not err_msgs:
        msg = "No errors found"
        msgs.append(msg)
    else:
        for row_index, err_msg in err_msgs:
            msgs.append("Row %s: %s" % (row_index, err_msg))

    if outdata_filepath:
        msgs.append('')
        write_data(data_parsed, outdata_filepath)
        msg = "Data saved on file %r" % outdata_filepath
        msgs.append(msg)

    msgs.append('')
    msg = "END OF ANALYSIS OF rmn FILE"
    msgs.append(msg)
    msgs.append('=' * len(msg))

    if out_filepath:
        with open(out_filepath, 'w') as fp:
            for msg in msgs:
                fp.write(msg + '\n')

    return msgs, data_parsed
