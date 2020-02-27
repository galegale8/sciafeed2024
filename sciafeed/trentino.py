"""
This module contains functions and utilities to parse a file with format used by region Trentino
"""
import csv
from datetime import datetime
from os.path import basename, join, splitext

from sciafeed import TEMPLATES_PATH

PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'trentino_params.csv')
LIMITING_PARAMETERS = {}


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the trentino stored parameters.
    Return a dictionary of type:
    ::

        {   CSV_CODE: dictionary of properties of parameter stored with the code CSV_CODE,
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
        csv_code = row['CSV_CODE']
        ret_value[csv_code] = dict()
        for prop in row.keys():
            ret_value[csv_code][prop] = row[prop].strip()
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the trentino stored parameters.
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
    Return the code of the station corresponding to the trentino input file named `filename`.
    The function assumes the filename is validated (see `validate_filename`).

    :param filename: the name of the trentino file
    :return: the code of the station
    """
    code, ext = splitext(filename)
    return code


def validate_filename(filename: str):
    """
    Check the name of the input trentino file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the trentino file
    :return: the string describing the error
    """
    err_msg = ''
    name, ext = splitext(filename)
    if ext.lower() != '.csv':
        err_msg = 'Extension expected must be .csv, found %s' % ext
    return err_msg


def guess_fieldnames(filepath, parameters_map):
    """
    Parse a trentino file to guess the right CSV header, the station code and some
    extra station properties.
    Station properties is a dictionary witk keys ['code', 'desc', 'lat', 'lon', height'].
    The measured parameters are taken from the parameters_map dictionary.

    :param filepath: path to the input trentino file
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: the tuple (list of fieldnames, station_code, extra_station_props)
    """
    station_code = None
    station_props_str = ''
    parameter = None
    with open(filepath, 'r', encoding='unicode_escape') as csv_file:
        for line in csv_file:
            line_tokens = [t.replace('"', '').replace("'", '').strip() for t in line.split(',')]
            line_tokens = [t for t in line_tokens if t]
            if len(line_tokens) < 2:
                continue
            elif len(line_tokens) > 3 and station_code and line_tokens[3].startswith(station_code):
                station_props_str = line_tokens[3]
                break
            first_col, second_col = line_tokens[:2]
            if first_col == 'Time':
                station_code = second_col
            elif first_col == 'and':
                parameter = parameters_map.get(second_col)['par_code']
    if not station_code or not parameter:
        raise ValueError('trentino header not compliant')
    fieldnames = ['date', parameter, 'quality']
    tokens = station_props_str.split()
    try:
        station_props = dict([
            ('code', station_code),
            ('lat', float([t for t in tokens if t.startswith('Lat:')][0][4:])),
            ('lon', float([t for t in tokens if t.startswith('Long:')][0][5:])),
            ('height', float([t for t in tokens if t.startswith('Elev:')][0][5:])),
        ])
        station_props['desc'] = ' '.join(
            tokens[tokens.index('-')+1:tokens.index('Lat:%s' % station_props['lat'])])
    except (IndexError, TypeError, ValueError):
        station_props = dict()
    return fieldnames, station_code, station_props


def parse_row(row, parameters_map):
    """
    Parse a row of a trentino file, and return the parsed data as a tuple of kind:
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

    :param row: a row dictionary of the trentino file as parsed by csv.DictReader
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: (datetime object, prop_dict)
    """
    the_time = datetime.strptime(row['date'].strip(), "%H:%M:%S %d/%m/%Y")
    prop_dict = dict()
    all_parameters = [p['par_code'] for p in parameters_map.values()]
    param_code = list(set(row.keys()).intersection(all_parameters))[0]
    if row['quality'].strip() in ('151', '255'):
        param_value = None
    else:
        param_value = float(row[param_code].strip())
    is_valid = row['quality'].strip() in ('1', '76', '151', '255')
    prop_dict[param_code] = (param_value, is_valid)
    return the_time, prop_dict


def validate_row_format(row):
    """
    It checks a row of a trentino file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: a row dictionary of the trentino file as parsed by csv.DictReader
    :return: the string describing the error
    """
    err_msg = ''
    try:
        datetime.strptime(row['date'], "%H:%M:%S %d/%m/%Y")
    except ValueError:
        err_msg = 'the date format is wrong'
        return err_msg
    for key, value in row.items():
        if (key, value) == ('quality', ''):
            err_msg = 'the value for quality is missing'
            return err_msg
        if key not in ('date', 'quality', None):
            # key is the parameter
            try:
                float(value)
            except (TypeError, ValueError):
                err_msg = 'the value for %s is not numeric' % key
                return err_msg
    return err_msg


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read a trentino file located at `filepath` and returns the data stored inside. Value
    returned is a tuple (station_code, lat, data) where data is a dictionary of type:
    :: 

        {   timeA: { par1_name: (par1_value,flag), ....},
            timeB: { par1_name: (par1_value,flag), ....},
            ...
        }

    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the trentino file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, lat, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    fieldnames, station, stat_props = guess_fieldnames(filepath, parameters_map)
    lat = stat_props.get('lat', '')
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=fieldnames)
    data = dict()
    for row in csv_reader:
        if (row['date'].strip(), row['quality'].strip()) != ('', 'Qual'):
            continue
        break
    for row in csv_reader:
        row = {k.strip(): v.strip() for k, v in row.items() if k}
        the_time, prop_dict = parse_row(row, parameters_map)
        if prop_dict:
            data[the_time] = prop_dict
    return station, lat, data


def write_data(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` of a trentino file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the output of the function `parse`.

    :param data: trentino file data
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
    Open a trentino file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the trentino file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)
    if err_msg:
        return [(0, err_msg)]
    parameters_map = load_parameter_file(parameters_filepath)
    try:
        fieldnames, station, stat_props = guess_fieldnames(filepath, parameters_map)
        if not station:
            raise ValueError('not found station name')
    except ValueError as err:
        return [(0, str(err))]

    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=fieldnames)
    j = 0
    for j, row in enumerate(csv_reader, 1):
        if (row['date'], row['quality']) != ('', 'Qual'):
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
    Get the weak climatologic check for a parsed row of a trentino file, i.e. it flags
    as invalid a value is out of a defined range.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param parsed_row: the row of a trentino file
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
    Get the weak climatologic check for a trentino file, i.e. it flags
    as invalid a value is out of a defined range.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the trentino file
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
    fieldnames, code, _ = guess_fieldnames(filepath, parameters_map)
    err_msgs = []
    data = dict()
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=fieldnames)
    j = 0
    for j, row in enumerate(csv_reader, 1):
        if (row['date'], row['quality']) != ('', 'Qual'):
            continue
        break
    for i, row in enumerate(csv_reader, j+1):
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


def row_internal_consistence_check(parsed_row, limiting_params=None): # pragma: no cover
    """
    Get the internal consistent check for a parsed row of a trentino file.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the parsed_row modified.
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param parsed_row: the row of a trentino file
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    err_msg = 'Each row has only a parameter: cannot do internal consistence check'
    raise NotImplementedError(err_msg)


def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):  # pragma: no cover
    """
    Get the internal consistent check for a trentino file.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the trentino file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    err_msg = 'Each row has only a parameter: cannot do internal consistence check'
    raise NotImplementedError(err_msg)


def parse_and_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                    limiting_params=LIMITING_PARAMETERS):
    """
    Read a trentino file located at `filepath`, and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the trentino file
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

    fieldnames, code, _ = guess_fieldnames(filepath, par_map)
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=fieldnames)
    j = 0
    for j, row in enumerate(csv_reader, 1):
        if (row['date'], row['quality']) != ('', 'Qual'):
            continue
        break
    for i, row in enumerate(csv_reader, j+1):
        if i in fmt_err_indexes_dict:
            continue
        parsed_row = parse_row(row, par_map)
        err_msgs1_row, parsed_row = row_weak_climatologic_check(parsed_row, par_thresholds)
        # err_msgs2_row, parsed_row = row_internal_consistence_check(parsed_row, limiting_params)
        row_date, props = parsed_row
        data[row_date] = props
        err_msgs.extend([(i, err_msg1_row) for err_msg1_row in err_msgs1_row])
        # err_msgs.extend([(i, err_msg2_row) for err_msg2_row in err_msgs2_row])
    ret_value = err_msgs, (code, data)
    return ret_value


def is_format_compliant(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: True if the file is compliant, False otherwise
    """
    parameters_map = load_parameter_file(parameters_filepath)
    try:
        fieldnames, station, _ = guess_fieldnames(filepath, parameters_map)
        if not station:
            return False
    except:
        return False
    return True
