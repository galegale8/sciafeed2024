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


def extract_metadata(filepath):
    """
    Extract station information and extra metadata from a file `filepath`
    of format trentino.
    Return the list of dictionaries [stat_props, extra_metadata]

    :param filepath: path to the file to validate
    :return: [stat_props, extra_metadata]
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)
    if err_msg:
        raise ValueError(err_msg)
    code = parse_filename(filename)
    stat_props = {'code': code}
    extra_metadata = dict()
    return [stat_props, extra_metadata]


def parse_row(row, parameters_map, stat_props=None):
    """
    Parse a row of a trentino file, and return the parsed data. Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row dictionary of the trentino file as parsed by csv.DictReader
    :param parameters_map: dictionary of information about stored parameters at each position
    :param stat_props: default stat_props if not provided in the row
    :return: (datetime object, prop_dict)
    """
    if stat_props is None:
        stat_props = dict()
    else:
        stat_props = stat_props.copy()
    date_obj = datetime.strptime(row['date'].strip(), "%H:%M:%S %d/%m/%Y")
    all_parameters = [p['par_code'] for p in parameters_map.values()]
    param_code = list(set(row.keys()).intersection(all_parameters))[0]
    if row['quality'].strip() in ('151', '255') or row[param_code].strip() == '':
        param_value = None
    else:
        param_value = float(row[param_code].strip())
    is_valid = row['quality'].strip() in ('1', '76', '151', '255')
    measure = [stat_props, date_obj, param_code, param_value, is_valid]
    data = [measure]
    return data


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
        if key not in ('date', 'quality', None) and value != '':
            # key is the parameter
            try:
                float(value)
            except (TypeError, ValueError):
                err_msg = 'the value for %s is not numeric' % key
                return err_msg
    return err_msg


def rows_generator(filepath, parameters_map, station_props, extra_metadata):
    fieldnames, _, _ = guess_fieldnames(filepath, parameters_map)
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=fieldnames)
    for j, row in enumerate(csv_reader, 1):
        if (row['date'].strip(), row['quality'].strip()) != ('', 'Qual'):
            continue
        break
    for i, row in enumerate(csv_reader, j+1):
        row = {k.strip(): v.strip() for k, v in row.items() if k}
        yield i, row


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read a trentino file located at `filepath` and returns the data stored inside. 
    Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]
    
    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the trentino file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, lat, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    _, extra_metadata = extract_metadata(filepath)
    _, _, stat_props = guess_fieldnames(filepath, parameters_map)
    data = []
    for i, row in rows_generator(filepath, parameters_map, stat_props, extra_metadata):
        parsed_row = parse_row(row, parameters_map, stat_props)
        data.extend(parsed_row)
    return data


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
        row_measures = parse_row(row, parameters_map, stat_props=stat_props)
        if not row_measures:
            continue
        cur_time = row_measures[0][1]
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
