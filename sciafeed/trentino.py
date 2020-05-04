"""
This module contains functions and utilities to parse a file with format used by region Trentino
"""
import csv
from datetime import datetime
from os.path import abspath, basename, dirname, join, splitext
from pathlib import PurePath

from sciafeed import TEMPLATES_PATH
from sciafeed import utils

PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'trentino_params.csv')
LIMITING_PARAMETERS = {}
FORMAT_LABEL = 'TRENTINO'


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the TRENTINO stored parameters.
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
        ret_value[csv_code]['convertion'] = utils.string2lambda(ret_value[csv_code]['convertion'])
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the TRENTINO stored parameters.
    Return a dictionary of type:
    ::

        { param_code: [min_value, max_value], ...}

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
    Return the code of the station corresponding to the TRENTINO input file named `filename`.
    The function assumes the filename is validated (see `validate_filename`).

    :param filename: the name of the TRENTINO file
    :return: the code of the station
    """
    code, ext = splitext(filename)
    return code


def validate_filename(filename: str):
    """
    Check the name of the input TRENTINO file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the TRENTINO file
    :return: the string describing the error
    """
    err_msg = ''
    name, ext = splitext(filename)
    if ext.lower() != '.csv':
        err_msg = 'Extension expected must be .csv, found %s' % ext
    return err_msg


def guess_fieldnames(filepath, parameters_map):
    """
    Parse a TRENTINO file to guess the right CSV header, the station code and some
    extra station properties.
    Station properties is a dictionary witk keys ['cod_utente', 'desc', 'lat', 'lon', height'].
    The measured parameters are taken from the parameters_map dictionary.

    :param filepath: path to the input TRENTINO file
    :param parameters_map: dictionary of information about stored parameters at each position
    :return: the tuple (list of fieldnames, station_code, extra_station_props)
    """
    station_code = None
    station_props_str = ''
    parameter = None
    err_msg = validate_filename(basename(filepath))
    if err_msg:
        raise ValueError(err_msg)
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
    if not station_code or not parameter or len(station_code) < 2:
        raise ValueError('trentino header not compliant')
    station_code = station_code[1:]
    fieldnames = ['date', parameter, 'quality']
    tokens = station_props_str.split()
    try:
        station_props = dict([
            ('cod_utente', station_code),
            ('lat', float([t for t in tokens if t.startswith('Lat:')][0][4:])),
            ('lon', float([t for t in tokens if t.startswith('Long:')][0][5:])),
            ('height', float([t for t in tokens if t.startswith('Elev:')][0][5:])),
        ])
        station_props['desc'] = ' '.join(
            tokens[tokens.index('-')+1:tokens.index('Lat:%s' % station_props['lat'])])
    except (IndexError, TypeError, ValueError):
        station_props = dict()
    if not station_code:
        raise ValueError('not found station name')
    return fieldnames, station_code, station_props


def parse_row(row, parameters_map, metadata=None):
    """
    Parse a row of a TRENTINO file, and return the parsed data. Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row dictionary of the TRENTINO file as parsed by csv.DictReader
    :param parameters_map: dictionary of information about stored parameters at each position
    :param metadata: default metadata if not provided in the row
    :return: (datetime object, prop_dict)
    """
    if metadata is None:
        metadata = dict()
    else:
        metadata = metadata.copy()
    date_obj = datetime.strptime(row['date'].strip(), "%H:%M:%S %d/%m/%Y").date()
    all_parameters = [p['par_code'] for p in parameters_map.values()]
    param_code = list(set(row.keys()).intersection(all_parameters))[0]
    props = [p for p in parameters_map.values() if p['par_code'] == param_code][0]
    if row['quality'].strip() in ('151', '255') or row[param_code].strip() == '':
        param_value = None
    else:
        param_value = props['convertion'](float(row[param_code].strip()))
    is_valid = row['quality'].strip() in ('1', '76', '151', '255')
    measure = (metadata, date_obj, param_code, param_value, is_valid)
    data = [measure]
    return data


def validate_row_format(row):
    """
    It checks a row of a TRENTINO file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: a row dictionary of the TRENTINO file as parsed by csv.DictReader
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


def rows_generator(filepath, parameters_map, metadata):
    """
    A generator of rows of a TRENTINO file containing data. Each value returned
    is a tuple (index of the row, row). row is a dictionary.

    :param filepath: the file path of the input file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param metadata: default metadata if not provided in the row
    :return: iterable of (index of the row, row)
    """
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=metadata['fieldnames'])
    j = 0
    for j, row in enumerate(csv_reader, 1):
        if (row['date'].strip(), row['quality'].strip()) != ('', 'Qual'):
            continue
        break
    for i, row in enumerate(csv_reader, j+1):
        row = {k.strip(): v.strip() for k, v in row.items() if k}
        yield i, row


# entry point candidate
def extract_metadata(filepath, parameters_filepath):
    """
    Extract generic metadata information from a file `filepath` of format TRENTINO.
    Return the dictionary of the metadata extracted.
    The function assumes the file is validated against the format (see function
    `guess_fieldnames`).

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: dictionary of metadata extracted
    """
    source = join(*PurePath(abspath(filepath)).parts[-2:])
    parameters_map = load_parameter_file(parameters_filepath)
    fieldnames, _, metadata = guess_fieldnames(filepath, parameters_map)
    metadata['fieldnames'] = fieldnames
    metadata['source'] = source
    metadata['format'] = FORMAT_LABEL
    folder_name = dirname(source)
    metadata.update(utils.folder2props(folder_name))
    return metadata


# entry point candidate
def validate_format(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Open a TRENTINO file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the TRENTINO file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    parameters_map = load_parameter_file(parameters_filepath)
    try:
        guess_fieldnames(filepath, parameters_map)
    except ValueError as err:
        return [(0, str(err))]
    metadata = extract_metadata(filepath, parameters_filepath)
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=',', fieldnames=metadata['fieldnames'])
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
        metadata['row'] = i
        row_measures = parse_row(row, parameters_map, metadata=metadata)
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


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read a TRENTINO file located at `filepath` and returns the data stored inside and the list
    of error messages eventually found. 
    Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]
    
    The list of error messages is returned as the function `validate_format` does.

    :param filepath: path to the TRENTINO file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (data, found_errors)
    """""
    data = []
    found_errors = validate_format(filepath, parameters_filepath)
    found_errors_dict = dict(found_errors)
    if 0 in found_errors_dict:
        return data, found_errors
    metadata = extract_metadata(filepath, parameters_filepath)
    parameters_map = load_parameter_file(parameters_filepath)
    data = []
    for i, row in rows_generator(filepath, parameters_map, metadata):
        if i in found_errors_dict:
            continue
        metadata['row'] = i
        parsed_row = parse_row(row, parameters_map, metadata)
        data.extend(parsed_row)
    return data, found_errors


# entry point candidate
def is_format_compliant(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: True if the file is compliant, False otherwise
    """
    parameters_map = load_parameter_file(parameters_filepath)
    try:
        guess_fieldnames(filepath, parameters_map)
    except:
        return False
    return True
