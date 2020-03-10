"""
This module contains the functions and utilities to parse a RMN file
(Rete Mareografica Nazionale)
"""
import csv
from datetime import datetime
from os.path import join

from sciafeed import TEMPLATES_PATH

PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'rmn_params.csv')
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
        token = token.replace('À', 'A').replace('Ã\x80', 'A').replace('ï¿½', 'A')
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


def extract_metadata(filepath, parameters_filepath):
    """
    Extract station information and extra metadata from a file `filepath`
    of format RMN.
    Return the list of dictionaries [stat_props, extra_metadata]

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [stat_props, extra_metadata]
    """
    parameters_map = load_parameter_file(parameters_filepath)
    fieldnames, station = guess_fieldnames(filepath, parameters_map)
    stat_props = {'code': station}
    extra_metadata = {'fieldnames': fieldnames}
    return [stat_props, extra_metadata]


def parse_row(row, parameters_map, stat_props=None):
    """
    Parse a row of a arpa19 file, and return the parsed data. Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row dictionary of the rmn file as parsed by csv.DictReader
    :param parameters_map: dictionary of information about stored parameters at each position
    :param stat_props: default stat_props if not provided in the row
    :return: (datetime object, prop_dict)
    """
    if stat_props is None:
        stat_props = dict()
    else:
        stat_props = stat_props.copy()
    time_str = "%s %s" % (row['DATA'], row['ORA'])
    date_obj = datetime.strptime(time_str, "%Y%m%d %H:%M")
    data = []
    for par_indx in parameters_map:
        param_code = parameters_map[par_indx]['par_code']
        if param_code not in row:
            continue
        param_value = row[param_code].strip()
        if param_value in ('-', ''):
            param_value = None
        else:
            param_value = float(param_value.replace(',', '.'))
        measure = [stat_props, date_obj, param_code, param_value, True]
        data.append(measure)
    return data


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


def rows_generator(filepath, parameters_map, station_props, extra_metadata):
    fieldnames = extra_metadata['fieldnames']
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';', fieldnames=fieldnames)
    for i, row in enumerate(csv_reader, 1):
        if (row['DATA'], row['ORA']) != ('DATA', 'ORA'):
            continue
        break
    for j, row in enumerate(csv_reader, i+1):
        yield j, row


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read an arpa19 file located at `filepath` and returns the data stored inside. 
    Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]
      
    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the rmn file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    stat_props, extra_metadata = extract_metadata(filepath, parameters_filepath)
    data = []
    for i, row in rows_generator(filepath, parameters_map, stat_props, extra_metadata):
        parsed_row = parse_row(row, parameters_map, stat_props)
        data.extend(parsed_row)
    return data


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
    stat_props = {'code': station}
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
        fieldnames, station = guess_fieldnames(filepath, parameters_map)
        if not station:
            return False
    except:
        return False
    return True
