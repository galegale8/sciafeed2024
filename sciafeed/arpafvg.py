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


def extract_metadata(filepath, parameters_filepath):
    """
    Extract station information and extra metadata from a file `filepath`
    of format arpa-fvg.
    Return the list of dictionaries [stat_props, extra_metadata]

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [stat_props, extra_metadata]
    """
    filename = basename(filepath)
    err_msg = validate_filename(filename)
    if err_msg:
        raise ValueError(err_msg)
    code, start_obj, end_obj = parse_filename(filename)
    stat_props = {'cod_utente': code}
    extra_metadata = {'start_date': start_obj, 'end_date': end_obj}
    return [stat_props, extra_metadata]


def parse_row(row, parameters_map, stat_props=None):
    """
    Parse a row of a arpafvg file, and return the parsed data. Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).

    :param row: a row of the arpafvg file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param stat_props: default stat_props if not provided in the row
    :return: (datetime object, latitude, prop_dict)
    """
    if stat_props is None:
        stat_props = dict()
    else:
        stat_props = stat_props.copy()
    tokens = row.split()
    stat_props['lat'] = float(tokens[14])
    date_str = ''.join(tokens[:4])
    date_obj = datetime.strptime(date_str, '%y%m%d%H.%M')
    par_values = tokens[5:14]
    data = []
    for i, param_i_value_str in enumerate(par_values):
        param_i_code = parameters_map[i + 1]['par_code']
        param_i_value = float(param_i_value_str)
        measure = [stat_props, date_obj, param_i_code, param_i_value, True]
        data.append(measure)
    return data


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


def rows_generator(filepath, parameters_filepath, station_props, extra_metadata):
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            yield i, row


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read an arpafvg file located at `filepath` and returns the data stored inside. 
    Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]

    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the arpafvg file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, station_latitude, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    stat_props, extra_metadata = extract_metadata(filepath, parameters_filepath)
    data = []
    for i, row in rows_generator(filepath, parameters_map, stat_props, extra_metadata):
        parsed_row = parse_row(row, parameters_map, stat_props=stat_props)
        data.extend(parsed_row)
    return data


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
    stat_props = {'cod_utente': code}
    parameters_map = load_parameter_file(parameters_filepath)
    with open(filepath) as fp:
        last_row_date = None
        last_row = None
        official_lat = None
        for i, row in enumerate(fp, 1):
            if not row.strip():
                continue
            err_msg = validate_row_format(row)
            if err_msg:
                found_errors.append((i, err_msg))
                continue
            row_measures = parse_row(row, parameters_map, stat_props=stat_props)
            if not row_measures:
                continue
            current_row_date = row_measures[0][1]
            if not official_lat:
                # NOTE: assuming the official latitude is the one in the first row
                official_lat = row_measures[0][0].get('lat')
            current_row_lat = row_measures[0][0].get('lat')
            if last_row_date and last_row_date > current_row_date:
                err_msg = "it is not strictly after the previous"
                found_errors.append((i, err_msg))
            elif official_lat and official_lat != current_row_lat:
                err_msg = "the latitude changes"
                found_errors.append((i, err_msg))
            elif last_row and last_row_date and last_row_date == current_row_date and \
                    row != last_row:
                err_msg = "duplication of rows with different data"
                found_errors.append((i, err_msg))
            elif not start <= current_row_date <= end:
                err_msg = "the time is not coherent with the filename"
                found_errors.append((i, err_msg))
            last_row_date = current_row_date
            last_row = row
    return found_errors


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
