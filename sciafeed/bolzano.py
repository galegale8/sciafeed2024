"""
This module contains functions and utilities to parse a BOLZANO file
"""
import csv
from datetime import datetime
from os.path import join, splitext

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
    name, ext = splitext(filepath)
    if ext.lower() != '.xls':
        err_msg = 'Extension expected must be .xls, found %s' % ext
        raise ValueError(err_msg)
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


def extract_metadata(filepath):
    """
    Extract station information and extra metadata from a file `filepath`
    of format bolzano.
    Return the list of dictionaries [stat_props, extra_metadata]

    :param filepath: path to the file to validate
    :return: [stat_props, extra_metadata]
    """
    stat_props = get_station_props(filepath)
    extra_metadata = dict()
    return [stat_props, extra_metadata]


def parse_row(row, parameters_map, stat_props=None):
    """
    Parse a row of a BOLZANO file, and return the parsed data. Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a list of values for each cell of the original Excel file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param stat_props: default stat_props if not provided in the row
    :return: (datetime object, prop_dict)
    """
    if stat_props is None:
        stat_props = dict()
    else:
        stat_props = stat_props.copy()
    # NOTE: assuming the column with the date is the second one
    date_obj = datetime.strptime(row[1].strip(), "%d.%m.%Y")
    data = []
    for col_indx, par_props in parameters_map.items():
        par_code = par_props['par_code']
        par_value = str(row[int(col_indx)-1]).strip().replace(',', '.')
        if par_value == '':
            par_value = None
        else:
            par_value = float(par_value)
        measure = [stat_props, date_obj, par_code, par_value, True]
        data.append(measure)
    return data


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
        cell = str(cell).strip().replace(',', '.')
        if cell != '':
            try:
                float(cell)
            except (TypeError, ValueError):
                err_msg = 'the row contains values not numeric'
                return err_msg
    return err_msg


def rows_generator(filepath, parameters_map, station_props, extra_metadata):
    # NOTE: assuming the column with the date is the second one
    date_column_indx = 1
    for i, row in enumerate(utils.load_excel(filepath), 1):
        try:
            datetime.strptime(row[date_column_indx], "%d.%m.%Y")
        except ValueError:
            continue
        yield i, row


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Parse a row of a BOLZANO file, and return the parsed data. Data structure is as a list:
    ::

      [(stat_props, datetime object, par_code, par_value, flag), ...]
      
    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the BOLZANO file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (station_code, data)
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    station_props, extra_metadata = extract_metadata(filepath)
    data = []
    for i, row in rows_generator(filepath, parameters_filepath, station_props, extra_metadata):
        row_data = parse_row(row, parameters_map, station_props)
        data.extend(row_data)
    return data


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
        stat_props, _ = extract_metadata(filepath)
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
    try:
        station = get_station_props(filepath)
        if not station:
            return False
    except:
        return False
    return True
