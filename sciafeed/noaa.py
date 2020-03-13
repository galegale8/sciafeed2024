"""
This module contains the functions and utilities to parse a NOAA file
"""
import csv
from datetime import datetime
from os.path import abspath, join, splitext
from pathlib import PurePath

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


def extract_metadata(filepath, parameters_filepath):
    """
    Extract generic metadata information from a file `filepath` of format NOAA.
    Return the dictionary of the metadata extracted.

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: dictionary of metadata extracted
    """
    metadata = {'source': join(*PurePath(abspath(filepath)).parts[-2:])}
    return metadata


def parse_row(row, parameters_map, missing_value_markers=MISSING_VALUE_MARKERS, metadata=None):
    """
    Parse a row of a NOAA file, and return the parsed data. Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row of the NOAA file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param missing_value_markers: the map of the strings used as a marker for missing value
    :param metadata: default metadata if not provided in the row
    :return: (datetime object, prop_dict)
    """
    date_str = row[14:22]
    if metadata is None:
        metadata = dict()
    else:
        metadata = metadata.copy()
    metadata['cod_utente'] = row[0:6].strip()
    metadata['wban'] = row[7:12].strip()
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
    data = []
    for noaa_code, par_props in parameters_map.items():
        par_code = par_props['par_code']
        par_value_str = prop_dict_raw[noaa_code].strip()
        if par_value_str in (missing_value_markers.get(noaa_code), ''):
            par_value = None
        else:
            par_value = float(par_value_str.replace('*', ''))
        measure = [metadata, date_obj, par_code, par_value, True]
        data.append(measure)
    return data


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


def rows_generator(filepath, parameters_map, metadata):
    with open(filepath) as fp:
        for _ in fp:
            break  # avoid first line!
        for i, row in enumerate(fp, 2):
            if not row.strip():
                continue
            yield i, row


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH,
          missing_value_markers=MISSING_VALUE_MARKERS):
    """
    Read a NOAA file located at `filepath` and returns the data stored inside.  
    Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]
    
    The function assumes the file as validated against the format (see function 
    `validate_format`). No checks on data are performed.

    :param filepath: path to the NOAA file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param missing_value_markers: the map of the strings used as a marker for missing value
    :return: [(metadata, datetime object, par_code, par_value, flag), ...]
    """""
    parameters_map = load_parameter_file(parameters_filepath)
    metadata = extract_metadata(filepath, parameters_filepath)
    data = []
    for i, row in rows_generator(filepath, parameters_map, metadata):
        parsed_row = parse_row(row, parameters_map,
                               missing_value_markers=missing_value_markers, metadata=metadata)
        data.extend(parsed_row)
    return data


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
            row_measures = parse_row(row, parameters_map)
            if not row_measures:
                continue
            current_row_date = row_measures[0][1]
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
