"""
This module contains the functions and utilities to parse an ARPA-Emilia Romagna file
"""
import copy
import csv
from datetime import timedelta
import json
import os
from os.path import abspath, basename, dirname, join, splitext
from pathlib import PurePath

import requests

from sciafeed import TEMPLATES_PATH
from sciafeed import utils

JSON_ANY_MARKER = '999999'

# the name of the 'resource' table, where to find data: it can be taken from the link of
# of `dati recenti' from the URL
# `https://dati.arpae.it/dataset/dati-dalle-stazioni-meteo-locali-della-rete-idrometeorologica-regionale`
TABLE_NAME = "1396fb33-d6a1-442b-a1a1-90ff7a3d3642"

# # taken from https://raw.githubusercontent.com/ARPA-SIMC/dballe/master/tables/dballe.txt
# BTABLE_PATH = join(TEMPLATES_PATH, 'dballe.txt')
# # taken from https://github.com/ARPA-SIMC/dballe/blob/v8.2-1/doc/fapi_ltypes.md
# LTYPES_PATH = join(TEMPLATES_PATH, 'fapi_ltypes.md')
# # taken from https://raw.githubusercontent.com/ARPA-SIMC/dballe/v8.2-1/doc/fapi_tranges.md
# TRANGES_PATH = join(TEMPLATES_PATH, 'fapi_tranges.md')

DATASTORE_QUERY_URL = 'https://arpae.datamb.it/api/action/datastore_search_sql'
PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'arpaer_params.csv')
LIMITING_PARAMETERS = {}
FORMAT_LABEL = 'ARPA-ER'

# ##### start of online interface utilities #####


def build_sql(table_name, start=None, end=None, limit=None, only_bcodes=None, **kwargs):
    """
    Build a SQL query for the ARPAER database.
    Assume `table_name` has the following columns (can be used on `kwargs`):
    ::

    - _id
    - date
    - data
    - network
    - lat
    - lon
    - ident
    - version
    - _full_text

    :param table_name: resourse table name of the ARPAER database
    :param start: start datetime for field `date`
    :param end: end datetime for field `date`
    :param limit: optional number to limit the number of results
    :param only_bcodes: if not None, select only records containing this list of BCODES
    :param kwargs: additional filters on the table's columns
    :return: the sql string
    """
    sql = """SELECT * FROM "%s" """ % table_name

    and_clauses = []
    if start:
        start_str = start.strftime('%Y-%m-%d %H:%M')
        and_clauses.append("date >= '%s'" % start_str)
    if end:
        end_str = end.strftime('%Y-%m-%d %H:%M')
        and_clauses.append("date <= '%s'" % end_str)
    for field, value in kwargs.items():
        and_clauses.append("%s  = '%s'" % (field, value))

    or_clauses = []
    if only_bcodes:
        for bcode in only_bcodes:
            clause = "data::text LIKE '%" + bcode + "%'"
            or_clauses.append(clause)
        or_clauses_str = ' OR '.join(or_clauses)
        if len(or_clauses) > 1:
            or_clauses_str = '(%s)' % or_clauses_str
        if or_clauses_str:
            and_clauses.append(or_clauses_str)

    clauses_str = ' AND '.join(and_clauses)
    if clauses_str:
        sql += " WHERE %s" % clauses_str
    if limit:
        sql += " limit %s" % limit
    return sql


def sql2results(sql, timeout=None):
    """
    Make a query to the ARPAER database

    :param sql: the sql string
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :return: the list of dictionaries of the results
    """
    payload = {'sql': sql}
    print(sql)
    response = requests.get(DATASTORE_QUERY_URL, params=payload, timeout=timeout).json()
    print(response)
    result = response.get('result', [])
    if response.get('success') and 'records' in result:
        return result['records']
    return []


def get_db_results(start=None, end=None, limit=None, only_bcodes=None, bcodes_filters=None,
                   timeout=None, **kwargs):
    """
    Query the ARPAER database and return the results as a list of dictionaries.
    The dictionaries are loaded JSON (for the used json format, see
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json ).
    The bcode_filters are a dictionary of BCODES to filter, of kind:
    {bcode: {'trange': ..., 'level': ...}, ...}.
    The 'trange' and 'level' can be exact match, but you can use the 'any' value
    using the marker arpaer.JSON_ANY_MARKER .

    :param start: the datetime of the start time
    :param end: the datetime of the end time
    :param limit: number to limit the number of results
    :param only_bcodes: if not None, select only records containing this list of provided BCODES
    :param bcodes_filters: properties 'level' and 'trange' for some BCODES to be matched
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param kwargs: additional filters on the table's columns
    :return: list of dictionaries of results
    """
    sql = build_sql(TABLE_NAME, start, end, limit, only_bcodes, **kwargs)
    results = sql2results(sql, timeout=timeout)
    results_filtered = []
    # follow removing the results not including right BCODES, level, and trange
    for result in results:
        station_md = result['data'][0]
        data_results = []
        for data_item_original in result['data'][1:]:
            data_item = copy.deepcopy(data_item_original)
            data_level = data_item['level']
            data_timerange = data_item['timerange']
            for var in data_item['vars'].copy():
                if only_bcodes is not None and var not in only_bcodes:
                    del data_item['vars'][var]
                    continue
                if bcodes_filters and var in bcodes_filters:
                    props = bcodes_filters[var]
                    for prop in props:
                        bcode_level = prop['level']
                        bcode_trange = prop['trange']
                        if utils.is_same_list(bcode_level, data_level, JSON_ANY_MARKER) and \
                                utils.is_same_list(bcode_trange, data_timerange, JSON_ANY_MARKER):
                            break
                    else:  # never gone on break: not found the bcode in the properties
                        del data_item['vars'][var]
            if data_item['vars']:
                data_results.append(data_item)
        if data_results:
            data_results = [station_md] + data_results
            result_filtered = result.copy()
            result_filtered['data'] = data_results
            results_filtered.append(result_filtered)
    return results_filtered


def save_db_results(filepath, db_results):
    """
    Save the db results from an ARPA-ER file located at `filepath` path.
    The file format is saved according to ARPA-ER conventions.

    :param filepath: path of the ARPA-ER file
    :param db_results: list of DB result objects to save
    """
    with open(filepath, 'w') as fp:
        for db_result in db_results:
            dumped = json.dumps(db_result)
            fp.write(dumped + os.linesep)


def load_db_results(filepath):
    """
    Load the DB results from an ARPA-ER file located at `filepath` path.
    The file is assumed to follow the ARPA-ER format conventions.

    :param filepath: path of the ARPA-ER file
    :return: a list of dictionaries of the DB results
    """
    results = []
    with open(filepath) as fp:
        for line in fp:
            result = json.loads(line)
            results.append(result)
    return results


# entry point candidate
def query_and_save(save_path, parameters_filepath=PARAMETERS_FILEPATH, only_bcodes=None,
                   start=None, end=None, timeout=None, limit=None, **kwargs):
    """
    Query the online datastore and save the results on `save_path`.

    :param save_path: path where to save the JSON results of the query
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param only_bcodes: if not None, select only records containing this list of provided BCODES
    :param start: the datetime of the start time to select
    :param end: the datetime of the end time to select
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param limit: number to limit the number of results
    :param kwargs: additional filters on the table's columns
    """
    # TODO: verify if there's a way to check if data is recent or not, so where to download
    parameters_map = load_parameter_file(parameters_filepath)
    if not only_bcodes:
        only_bcodes = list(parameters_map.keys())
    bcodes_filters = {k: v for k, v in parameters_map.items() if k in only_bcodes}
    db_results = get_db_results(start, end, limit=limit, timeout=timeout, only_bcodes=only_bcodes,
                                bcodes_filters=bcodes_filters, **kwargs)
    save_db_results(save_path, db_results)


# ##### end of online interface utilities #####


# def load_btable(btable_path=BTABLE_PATH):
#     """
#     Load the BTABLE (codes of ARPAER parameters) in a python dictionary
#     of kind {code: dictionary of properties} where properties include
#     description, unit, scale, char_length.
#     Assuming header of BTABLE is:
#     [bcode, description, ?, ?, ?, ?, unit, scale, length]
#
#     :param btable_path: path of the BTABLE
#     :return: the dictionary of the BTABLE
#     """
#     ret_value = dict()
#     with open(btable_path) as fp:
#         for row in fp:
#             code = 'B' + row[2:7]
#             ret_value[code] = {
#                 'par_code': code,  # for similitude with parameters file
#                 'description': row[8:73].strip(),
#                 'unit': row[119:143].strip(),
#                 'format': row[143:146].strip(),
#                 'length': row[146:157].strip()
#             }
#     return ret_value


def validate_filename(filename: str):
    """
    Check the name of the input arpa-er file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the arpa-er file
    :return: the string describing the error
    """
    err_msg = ''
    name, ext = splitext(filename)
    if ext.lower() != '.json':
        err_msg = 'Extension expected must be .json, found %s' % ext
    return err_msg


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the ARPA-ER stored parameters.
    Return a dictionary of type:
    ::

        {   BCODE: [list of dictionary of properties of parameter stored with the BCODE specified]
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
        bcode, level, trange = row['BCODE-LEVEL-TRANGE'].split('-')
        ret_value[bcode] = ret_value.get(bcode, [])
        prop_item = dict()
        for prop in row.keys():
            prop_item[prop] = row[prop].strip()
        prop_item['level'] = json.loads(level.replace('x', JSON_ANY_MARKER))
        prop_item['trange'] = json.loads(trange.replace('x', JSON_ANY_MARKER))
        prop_item['convertion'] = utils.string2lambda(prop_item['convertion'])
        ret_value[bcode].append(prop_item)
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the ARPA-ER stored parameters.
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
    Extract generic metadata information from a file `filepath` of format ARPA-ER.
    Return the dictionary of the metadata extracted.
    The function assumes the file name is validated against the format (see function
    `validate_filename`).

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: dictionary of metadata extracted
    """
    source = join(*PurePath(abspath(filepath)).parts[-2:])
    ret_value = {'source': source, 'format': FORMAT_LABEL}
    folder_name = dirname(source)
    ret_value.update(utils.folder2props(folder_name))
    return ret_value


def validate_row_format(row):
    """
    It checks a row of an ARPA-ER file for validation against the format,
    and returns the description of the error (if found).
    Input row is a loaded JSON, the JSON format is described here:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: the ARPA-ER file row to validate
    :return: the string describing the error
    """
    err_msg = ''
    if row.__class__ != dict:
        err_msg = 'the row does not follow the standard'
        return err_msg
    # extracting station data
    try:
        station_data = row['data'][0]['vars']
        station_data['B01019']['v']
        row['lat']
        row['lon']
        row['network']
        row['ident']
    except KeyError:
        err_msg = 'information of the station is not parsable'
        return err_msg
    # date format: try 2 formats
    if not utils.parse_date(row['date'], ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S']):
        err_msg = 'information of the date is wrong'
        return err_msg
    # extracting measures
    try:
        for measurement_group in row['data'][1:]:
            for key in ['level', 'timerange', 'vars']:
                assert key in measurement_group
    except AssertionError:
        err_msg = 'information of the measurements is not parsable'
        return err_msg
    return err_msg


def parse_row(row, parameters_map, metadata=None):
    """
    Parse a row of a ARPA-ER file, and return the parsed data. Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).

    :param row: a dictionary of a DB result according to the format (loaded JSON)
    :param parameters_map: dictionary of information about stored parameters
    :param metadata: default metadata if not provided in the row
    :return: [(metadata, datetime object, par_code, par_value, flag), ...]
    """
    measures = []
    if metadata is None:
        metadata = dict()
    else:
        metadata = metadata.copy()
    station_data = row['data'][0]['vars']
    metadata['cod_utente'] = station_data['B01019']['v']
    metadata['lat'] = row['lat']
    metadata['lon'] = row['lon']
    metadata['network'] = row['network']
    metadata['is_fixed'] = row['ident'] is None
    patterns = ['%Y-%m-%dT%H:%M:%SZ', '%Y-%m-%dT%H:%M:%S']
    thedate = utils.parse_date(row['date'], patterns) - timedelta(hours=1)
    for measurement_group in row['data'][1:]:
        group_level = measurement_group['level']
        group_trange = measurement_group['timerange']
        current_vars = measurement_group['vars']
        for bcode in current_vars:
            if bcode not in parameters_map:
                continue
            for props in parameters_map[bcode]:
                par_code = props['par_code']
                par_level = props['level']
                par_trange = props['trange']
                if not utils.is_same_list(par_level, group_level, JSON_ANY_MARKER) or not \
                        utils.is_same_list(par_trange, group_trange, JSON_ANY_MARKER):
                    continue
                par_value = current_vars[bcode]['v']
                if par_value is not None:
                    par_value = props['convertion'](float(par_value))
                flag = 'B33196' not in current_vars[bcode].get('a', {})
                measure = (metadata, thedate, par_code, par_value, flag)
                measures.append(measure)
    return measures


def rows_generator(filepath, parameters_map, metadata):
    """
    A generator of rows of an arpa-er file containing data. Each value returned
    is a tuple (index of the row, row). `row` is a JSON-parsed dictionary.

    :param filepath: the file path of the input file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param metadata: default metadata if not provided in the row
    :return: iterable of (index of the row, row)
    """
    with open(filepath) as fp:
        for i, dumped_json in enumerate(fp, 1):
            if not dumped_json.strip():
                continue
            row = json.loads(dumped_json)
            yield i, row


# entry point candidate
def validate_format(filepath, parameters_filepath=None):
    """
    Open an ARPA-ER file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the arpa-er file
    :param parameters_filepath: not used at the moment (maintained for API compliance)
    :return: [..., (row index, error message), ...]
    """
    err_msg = validate_filename(basename(filepath))
    if err_msg:
        return [(0, err_msg)]
    ret_value = []
    with open(filepath) as fp:
        for i, dumped_json in enumerate(fp, 1):
            if not dumped_json.strip():
                continue
            try:
                row = json.loads(dumped_json)
            except:
                err_msg = 'the row is not a parsable JSON'
                ret_value.append((i, err_msg))
                continue
            err_msg = validate_row_format(row)
            if err_msg:
                ret_value.append((i, err_msg))
    return ret_value


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read an ARPA-ER file located at `filepath` and returns the data stored inside and the list
    of error messages eventually found.

    Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The list of error messages is returned as the function `validate_format` does.

    :param filepath: the input ARPA-ER file path
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (data, found_errors)
    """
    data = []
    found_errors = validate_format(filepath, parameters_filepath)
    found_errors_dict = dict(found_errors)
    if 0 in found_errors_dict:
        return data, found_errors
    metadata = extract_metadata(filepath, parameters_filepath)
    parameters_map = load_parameter_file(parameters_filepath)
    for i, row in rows_generator(filepath, parameters_map, metadata):
        if i in found_errors_dict:
            continue
        metadata['row'] = i
        parsed_row = parse_row(row, parameters_map, metadata)
        data.extend(parsed_row)
    return data, found_errors


# entry point candidate
def is_format_compliant(filepath):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :return: True if the file is compliant, False otherwise
    """
    if validate_filename(basename(filepath)):
        return False
    # read first line
    with open(filepath) as fp:
        try:
            line1 = fp.readline()
            result1 = json.loads(line1)
        except:
            return False
    return bool(result1)
