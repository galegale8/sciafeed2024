"""
This module contains the functions and utilities to parse an ARPA-Emilia Romagna file
"""
import csv
from datetime import datetime
import operator
from os.path import join
import json
import sys

import requests
from pprint import pprint

from sciafeed import this_path

# the name of the 'resource' table, where to find data: it can be taken from the link of
# of `dati recenti' from the URL
# `https://dati.arpae.it/dataset/dati-dalle-stazioni-meteo-locali-della-rete-idrometeorologica-regionale`
TABLE_NAME = "1396fb33-d6a1-442b-a1a1-90ff7a3d3642"

# taken from https://raw.githubusercontent.com/ARPA-SIMC/dballe/master/tables/dballe.txt
BTABLE_PATH = join(this_path, 'dballe.txt')

# taken from https://github.com/ARPA-SIMC/dballe/blob/v8.2-1/doc/fapi_ltypes.md
LTYPES_PATH = join(this_path, 'fapi_ltypes.md')

# taken from https://raw.githubusercontent.com/ARPA-SIMC/dballe/v8.2-1/doc/fapi_tranges.md
TRANGES_PATH = join(this_path, 'fapi_tranges.md')

DATASTORE_QUERY_URL = 'https://arpae.datamb.it/api/action/datastore_search_sql'
PARAMETERS_FILEPATH = join(this_path, 'arpaer_params.csv')
LIMITING_PARAMETERS = {}


# ##### start of online interface utilities #####


def build_sql(table_name, start=None, end=None, limit=None, only_bcodes=None, **kwargs):
    """
    Build a SQL query for the ARPAER database.
    Assume `table_name` has the following columns (can be used on `kwargs`):
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
    :return: the JSON results
    """
    payload = {'sql': sql}
    print(sql)
    response = requests.get(DATASTORE_QUERY_URL, params=payload, timeout=timeout).json()
    print(response)
    result = response.get('result', [])
    if response.get('success') and 'records' in result:
        return result['records']
    return []


def get_json_results(start=None, end=None, limit=None,
                     only_bcodes=None, timeout=None, **kwargs):
    """
    Query the ARPAER database and return the JSON results.

    For the json output format, see:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json

    :param start: the datetime of the start time
    :param end: the datetime of the end time
    :param limit: number to limit the number of results
    :param only_bcodes: if not None, select only records containing this list of BCODES
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param kwargs: additional filters on the table's columns
    :return: the JSON results
    """
    sql = build_sql(TABLE_NAME, start, end, limit, only_bcodes, **kwargs)
    results = sql2results(sql, timeout=timeout)
    return results


def save_json_results(filepath, json_results):
    """
    Save the json results from an ARPA-ER file located at `filepath` path.

    :param filepath: path of the ARPA-ER file
    :param json_results: list of JSON result objects to save
    """
    with open(filepath, 'w') as fp:
        for json_result in json_results:
            dumped = json.dumps(json_result)
            fp.write(dumped)


def load_json_results(filepath):
    """
    Load the json results from an ARPA-ER file located at `filepath` path.

    :param filepath: path of the ARPA-ER file
    :return: a list of JSON result objects
    """
    results = []
    with open(filepath) as fp:
        for line in fp:
            result = json.loads(line)
            results.append(result)
    return results


# entry point candidate
def query_and_save(save_path, parameters_filepath=PARAMETERS_FILEPATH,
                   start=None, end=None, timeout=None, limit=None, **kwargs):
    """
    Query the online datastore and save the results on `save_path`.

    :param save_path: path where to save the JSON results of the query
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param start: the datetime of the start time to select
    :param end: the datetime of the end time to select
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param limit: number to limit the number of results
    :param kwargs: additional filters on the table's columns
    """
    # TODO: verify if there's a way to check if data is recent or not, so where to download
    parameters_map = load_parameter_file(parameters_filepath)
    only_bcodes = list(parameters_map.keys())
    json_results = get_json_results(start, end, limit, only_bcodes, timeout, **kwargs)
    save_json_results(save_path, json_results)


# ##### end of online interface utilities #####


def load_btable(btable_path=BTABLE_PATH):
    """
    Load the BTABLE (codes of ARPAER parameters) in a python dictionary
    of kind {code: dictionary of properties} where properties include
    description, unit, scale, char_length.
    Assuming header of BTABLE is:
    [bcode, description, ?, ?, ?, ?, unit, scale, length]

    :param btable_path: path of the BTABLE
    :return: the dictionary of the BTABLE
    """
    ret_value = dict()
    with open(btable_path) as fp:
        for row in fp:
            code = 'B' + row[2:7]
            ret_value[code] = {
                'par_code': code,  # for similitude with parameters file
                'description': row[8:73].strip(),
                'unit': row[119:143].strip(),
                'format': row[143:146].strip(),
                'length': row[146:157].strip()
            }
    return ret_value


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the arpaer stored parameters.
    Return a dictionary of type:
    ::

        {   BCODE: dictionary of properties of parameter stored with the BCODE specified,
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
        bcode = row['BCODE']
        ret_value[bcode] = dict()
        for prop in row.keys():
            ret_value[bcode][prop] = row[prop].strip()
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the arpa21 stored parameters.
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


def validate_row_format(row):
    """
    It checks a row of an ARPA-ER file for validation against the format,
    and returns the description of the error (if found).
    Input row is a dumped JSON, the JSON format is described here:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: the ARPA-ER file row to validate
    :return: the string describing the error
    """
    err_msg = ''
    if not row.strip():  # no errors on empty rows
        return err_msg
    try:
        json_result = json.loads(row)
    except:
        err_msg = 'it does not seem a valid JSON'
        return err_msg
    # extracting station data
    try:
        station_data = json_result['data'][0]['vars']
        station_data['B01019']['v']
        json_result['lat']
        json_result['lon']
        json_result['network']
        json_result['ident']
    except KeyError:
        err_msg = 'information of the station is not parsable'
        return err_msg
    # date format
    try:
        datetime.strptime(json_result['date'], '%Y-%m-%dT%H:%M:%S')
    except ValueError:
        err_msg = 'information of the date is wrong'
        return err_msg
    # extracting measures
    try:
        measurement_groups = json_result['data'][1:]
        for measurement_group in measurement_groups:
            measurement_group['level'][0]
            measurement_group['timerange'][0]
            current_vars = measurement_group['vars']
            for bcode in current_vars:
                current_vars[bcode]['v']
    except KeyError:
        err_msg = 'information of the measurements is not parsable'
        return err_msg
    return err_msg


def parse_row(row, parameters_map):
    """
    Return a tuple of the data parsed from an input row of an ARPA-ER file.
    It assume that the row is validated with `validate_row_format`.
    The output is the tuple (stat_props, date, measures), where:
    - stat_props is a dictionary of properties of the station
    - date is the reference date for the measures
    - measures is a list of kind:
    [(parameter, level, trange, value, is_valid), ...]
    Only measures with parameters in the parameters_map are stored.

    :param row: a dumped JSON
    :param parameters_map: dictionary of information about stored parameters
    :return: the tuple (stat_props, date, measures)
    """
    json_result = json.loads(row)
    station_data = json_result['data'][0]['vars']
    measurement_groups = json_result['data'][1:]
    stat_props = dict()
    stat_props['station'] = station_data['B01019']['v']
    stat_props['lat'] = json_result['lat']
    stat_props['lon'] = json_result['lon']
    stat_props['network'] = json_result['network']
    stat_props['is_fixed'] = json_result['ident'] is None
    date = datetime.strptime(json_result['date'], '%Y-%m-%dT%H:%M:%S')
    measures = []
    for measurement_group in measurement_groups:
        group_level = measurement_group['level'][0]
        group_trange = measurement_group['timerange'][0]
        current_vars = measurement_group['vars']
        for bcode in current_vars:
            if bcode not in parameters_map:
                continue
            par_desc = parameters_map[bcode]['par_code']
            par_value = current_vars[bcode]['v']
            is_valid = 'B33196' not in current_vars[bcode].get('a', {})
            measure = (par_desc, group_level, group_trange, par_value, is_valid)
            measures.append(measure)
    return stat_props, date, measures


def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH, include_empty=False):
    """
    Load an input ARPA-ER file located at `filepath` and return the data stored inside.
    The function assumes the file as validated against the format (see function
    `validate_format`). No checks on data are performed.
    Data returned is a list of results of the function `parse_row`, i.e. a list of
    kind [(stat_props, date, measures), ...], where:
    - stat_props is a dictionary of properties of the station
    - date is the reference date for the measures
    - measures is a list of kind:
    [(parameter, level, trange, value, is_valid), ...]

    :param filepath: the input ARPA-ER file path
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param include_empty: if True, include empty measures (default False)
    :return: [(stat_props, date, measures), ...]
    """
    parameters_map = load_parameter_file(parameters_filepath)
    data = []
    with open(filepath) as fp:
        for row in fp:
            parsed_row = parse_row(row, parameters_map)
            if parsed_row[2] or include_empty:
                data.append(parsed_row)
    return data


def write_data(data, out_filepath, omit_parameters=()):
    """
    Write a CSV file with a representation of the downloaded data.
    Input `data` has the format as returned by the function `parse`.

    :param data: input data
    :param omit_parameters: list of the parameters to omit
    :param out_filepath: the output file path
    """
    fieldnames = ['station', 'latitude', 'longitude', 'network', 'date',
                  'parameter', 'level', 'trange', 'value', 'valid']
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        data.sort(key=operator.itemgetter(1))  # sort by dates
        for stat_props, current_date, measures in data:
            base_row = {
                'station': stat_props['station'],
                'latitude': stat_props['lat'],
                'longitude': stat_props['lon'],
                'network': stat_props['network'],
            }
            for parameter, level, trange, value, is_valid in measures:
                if parameter in omit_parameters:
                    continue
                row = base_row.copy()
                row['date'] = current_date
                row['parameter'] = parameter
                row['level'] = level
                row['trange'] = trange
                row['value'] = value
                row['valid'] = is_valid
                writer.writerow(row)


def validate_format(filepath):
    """
    Open an ARPA-ER file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the arpa-er file
    :return: [..., (row index, error message), ...]
    """
    ret_value = []
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            err_msg = validate_row_format(row)
            if err_msg:
                ret_value.append((i, err_msg))
    return ret_value


def row_weak_climatologic_check(parsed_row, parameters_thresholds=None):
    """
    Get the weak climatologic check for a parsed row of an arpa-er file, i.e. it flags
    as invalid a value if it is out of a defined range.
    It assumes that the parsed row is written as result of the function `parse_row`.
    Return the list of error messages, and the resulting data with flags updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param parsed_row: the parsed row of an arpa-er file
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_parsed)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    err_msgs = []
    measures = parsed_row[2]
    measures_updated = measures.copy()
    for measure in measures:
        par_code, level, trange, par_value, par_flag = measure
        if par_code not in parameters_thresholds or not par_flag or par_value is None:
            # no check if limiting parameters are flagged invalid or the value is None
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            measures_updated[4] = False
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append(err_msg)
    data_parsed_updated = (parsed_row[0], parsed_row[1], measures_updated)
    return err_msgs, data_parsed_updated


# entry point candidate
def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for arpa-er file located at `filepath`,
    i.e. it flags as invalid a value is out of a defined range.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the arpa-er file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: ([..., (row index info, err_msg), ...], data_parsed)
    """
    fmt_errors = validate_format(filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    err_msgs = []
    data_parsed = []
    parameters_map = load_parameter_file(parameters_filepath)
    parameters_thresholds = load_parameter_thresholds(parameters_filepath)
    # assuming in a row (i.e. date) we have all the station's measurements for that date
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_errors_dict:
                continue
            parsed_row = parse_row(row, parameters_map=parameters_map)
            err_msgs_row, parsed_row = row_weak_climatologic_check(
                parsed_row, parameters_thresholds)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            data_parsed.append(parsed_row)
    return err_msgs, data_parsed


# entry point candidate
def do_internal_consistence_check(filepath, limiting_params=None):
    """
    Get the internal consistent check for arpaer CSV data written using the function `write_data`.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the arpaer CSV file
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: ([..., (row index, err_msg), ...], data_updated)
    """
    fmt_errors = validate_format(filepath)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None

    # must load grouping by date and station
    # FIXME
    err_msgs = []
    data_updated = data.copy()
    for current_date in data_updated:
        props = data[current_date]
        for par_code, (par_value, par_flag, row_idx) in props.items():
            if par_code not in limiting_params or not par_flag:
                # no check if the parameter is floagged invalid or no in the limiting_params
                continue
            par_code_min, par_code_max = limiting_params[par_code]
            if par_code_min not in props or par_code_max not in props:
                # no check if no measurements for limiting parameters
                continue
            par_code_min_value, par_code_min_flag = props[par_code_min]
            par_code_max_value, par_code_max_flag = props[par_code_max]
            if not par_code_min_flag or not par_code_max_flag:
                # no check if limiting parameters are flagged invalid
                continue
            if not (par_code_min_value <= par_value <= par_code_max_value):
                data_updated[current_date][par_code] = (par_value, False)
                err_msg = "The values of %r, %r and %r are not consistent" \
                          % (par_code, par_code_min, par_code_max)
                err_msgs.append((row_idx, err_msg))

    ret_value = err_msgs, data_updated
    return ret_value


# FIXME
def parse_and_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                    limiting_params=LIMITING_PARAMETERS):
    """
    Read an for arpaer CSV data written using the function `write_data`,  located at `filepath`,
    and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the arpaer CSV file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_parsed)
    """
    par_map = load_parameter_file(parameters_filepath)
    par_thresholds = load_parameter_thresholds(parameters_filepath)
    err_msgs = []
    data = dict()
    fmt_err_msgs = validate_format(filepath)
    err_msgs.extend(fmt_err_msgs)
    fmt_err_indexes_dict = dict(fmt_err_msgs)
    if 0 in fmt_err_indexes_dict:
        # global error, no parsing
        return err_msgs, (None, None, data)
    icc_err_msgs, data_updated = do_internal_consistence_check(filepath, limiting_params)
    icc_err_msgs_dict = dict(icc_err_msgs)
    err_msgs.extend(icc_err_msgs)

    csv_file = open(filepath, 'r')
    csv_reader = csv.DictReader(csv_file, delimiter=';')
    for i, row in enumerate(csv_reader, 2):
        if i in fmt_err_indexes_dict or i in icc_err_msgs_dict:
            continue
        err_msgs1_row, row_updated = row_weak_climatologic_check(row, par_thresholds)
        row_date = datetime.strptime(row['date'], '%Y-%m-%d-%H:%M:%S')
        data_updated[row_date][row_updated['parameter']] = \
            (float(row_updated['value']), row_updated['valid'] == '1', i)
        err_msgs.extend([(i, err_msg1_row) for err_msg1_row in err_msgs1_row])
    ret_value = err_msgs, data_updated
    return ret_value


# entry point candidate
def make_report(in_filepath, out_filepath=None, outdata_filepath=None,
                parameters_filepath=PARAMETERS_FILEPATH, limiting_params=LIMITING_PARAMETERS):
    """
    Read an for arpaer CSV data written using the function `write_data`,  located at `in_filepath`
    and generate a report on the parsing.
    If `out_filepath` is defined, the report string is written on a file.
    If the path `outdata_filepath` is defined, a file with the data parsed is created at the path.
    Return the list of report strings and the data parsed.

    :param in_filepath: arpaer CSV input file
    :param out_filepath: path of the output report
    :param outdata_filepath: path of the output file containing data
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (report_strings, data_parsed)
    """
    msgs = []
    msg = "START OF ANALYSIS OF ARPA-ER FILE %r" % in_filepath
    msgs.append(msg)
    msgs.append('='*len(msg))
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
    msg = "END OF ANALYSIS OF ARPA-ER FILE"
    msgs.append(msg)
    msgs.append('='*len(msg))

    if out_filepath:
        with open(out_filepath, 'w') as fp:
            for msg in msgs:
                fp.write(msg + '\n')

    return msgs, data_parsed
