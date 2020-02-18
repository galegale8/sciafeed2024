"""
This module contains the functions and utilities to parse an ARPA-Emilia Romagna file
"""
import csv
from datetime import datetime
import json
from os.path import join
from pprint import pprint
import requests

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
PARAMETERS_FILEPATH = ''
LIMITING_PARAMETERS = ''
ONLY_BCODES = (
    'B11001',  # WIND DIRECTION
    'B11002',  # WIND SPEED
    'B12001',  # TEMPERATURE/AIR TEMPERATURE
    'B13003',  # RELATIVE HUMIDITY'
    'B10004',  # PRESSURE
    'B10051',  # PRESSURE REDUCED TO MEAN SEA LEVEL
    'B14021',  # GLOBAL SOLAR RADIATION, INTEGRATED OVER PERIOD SPECIFIED
    'B14031',  # TOTAL SUNSHINE
    'B13212',  # Leaf wetness duration
)


def load_btable(btable_path=BTABLE_PATH):
    """
    Load the BTABLE (codes of ARPAER parameters) in a python dictionary
    of kind {code: dictionary of properties} where properties include
    description, unit, scale, char_length.
    Assuming header of BTABLE is:
    [Code, Description, ?, ?, ?, ?, Unit, Scale, length in characters]

    :param btable_path: path of the BTABLE
    :return: the dictionary of the BTABLE
    """
    ret_value = dict()
    with open(btable_path) as fp:
        for row in fp:
            code = 'B' + row[2:7]
            ret_value[code] = {
                'description': row[8:73].strip(),
                'unit': row[119:143].strip(),
                'format': row[143:146].strip(),
                'length': row[146:157].strip()
            }
    return ret_value


def build_sql(table_name, start=None, end=None, limit=None, only_bcodes=(), **kwargs):
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
    :param only_bcodes: select only records containing this list of BCODES
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
                     only_bcodes=ONLY_BCODES, timeout=None, **kwargs):
    """
    Query the ARPAER database and return the JSON results.

    For the json output format, see:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json

    :param start: the datetime of the start time
    :param end: the datetime of the end time
    :param limit: number to limit the number of results
    :param only_bcodes: select only records containing this list of BCODES
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param kwargs: additional filters on the table's columns
    :return: the JSON results
    """
    sql = build_sql(TABLE_NAME, start, end, limit, only_bcodes, **kwargs)
    results = sql2results(sql, timeout=timeout)
    return results


def parse_json_result(json_result, btable):
    """
    Return a tuple of the data parsed from an input json.
    It assume that the json result is validated with `validate_json_result`.
    For the input json format, see:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json

    The output is the tuple (stat_props, date, measures), where:
    - stat_props is a dictionary of properties of the station
    - measures is a list of kind:
    [(paramer, level, trange, value, is_valid), ...]

    :param json_result: a JSON result object
    :param btable: the loaded BTABLE
    :return: the tuple (stat_props, date, measures)
    """
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
            if bcode not in btable:
                continue
            par_desc = btable[bcode]['description']
            par_value = current_vars[bcode]['v']
            is_valid = 'B33196' not in current_vars[bcode].get('a', {})
            measure = (par_desc, group_level, group_trange, par_value, is_valid)
            measures.append(measure)
    return stat_props, date, measures


# entry point candidate
def get_data(download_path=None, omit_parameters=(), start=None, end=None, timeout=None,
             limit=None, only_bcodes=ONLY_BCODES, btable_path=BTABLE_PATH, **kwargs):
    """
    Query the ARPA-ER datastore and returns the data stored inside. Value
    returned is a list of kind [(stat_props, dates_measures), ...], where:
    - stat_props is a dictionary of properties of the station
    - dates_measures is dictionary of kind:
    ::
        { ...
        dateA: [(parameter, level, trange, value, is_valid), ...],
        dateB: [(parameter, level, trange, value, is_valid), ...],
        ...}

    if the path `download_path` is not None, data is save there with the function `write_data`.

    :param download_path: path where to write the data
    :param omit_parameters: list of the parameters to omit in the download_path
    :param start: the datetime of the start time to select
    :param end: the datetime of the end time to select
    :param limit: number to limit the number of results
    :param only_bcodes: select only records containing this list of BCODES
    :param btable_path: path of the BTABLE
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param kwargs: additional filters on the table's columns
    :return: (stat_props, dates_measures)
    """
    json_results = get_json_results(start, end, limit, only_bcodes, timeout,  **kwargs)
    data = []
    added_stations = []
    btable = load_btable(btable_path)
    for json_result in json_results:
        stat_props, date, measures = parse_json_result(json_result, btable)
        if stat_props not in added_stations:
            added_stations.append(stat_props)
            data.append((stat_props, dict()))
        dates_measures = data[added_stations.index(stat_props)][1]
        date_measures = dates_measures.get(date, [])
        date_measures.extend(measures)
        dates_measures[date] = date_measures
    if download_path:
        write_data(data, download_path, omit_parameters)
    return data


def write_data(data, out_filepath, omit_parameters=()):
    """
    Write a CSV file with a representation of the downloaded data.
    Input `data` has the format as returned by the function `get_data`.

    :param data: input data
    :param omit_parameters: list of the parameters to omit
    :param out_filepath: the output file path
    """
    fieldnames = ['station', 'latitude', 'longitude', 'network', 'date',
                  'parameter', 'level', 'trange', 'value', 'valid']
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for stat_props, dates_measures in data:
            base_row = {
                'station': stat_props['station'],
                'latitude': stat_props['lat'],
                'longitude': stat_props['lon'],
                'network': stat_props['network'],
            }
            for current_date in sorted(dates_measures):
                row = base_row.copy()
                row['date'] = current_date.isoformat()
                for parameter, level, trange, value, valid in dates_measures[current_date]:
                    if parameter in omit_parameters:
                        continue
                    row['parameter'] = parameter
                    row['level'] = level
                    row['trange'] = trange
                    row['value'] = value
                    row['valid'] = valid and '1' or '0'
                    writer.writerow(row)


def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    """
    Get the internal consistent check for arpaer CSV data written using the function `write_data`.
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath:
    :param parameters_filepath:
    :param limiting_params:
    :return:
    """
    pass


def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Get the weak climatologic check for arpaer CSV data written using the function `write_data`,
    i.e. it flags as invalid a value is out of a defined range.
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the arpaer CSV file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    pass


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
    pass


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
    pass




