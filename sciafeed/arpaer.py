"""
This module contains the functions and utilities to parse an ARPA-Emilia Romagna file
"""
from datetime import datetime
from os.path import join
from pprint import pprint
import requests
import urllib.parse

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


def get_json_data(start=None, end=None, limit=None, only_bcodes=ONLY_BCODES,
                  timeout=None, **kwargs):
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
    Return a dictionary of the data parsed from an input json.
    For the input json format, see:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json

    The output is the tuple (stat_props, date, measures), where:
    - stat_props is a dictionary of properties of the station
    - measures is a list of kind:
    [(paramer, level, trange, value, is_valid), ...]

    :param json_result: a JSON result object
    :param btable: the loaded BTABLE
    :return: the (stat_props, date, measures)
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


def download_data(start=None, end=None, timeout=None, limit=None, only_bcodes=ONLY_BCODES,
                  btable_path=BTABLE_PATH, **kwargs):
    """
    Query the ARPA-ER datastore and returns the data stored inside. Value
    returned is a list of kind [(stat_props, dates_measures), ...], where:
    - stat_props is a dictionary of properties of the station
    - dates_measures is dictionary of kind:
    ::
        { ...
        dateA: [(paramer, level, trange, value, is_valid), ...],
        dateB: [(paramer, level, trange, value, is_valid), ...],
        ...}

    :param start: the datetime of the start time to select
    :param end: the datetime of the end time to select
    :param limit: number to limit the number of results
    :param only_bcodes: select only records containing this list of BCODES
    :param btable_path: path of the BTABLE
    :param timeout: number of seconds to wait for a server feedback (None=wait forever)
    :param kwargs: additional filters on the table's columns
    :return: (stat_props, dates_measures)
    """
    json_results = get_json_data(start, end, limit, only_bcodes, timeout,  **kwargs)
    ret_value = []
    added_stations = []
    btable = load_btable(btable_path)
    for json_result in json_results:
        stat_props, date, measures = parse_json_result(json_result, btable)
        if stat_props not in added_stations:
            added_stations.append(stat_props)
            ret_value.append((stat_props, dict()))
        dates_measures = ret_value[added_stations.index(stat_props)][1]
        date_measures = dates_measures.get(date, [])
        date_measures.append(measures)
        dates_measures[date] = date_measures
    return ret_value


def write_data(data, output_path, omit_parameters=()):
    """
    Write a CSV file with a representation of the parsed data

    :param data:
    :param omit_parameters:
    :param output_path: the output file path
    """
    pass


def do_internal_consistence_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                                  limiting_params=None):
    pass


def do_weak_climatologic_check(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    pass


def parse_and_check(filepath, parameters_filepath=PARAMETERS_FILEPATH,
                    limiting_params=LIMITING_PARAMETERS):
    pass


def make_report(in_filepath, out_filepath=None, outdata_filepath=None,
                parameters_filepath=PARAMETERS_FILEPATH, limiting_params=LIMITING_PARAMETERS):
    pass





