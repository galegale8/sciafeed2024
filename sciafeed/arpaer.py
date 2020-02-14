"""
This module contains the functions and utilities to parse an ARPA-Emilia Romagna file
"""
import csv
from datetime import datetime
from os.path import join
from pprint import pprint
import requests
import urllib.parse

from sciafeed import this_path

BASE_URL = "https://dati.arpae.it/dataset/" \
           "dati-dalle-stazioni-meteo-locali-della-rete-idrometeorologica-regionale/" \
           "resource/1396fb33-d6a1-442b-a1a1-90ff7a3d3642"
# from https://raw.githubusercontent.com/ARPA-SIMC/dballe/master/tables/dballe.txt
# header: [Code, Description, ?, ?, ?, ?, Unit, Scale, length in characters]
BTABLE_PATH = join(this_path, 'dballe.txt')
# from https://github.com/ARPA-SIMC/dballe/blob/v8.2-1/doc/fapi_ltypes.md
LTYPES_PATH = join(this_path, 'fapi_ltypes.md')
# from https://raw.githubusercontent.com/ARPA-SIMC/dballe/v8.2-1/doc/fapi_tranges.md
TRANGES_PATH = join(this_path, 'fapi_tranges.md')
PARAMETERS_FILEPATH = ''
LIMITING_PARAMETERS = ''
ONLY_BCODES = [
    'B11001',  # WIND DIRECTION
    'B11002',  # WIND SPEED
    'B12001',  # TEMPERATURE/AIR TEMPERATURE
    'B13003',  # RELATIVE HUMIDITY'
    'B10004',  # PRESSURE
    'B10051',  # PRESSURE REDUCED TO MEAN SEA LEVEL
    'B14021',  # GLOBAL SOLAR RADIATION, INTEGRATED OVER PERIOD SPECIFIED
    'B14031',  # TOTAL SUNSHINE
    'B13212',  # Leaf wetness duration
]


def load_btable(btable_path=BTABLE_PATH, only_bcodes=ONLY_BCODES):
    """
    Load the BTABLE (codes of ARPAER parameters) in a python dictionary
    of kind {code: dictionary of properties} where properties include
    description, unit, scale, char_lenght.

    :param btable_path: path of the BTABLE
    :param only_bcodes: if not None, only selected b-codes
    :return: the dictionary of the BTABLE
    """
    csv_file = open(btable_path, 'r')
    ret_value = dict()
    for row in csv_file:
        code = 'B' + row[2:7]
        if only_bcodes is not None and code not in only_bcodes:
            continue
        ret_value[code] = {
            'description': row[8:73].strip(),
            'unit': row[119:143].strip(),
            'format': row[143:146].strip(),
            'lenght': row[146:157].strip()
        }
    return ret_value


def build_sql(table, start=None, end=None, limit=None, **kwargs):
    """
    Build a SQL query for the ARPAER database.

    :param table: table name
    :param start: start datetime for field `date`
    :param end: end datetime for field `date`
    :param limit: number to limit the number of results
    :param kwargs: additional filters on the table's columns
    :return: the sql string
    """
    sql = """SELECT * FROM "%s" """ % table
    and_clauses = []
    if start:
        start_str = start.strftime('%Y-%m-%d %H:%M')
        and_clauses.append("date >= '%s'" % start_str)
    if end:
        end_str = end.strftime('%Y-%m-%d %H:%M')
        and_clauses.append("date <= '%s'" % end_str)
    for field, value in kwargs.items():
        and_clauses.append("%s  = '%s'" % (field, value))
    and_clauses_str = ' AND '.join(and_clauses)
    or_clauses = []
    for bcode in ONLY_BCODES:
        clause = "data::text LIKE '%" + bcode + "%'"
        or_clauses.append(clause)
    or_clauses_str = ' OR '.join(or_clauses)
    if len(or_clauses_str) > 1:
        or_clauses_str = '(%s)' % or_clauses_str
    clauses_str = ' AND '.join([and_clauses_str, or_clauses_str])
    if clauses_str:
        sql += " WHERE %s" % clauses_str
    if limit:
        sql += " limit %s" % limit
    return sql


def sql2results(sql):
    """
    Make a query to the ARPAER database
    :param sql: the sql string
    :return: the JSON results
    """
    query_url = 'https://arpae.datamb.it/api/action/datastore_search_sql'
    payload = {'sql': sql}
    print(sql)
    response = requests.get(query_url, params=payload)
    return response.json()['result']['records']


def get_json_data(start=None, end=None, timeout=60, limit=None, **kwargs):
    """
    Query the ARPAER database and return the JSON results.
    `kwargs` are additional filters on the querying table. The table has the following columns:
    - _id
    - date
    - data
    - network
    - lat
    - lon
    - ident
    - version
    - _full_text
    For the json output format, see:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json

    :param start: the datetime of the start time
    :param end: the datetime of the end time
    :param timeout: number of seconds to get timeout of the HTTP request
    :param limit: number to limit the number of results
    :param kwargs: additional filters on the table's columns
    :return: the JSON results
    """
    tokens = urllib.parse.urlsplit(BASE_URL).path.split('/')
    table = tokens[tokens.index('resource')+1]
    sql = build_sql(table, start, end, limit, **kwargs)
    results = sql2results(sql)
    return results


def parse_json_result(json, btable):
    """
    Return a dictionary of the data parsed from an input json.
    For the input json format, see:
    http://www.raspibo.org/wiki/index.php/Gruppo_Meteo/RFC-rmap#Json

    The output is the tuple (stat_props, date, measures), where:
    - stat_props is a dictionary of properties of the station
    - measures is a list of kind:
    [(paramer, level, trange, value, is_valid), ...]

    :param json: a JSON result object
    :param btable: the loaded BTABLE
    :return: the (stat_props, date, measures)
    """
    station_data = json['data'][0]['vars']
    measurement_groups = json['data'][1:]
    stat_props = dict()
    stat_props['station'] = station_data['B01019']['v']
    stat_props['lat'] = json['lat']
    stat_props['lon'] = json['lon']
    stat_props['network'] = json['network']
    stat_props['is_fixed'] = json['ident'] is None
    date = datetime.strptime(json['date'], '%Y-%m-%dT%H:%M:%S')
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


def get_data(start=None, end=None, timeout=60, limit=None, **kwargs):
    json_results = get_json_data(start, end, timeout, limit, **kwargs)
    btable = load_btable()
    for json_result in json_results:
        stat_props, date, measures = parse_json_result(json_result, btable)
        print('STATION PROPERTIES: ')
        pprint(stat_props)
        print('Date: %r' % date)
        pprint('MEASURES:')
        pprint(measures)
        print()


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





