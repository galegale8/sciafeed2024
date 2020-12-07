"""
This module contains functions and utilities that extracts information from SCIA `database`.
"""
import functools
import itertools
import operator
from os import listdir
from os.path import isfile, join, splitext

from sqlalchemy.sql import select, and_, literal, func
from sqlalchemy import MetaData, Table

from sciafeed import db_utils
from sciafeed import export


@functools.lru_cache(maxsize=None)
def get_db_station(conn, anag_table, **kwargs):
    """
    Get a station object from the database, by its properties

    :param conn: db connection object
    :param anag_table: table object of the stations
    :param kwargs: dictionary of column values
    :return: the station object (if found), otherwise None
    """
    clause = literal(True)
    for col_name, col_value in kwargs.items():
        column = getattr(anag_table.c, col_name)
        if col_value is not None:
            if col_name == 'nome':
                # clause in this case is case insensitive
                clause = and_(clause, func.lower(column) == col_value.lower())
            # elif col_name in ('lat', 'lon'):
            #     # clause in this case is with precision 4
            #     down, top = float(col_value)-10**-4, float(col_value)+10**-4
            #     clause = and_(clause, column.between(down, top))
            else:
                clause = and_(clause, column == col_value)
    query = select([anag_table]).where(clause)
    results = conn.execute(query).fetchall()
    if results and len(results) == 1:
        return results[0]
    return None


def find_new_stations(data_folder, dburi):
    """
    Find stations from a set of CSV files inside a folder `data_folder`, that are not
    present in the database and creates a CSV with the list.
    Return the not found stations as a dictionary (key is ('cod_utente', 'cod_rete') )

    :param data_folder: folder path where CSV files are in
    :param dburi: db connection URI
    :return: ([list of report messages], {dict of not found stations})
    """
    msgs = []
    conn = db_utils.ensure_connection(dburi)
    meta = MetaData()
    anag_table = Table('anag__stazioni', meta, autoload=True, autoload_with=conn.engine,
                       schema='dailypdbadmclima')
    all_stations = dict()
    new_stations = dict()
    num_records = 0
    with conn.begin():
        total_to_see = listdir(data_folder)
        total_to_see_number = len(total_to_see)
        for i, file_name in enumerate(total_to_see):
            csv_path = join(data_folder, file_name)
            msg = 'examine file %s/%s...' % (i+1, total_to_see_number)
            print(msg)
            msgs.append(msg)
            if not isfile(csv_path) or splitext(file_name.lower())[1] != '.csv':
                continue
            records = export.csv2data(csv_path)
            for record in records:
                num_records += 1
                record_md = record[0]
                station_key = (record_md['cod_utente'], record_md['cod_rete'],
                               record_md['lat'], record_md['lon'])
                if station_key in all_stations:
                    continue
                station_props = {
                    'cod_rete': record_md['cod_rete']
                }
                if record_md['format'] not in ('ARPA-ER', 'RMN'):
                    station_props['cod_utente'] = record_md['cod_utente']
                else:  # workaround to manage Emilia Romagna/RMN: try to find by name
                    station_props['nome'] = record_md['cod_utente']
                    if record_md['lat']:
                        station_props['lat'] = record_md['lat']
                    if record_md['lon']:
                        station_props['lon'] = record_md['lon']
                db_station = get_db_station(conn, anag_table, **station_props)
                all_stations[station_key] = db_station
                if not db_station:  # NOT FOUND in the database
                    new_station = station_props
                    new_station['source'] = record_md['source']
                    new_station['lat'] = record_md['lat']
                    new_station['lon'] = record_md['lon']
                    new_stations[station_key] = new_station
                    msg = " - new station: cod_utente=%r, cod_rete=%r, lat=%s, lon=%s" \
                          % station_key
                    print(msg)
                    msgs.append(msg)
    num_all_stations = len(all_stations)
    num_new_stations = len(new_stations)
    msg0 = "Examined %i records" % num_records
    msg1 = "Found %i distinct stations" % num_all_stations
    msg2 = "Number of NEW stations: %i" % num_new_stations
    for msg in [msg0, msg1, msg2]:
        print(msg)
    msgs.extend([msg0, msg1, msg2])
    conn.close()
    return msgs, new_stations


def get_stations_by_where(dburi, station_where=None):
    """
    Return a list of id_staz from a 'where' string condition.

    :param dburi: postgresql connection URI
    :param station_where: a where SQL condition
    :return: the list of id_staz
    """
    if not station_where or not station_where.strip():
        where_sql = ''
    else:
        where_sql = "WHERE %s" % station_where
    assert ';' not in where_sql
    assert 'insert' not in where_sql.lower()
    assert 'update' not in where_sql.lower()
    assert '"' not in where_sql
    conn = db_utils.ensure_connection(dburi)
    sql = "SELECT id_staz FROM dailypdbadmclima.anag__stazioni %s" % where_sql
    results = [r[0] for r in conn.execute(sql).fetchall()]
    conn.close()
    return results


def load_main_station_groups(conn, group_table_name, schema="dailypdbanpacarica"):
    """
    Return a dictionary of kind {group_id: id_staz} where id_staz is the main reference station
    of the group.

    :param conn: db connection object
    :param group_table_name: table name with the station equivalences
    :param schema: db schema of the table
    :return: dictionary of main station for each group
    """
    ret_value = dict()
    sql = "SELECT idgruppo, id_staz FROM %s.%s ORDER BY idgruppo, progstazione" \
          % (schema, group_table_name)
    results = conn.execute(sql)

    group_by_idgruppo = operator.itemgetter(0)
    for group_id, group_stations in itertools.groupby(results, group_by_idgruppo):
        ret_value[group_id] = list(group_stations)[0][1]
    return ret_value


def select_prec_records(conn, sql_fields='*', stations_ids=None, schema='dailypdbanpacarica',
                        include_flag_values=None, exclude_flag_interval=None, exclude_values=(),
                        exclude_null=True, no_order=False):
    """
    Select all the records of the table ds__preci order by station, date.
    If  stations_ids is not None, filter also for station ids.
    By default, only not NULL values and flagged as valid are queried.

    :param conn: db connection object
    :param sql_fields: sql string of field selection
    :param stations_ids: list of station ids (if None: no filter by station)
    :param schema: database schema to consider
    :param include_flag_values: if not None, select where prec24.flag belongs to this values
    :param exclude_flag_interval: if not None, exclude where prec24.flag belongs to this interval
    :param exclude_values: query excludes not none values in this iterable
    :param exclude_null: if True, excludes NULL values
    :param no_order: if False, sort results by station, date
    :return: the iterable of the results
    """
    sql = "SELECT %s FROM %s.ds__preci" % (sql_fields, schema)
    where_clauses = []
    if stations_ids:
        where_clauses.append('cod_staz IN (%s)' % repr(list(stations_ids))[1:-1])
    if include_flag_values is not None:
        include_sql_str = repr(tuple(include_flag_values))
        if len(include_flag_values) == 1:
            include_sql_str = '(%s)' % include_flag_values[0]
        where_clauses.append('((prec24).flag).wht in %s' % include_sql_str)
    if exclude_flag_interval is not None:
        where_clauses.append('((((prec24).flag).wht < %s) OR (((prec24).flag).wht > %s))'
                             % (exclude_flag_interval[0], exclude_flag_interval[1]))
    if exclude_values:
        where_clauses.append('(prec24).val_tot NOT IN (%s)' % repr(list(exclude_values))[1:-1])
    if exclude_null:
        where_clauses.append('(prec24).val_tot IS NOT NULL')
    if where_clauses:
        sql += ' WHERE %s' % (' AND '.join(where_clauses))
    if not no_order:
        sql += ' ORDER BY cod_staz, data_i'
    # each record must be a list to make flag changeable:
    results = conn.execute(sql)
    results = db_utils.results_list(results)
    return results


def select_temp_records(conn, fields, sql_fields='*', stations_ids=None,
                        schema='dailypdbanpacarica', include_flag_values=None,
                        exclude_flag_interval=None, exclude_values=(),
                        exclude_null=True, where_sql=None, no_order=False):
    """
    Select all the records of the table ds__t200 order by station, date.
    If  stations_ids is not None, filter also for station ids.
    By default, only not NULL values and flagged as valid are queried, but additional
    parameters can change the filtering.

    :param conn: db connection object
    :param fields: list of field names, i.e. 'tmxgg' or 'tmngg', where to apply filters
    :param sql_fields: sql string of field selection
    :param stations_ids: list of station ids (if None: no filter by station)
    :param schema: database schema to consider
    :param include_flag_values: if not None, select fields where the flag belongs to this values
    :param exclude_flag_interval: if not None, exclude fields where flag belongs to this interval
    :param exclude_values: query excludes not none values in this iterable
    :param exclude_null: if True, excludes NULL values
    :param where_sql: if not None, add the selected where clause in sql
    :param no_order: if True, sort results by station, date
    :return: the iterable of the results
    """
    sql = "SELECT %s FROM %s.ds__t200" % (sql_fields, schema)
    where_clauses = []
    if stations_ids:
        where_clauses.append('cod_staz IN (%s)' % repr(list(stations_ids))[1:-1])
    if include_flag_values is not None:
        include_sql_str = repr(tuple(include_flag_values))
        if len(include_flag_values) == 1:
            include_sql_str = '(%s)' % include_flag_values[0]
        for field in fields:
            where_clauses.append('((%s).flag).wht in %s' % (field, include_sql_str))
    if exclude_flag_interval is not None:
        for field in fields:
            clause = '((((%s).flag).wht < %s) OR (((%s).flag).wht > %s))' \
                     % (field, exclude_flag_interval[0], field, exclude_flag_interval[1])
            where_clauses.append(clause)
    if exclude_values:
        for field in fields:
            where_clauses.append('(%s).val_md NOT IN (%s)'
                                 % (field, repr(list(exclude_values))[1:-1]))
    if exclude_null:
        for field in fields:
            where_clauses.append('(%s).val_md IS NOT NULL' % field)
    if where_sql:
        where_clauses.append(where_sql)
    if where_clauses:
        sql += ' WHERE %s' % (' AND '.join(where_clauses))
    if not no_order:
        sql += ' ORDER BY cod_staz, data_i'
    results = conn.execute(sql)
    # each record must be a list to make flag changeable:
    results = db_utils.results_list(results)
    return results


def select_records(conn, table, fields, sql_fields='*', stations_ids=None,
                   schema='dailypdbanpacarica', include_flag_values=None,
                   exclude_flag_interval=None, where_sql=None, no_order=False):
    """
    Select all the records of the table `table` order by station, date.
    If  stations_ids is not None, filter also for station ids.
    By default, only not NULL values and flagged as valid are queried, but additional
    parameters can change the filtering.

    :param conn: db connection object
    :param table: table name where to select
    :param fields: list of field names, i.e. 'tmxgg' or 'tmngg', where to apply filters on flags
    :param sql_fields: sql string of field selection
    :param stations_ids: list of station ids (if None: no filter by station)
    :param schema: database schema to consider
    :param include_flag_values: if not None, select fields where the flag belongs to this values
    :param exclude_flag_interval: if not None, exclude fields where flag belongs to this interval
    :param exclude_null: if True, excludes NULL values
    :param where_sql: if not None, add the selected where clause in sql
    :param no_order: if True, sort results by station, date
    :return: the iterable of the results
    """
    sql = "SELECT %s FROM %s.%s" % (sql_fields, schema, table)
    where_clauses = []
    if stations_ids:
        where_clauses.append('cod_staz IN (%s)' % repr(list(stations_ids))[1:-1])
    if include_flag_values is not None:
        include_sql_str = repr(tuple(include_flag_values))
        if len(include_flag_values) == 1:
            include_sql_str = '(%s)' % include_flag_values[0]
        for field in fields:
            where_clauses.append('((%s).flag).wht in %s' % (field, include_sql_str))
    if exclude_flag_interval is not None:
        for field in fields:
            clause = '((((%s).flag).wht < %s) OR (((%s).flag).wht > %s))' \
                     % (field, exclude_flag_interval[0], field, exclude_flag_interval[1])
            where_clauses.append(clause)
    if where_sql:
        where_clauses.append(where_sql)
    if where_clauses:
        sql += ' WHERE %s' % (' AND '.join(where_clauses))
    if not no_order:
        sql += ' ORDER BY cod_staz, data_i'
    results = conn.execute(sql)
    # each record must be a list to make flag changeable:
    results = db_utils.results_list(results)
    return results
