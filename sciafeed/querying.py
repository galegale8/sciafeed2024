"""
This module contains functions and utilities that extracts information from SCIA `database`.
"""
import functools
import itertools
import operator
from os import listdir
from os.path import isfile, join, splitext

from sqlalchemy.sql import select, and_, literal
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
            clause = and_(clause, column == col_value)
    results = conn.execute(select([anag_table]).where(clause)).fetchall()
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
    engine = db_utils.ensure_engine(dburi)
    meta = MetaData()
    anag_table = Table('anag__stazioni', meta, autoload=True, autoload_with=engine,
                       schema='dailypdbadmclima')
    conn = engine.connect()
    all_stations = dict()
    new_stations = dict()
    num_records = 0
    with conn.begin():
        total_to_see = listdir(data_folder)
        total_to_see_number = len(total_to_see)
        for i, file_name in enumerate(total_to_see):
            csv_path = join(data_folder, file_name)
            print('examine file %s/%s...' % (i+1, total_to_see_number))
            if not isfile(csv_path) or splitext(file_name.lower())[1] != '.csv':
                continue
            records = export.csv2data(csv_path)
            for record in records:
                num_records += 1
                record_md = record[0]
                station_key = (record_md['cod_utente'], record_md['cod_rete'])
                if station_key in all_stations:
                    continue
                station_props = {
                    'cod_rete': record_md['cod_rete']
                }
                if record_md['format'] != 'ARPA-ER':
                    station_props['cod_utente'] = record_md['cod_utente']
                else:  # workaround to manage Emilia Romagna: try to find by name
                    station_props['nome'] = record_md['cod_utente']
                db_station = get_db_station(conn, anag_table, **station_props)
                all_stations[station_key] = db_station
                if not db_station:  # NOT FOUND in the database
                    new_station = station_props
                    new_station['source'] = record_md['source']
                    new_station['lat'] = record_md['lat']
                    new_station['lon'] = record_md['lon']
                    new_stations[station_key] = new_station
                    msg = " - new station: cod_utente=%r, cod_rete=%r" % station_key
                    msgs.append(msg)
    num_all_stations = len(all_stations)
    num_new_stations = len(new_stations)
    msg0 = "Examined %i records" % num_records
    msg1 = "Found %i distinct stations" % num_all_stations
    msg2 = "Number of NEW stations: %i" % num_new_stations
    msgs = [msg0, msg1, msg2] + msgs
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
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
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
                        flag_threshold=1, exclude_values=(), exclude_null=True):
    """
    Select all the records of the table dailypdbadmclima.ds__preci order by station, date.
    If  stations_ids is not None, filter also for station ids.
    By default, only not NULL values and flagged as valid are queried.

    :param conn: db connection object
    :param sql_fields: sql string of field selection
    :param stations_ids: list of station ids (if None: no filter by station)
    :param schema: database schema to consider
    :param flag_threshold: if not None, consider where prec24.flag >= this threshold
    :param exclude_values: query excludes not none values in this iterable
    :param exclude_null: if True, excludes NULL values
    :return: the iterable of the results
    """
    sql = "SELECT %s FROM %s.ds__preci" % (sql_fields, schema)
    where_clauses = []
    if stations_ids:
        where_clauses.append('cod_staz IN %s' % repr(tuple(stations_ids)))
    if flag_threshold is not None:
        where_clauses.append('((prec24).flag).wht >= %s' % flag_threshold)
    if exclude_values:
        where_clauses.append('(prec24).val_tot NOT IN (%s)' % repr(list(exclude_values))[1:-1])
    if exclude_null:
        where_clauses.append('(prec24).val_tot IS NOT NULL')
    if where_clauses:
        sql += ' WHERE %s' % (' AND '.join(where_clauses))
    sql += ' ORDER BY cod_staz, data_i'
    # each record must be a list to make flag changeable:
    results = map(list, conn.execute(sql))
    return results


def select_temp_records(conn, fields, sql_fields='*', stations_ids=None,
                        schema='dailypdbanpacarica', flag_threshold=1, exclude_values=(),
                        exclude_null=True, where_sql=None):
    """
    Select all the records of the table dailypdbadmclima.ds__t200 order by station, date.
    If  stations_ids is not None, filter also for station ids.
    By default, only not NULL values and flagged as valid are queried, but additional
    parameters can change the filtering.

    :param conn: db connection object
    :param fields: list of field names, i.e. 'tmxgg' or 'tmngg', where to apply filters
    :param sql_fields: sql string of field selection
    :param stations_ids: list of station ids (if None: no filter by station)
    :param schema: database schema to consider
    :param flag_threshold: if not None, consider where prec24.flag >= this threshold
    :param exclude_values: query excludes not none values in this iterable
    :param exclude_null: if True, excludes NULL values
    :param where_sql: if not None, add the selected where clause in sql
    :return: the iterable of the results
    """
    sql = "SELECT %s FROM %s.ds__t200" % (sql_fields, schema)
    where_clauses = []
    if stations_ids:
        where_clauses.append('cod_staz IN %s' % repr(tuple(stations_ids)))
    if flag_threshold is not None:
        for field in fields:
            where_clauses.append('((%s).flag).wht >= %s' % (field, flag_threshold))
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
    sql += ' ORDER BY cod_staz, data_i'
    # each record must be a list to make flag changeable:
    results = map(list, conn.execute(sql))
    return results
