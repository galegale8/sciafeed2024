"""
This module contains functions and utilities that extracts information from SCIA `database`.
"""
import functools
from os import listdir
from os.path import isfile, join, splitext

from sqlalchemy.sql import select, and_, literal
from sqlalchemy import MetaData, Table

from sciafeed import db_utils
from sciafeed import export


@functools.lru_cache(maxsize=None)
def get_db_station(conn, anag_table, id_staz=None, cod_utente=None, cod_rete=None):
    """
    Get a station object from the database, by one of its primary keys

    :param conn: db connection object
    :param anag_table: table object of the stations
    :param id_staz: id of the station
    :param cod_utente: cod_utente of the station
    :param cod_rete: cod_rete of the station
    :return: the station object (if found), otherwise None
    """
    if cod_rete and cod_utente:
        clause = and_(anag_table.c.cod_utente == cod_utente, anag_table.c.cod_rete == cod_rete)
    elif id_staz:
        clause = anag_table.c.id_staz == id_staz
    else:
        raise ValueError(
            "Not possible to find a station without id_staz or without (cod_rete, cod_utente)")
    results = conn.execute(select([anag_table]).where(clause)).fetchall()
    if results:
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
                       schema='dailypdbadmclima')  # schema='sciapgm'
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
                db_station = get_db_station(
                    conn, anag_table, cod_utente=record_md['cod_utente'],
                    cod_rete=record_md['cod_rete'])
                all_stations[station_key] = db_station
                if not db_station:  # NOT FOUND in the database
                    new_station = {
                        'cod_utente': record_md['cod_utente'],
                        'cod_rete': record_md['cod_rete'],
                        'source': record_md['source'],
                        'lat': record_md['lat'],
                        'lon': record_md['lon'],
                    }
                    new_stations[station_key] = new_station
                    msg = " - new station: cod_utente=%r, cod_rete=%r" % station_key
                    msgs.append(msg)
    num_all_stations = len(all_stations)
    num_new_stations = len(new_stations)
    msg0 = "Examined %i records" % num_records
    msg1 = "Found %i distinct stations" % num_all_stations
    msg2 = "Number of NEW stations: %i" % num_new_stations
    msgs = [msg0, msg1, msg2] + msgs
    return msgs, new_stations


def get_stations_by_where(dburi, station_where):
    """
    Return a list of id_staz from a 'where' string condition.

    :param dburi: postgresql connection URI
    :param station_where: a where SQL condition
    :return: the list of id_staz
    """
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    if not station_where.strip():
        return None
    assert ';' not in station_where
    assert 'insert' not in station_where.lower()
    assert 'update' not in station_where.lower()
    assert '"' not in station_where
    sql = "SELECT id_staz FROM dailypdbadmclima.anag__stazioni WHERE %s" % station_where
    results = [r[0] for r in conn.execute(sql).fetchall()]
    return results


def select_prec_records(conn, stations_ids=None):
    """
    Select all the records of the table dailypdbadmclima.ds__preci.
    If  stations_ids is not None, filter also for station ids.

    :param conn: db connection object
    :param stations_ids: list of station ids (if None: no filter by station)
    :return: the iterable of the results
    """
    meta = MetaData()
    table = Table('ds__preci', meta, autoload=True, autoload_with=conn.engine,
                  schema='dailypdbadmclima')
    clause = literal(True)
    if stations_ids:
        clause = table.c.cod_staz in stations_ids
    results = conn.execute(select([table]).where(clause)).order_by('data_i')
    return results
