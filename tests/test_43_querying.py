
from datetime import datetime
from decimal import Decimal
from os.path import join

from sqlalchemy import MetaData, Table

from sciafeed import db_utils
from sciafeed import querying
from . import TEST_DATA_PATH


def test_get_db_station(conn):
    meta = MetaData()
    anag_table = Table('anag__stazioni', meta, autoload=True, autoload_with=conn.engine,
                       schema='dailypdbadmclima')

    # query by id_staz
    kwargs = {'id_staz': 1}
    station = querying.get_db_station(conn, anag_table, **kwargs)
    assert station
    assert station.id_staz == kwargs['id_staz']

    # query by (cod_utente, cod_rete)
    kwargs = {'cod_utente': '00002', 'cod_rete': 20}
    station = querying.get_db_station(conn, anag_table, **kwargs)
    assert station
    assert station.cod_utente == kwargs['cod_utente']
    assert station.cod_rete == kwargs['cod_rete']

    # query with more results
    kwargs = {'cod_rete': 20}
    station = querying.get_db_station(conn, anag_table, **kwargs)
    assert not station

    # query without results
    kwargs = {'cod_rete': 1220}
    station = querying.get_db_station(conn, anag_table, **kwargs)
    assert not station


def test_find_new_stations():
    dburi = db_utils.DEFAULT_DB_URI
    data_folder = join(TEST_DATA_PATH, 'indicators', 'input')
    msgs, new_stations = querying.find_new_stations(data_folder, dburi)
    assert msgs == [
        "Examined 2880 records", "Found 1 distinct stations",
        "Number of NEW stations: 1", " - new station: cod_utente='9', cod_rete='38'"]
    assert len(new_stations) == 1
    assert ('9', '38') in new_stations
    assert new_stations[('9', '38')] == {
        'cod_rete': '38', 'cod_utente': '9',
        'source': '38_ARSIALLazio/loc01_00009_201801010100_201901010100.dat',
        'lat': '', 'lon': ''
    }


def test_get_stations_by_where():
    dburi = db_utils.DEFAULT_DB_URI
    # no where condition
    station_where = None
    results = querying.get_stations_by_where(dburi, station_where)
    assert len(results) == 13525
    # filtering single where condition
    station_where = "cod_rete = 20"
    results = querying.get_stations_by_where(dburi, station_where)
    assert len(results) == 90
    assert 1 in results
    # filtering more where condition
    station_where = "cod_rete = 20 and cod_utente = '00002'"
    results = querying.get_stations_by_where(dburi, station_where)
    assert len(results) == 1
    assert results[0] == 1


def test_select_prec_records(conn):
    sql_fields = "cod_staz, data_i, (prec24).val_tot"
    stations_ids = [1, 2, 3]
    records = querying.select_prec_records(conn, sql_fields=sql_fields, stations_ids=stations_ids)
    results = [r for r in records]
    assert len(results) == 16244
    assert results[0] == [1, datetime(2001, 5, 18, 0, 0), Decimal('0')]
    assert results[-1] == [3, datetime(2019, 12, 31, 0, 0), Decimal('0.2')]

    sql_fields = "cod_staz, data_i"
    stations_ids = [1, 2, 3]
    records = querying.select_prec_records(conn, sql_fields=sql_fields, stations_ids=stations_ids)
    results = [r for r in records]
    assert len(results) == 16244
    assert results[0] == [1, datetime(2001, 5, 18, 0, 0)]
    assert results[-1] == [3, datetime(2019, 12, 31, 0, 0)]

    sql_fields = "cod_staz, data_i, (prec24).val_tot"
    exclude_values = (0, 0.2)
    records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=stations_ids, exclude_values=exclude_values)
    results = [r for r in records]
    assert len(results) == 4044
    assert results[0] == [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4')]
    assert results[-1] == [3, datetime(2019, 12, 22, 0, 0), Decimal('34')]


def test_select_temp_records(conn):
    fields = ['tmxgg']
    stations_ids = [1, 2, 3]
    records = querying.select_temp_records(conn, fields, stations_ids=stations_ids)
    results = [r for r in records]
    assert len(results) == 17806
    assert len(results[0]) == 36
    assert results[0][:10] == [
        datetime(2001, 5, 18, 0, 0), 1, 4, '("(24,1)",23.6,,,)',
        '(0,0,0,0,0,0,1,0,0,0,0)', '("(24,1)",14.5,,,)', '(0,0,0,0,0,0,0,1,0)',
        '("(24,1)",18.9,2.6)', '("(24,1)",19.05,)', '("(24,1)",9.1,,,)']
    assert results[-1][:10] == [
        datetime(2019, 12, 31, 0, 0), 3, 4, '("(24,1)",8.2,,,)', '(0,0,0,1,0,0,0,0,0,0,0)',
        '("(24,1)",-0.1,,,)', '(0,0,0,0,1,0,0,0,0)', '("(24,1)",2.5,2.5)', '("(24,1)",4.05,)',
        '("(24,1)",8.3,,,)']

    sql_fields = "cod_staz, data_i, (tmxgg).val_md"
    exclude_values = (23.6, 8.2)
    records = querying.select_temp_records(
        conn, fields, sql_fields=sql_fields, stations_ids=stations_ids,
        exclude_values=exclude_values)
    results = [r for r in records]
    assert len(results) == 17673
    assert len(results[0]) == 3
    assert results[0] == [1, datetime(2001, 5, 19, 0, 0), Decimal('22')]
    assert results[-1] == [3, datetime(2019, 12, 30, 0, 0), Decimal('1.8')]

    fields = ['tmngg']
    sql_fields = "cod_staz, data_i, (tmngg).val_md"
    records = querying.select_temp_records(conn, fields, sql_fields=sql_fields,
                                           stations_ids=stations_ids)
    results = [r for r in records]
    assert len(results) == 17801
    assert len(results[0]) == 3
    assert results[0] == [1, datetime(2001, 5, 18, 0, 0), Decimal('14.5')]
    assert results[-1] == [3, datetime(2019, 12, 31, 0, 0), Decimal('-0.1')]
