
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
