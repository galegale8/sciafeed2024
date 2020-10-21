
import pytest

from sciafeed import db_utils


@pytest.yield_fixture(scope='session')
def conn():
    engine = db_utils.ensure_engine()
    connection = engine.connect()
    connection.execute("drop schema if exists test cascade")
    connection.execute('create schema if not exists test')

    # 1896409 records of TEMP
    connection.execute("create table test.ds__t200 as select * from dailypdbanpacarica.ds__t200 "
                       "where cod_staz <=5800 and cod_staz >= 5500")
    # 1695420 records of PREC
    connection.execute("create table test.ds__preci as select * from dailypdbanpacarica.ds__preci "
                       "where cod_staz <=5800 and cod_staz >= 5500")
    yield connection
    connection.execute("drop schema test cascade")
    connection.close()