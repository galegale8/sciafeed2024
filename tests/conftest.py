
import pytest

from sciafeed import db_utils


@pytest.yield_fixture(scope='session')
def conn():
    engine = db_utils.ensure_engine()
    connection = engine.connect()
    sql_cmds = [
        "drop schema if exists test cascade",
        'create schema if not exists test',
        'create schema if not exists test2',
        # 1.896.409 records of TEMP
        """create table test.ds__t200 as select * from dailypdbanpacarica.ds__t200 
           where cod_staz <=5800 and cod_staz >= 5500""",
        "ALTER TABLE test.ds__t200 ADD PRIMARY KEY (cod_staz, data_i, cod_aggr)",
        "create table test2.ds__t200 (like dailypdbanpaclima.ds__t200 including all)",
        # 1.695.420 records of PREC
        """create table test.ds__preci as select * from dailypdbanpacarica.ds__preci 
        where cod_staz <=5800 and cod_staz >= 5500""",
        "ALTER TABLE test.ds__preci ADD PRIMARY KEY (cod_staz, data_i, cod_aggr)",
        "create table test2.ds__preci (like dailypdbanpaclima.ds__preci including all)"
    ]
    for sql_cmd in sql_cmds:
        connection.execute(sql_cmd)
    yield connection
    connection.execute("drop schema test cascade")
    connection.execute("drop schema test2 cascade")
    connection.close()
