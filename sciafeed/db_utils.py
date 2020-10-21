"""
This module contains functions and utilities to interface with a database
"""
import functools

from sqlalchemy.sql import select
from sqlalchemy import engine_from_config, MetaData, Table


USER = 'scia'
PASSWORD = 'scia'
ADDRESS = ''
PORT = '5432'
DB_NAME = 'sciapg'

DEFAULT_DB_URI = "postgresql://%s:%s@%s:%s/%s" % (USER, PASSWORD, ADDRESS, PORT, DB_NAME)
ENGINE = None


def configure(db_uri=None):
    """
    Configure the connection to the database, setting the value of the ENGINE
    global variable.
    This method should be launched before the first use of this module.

    :param db_uri: postgresql connection URI
    """
    global ENGINE
    if not db_uri:
        db_uri = DEFAULT_DB_URI
    db_config = {
            'sqlalchemy.url': db_uri,
            'sqlalchemy.encoding': 'utf-8',
            'sqlalchemy.echo': False,
            'sqlalchemy.pool_recycle': 300,
    }
    ENGINE = engine_from_config(db_config)


configure()


def ensure_engine(db_uri='sqlite:///:memory:'):
    """
    Return a sqlalchemy engine object. If not configured,
    the engine is bound to the memory.

    :return: the engine object
    """
    if ENGINE is None:
        configure(db_uri=db_uri)
    return ENGINE


def get_table_columns(table_name):
    """
    Return the list of column names of a table named `table_name`.

    :param table_name: the table name
    :return: the list of column names
    """
    meta = MetaData()
    engine = ensure_engine()
    schema = 'public'
    if '.' in table_name:
        schema, table_name = table_name.split('.', 1)
    try:
        table_obj = Table(table_name, meta, autoload=True, autoload_with=engine, schema=schema)
    except:
        return []
    return [c.name for c in table_obj.columns]


def reset_flags(conn, stations_ids, flag_threshold=-10, set_flag=1, dry_run=False):
    """
    Reset all the flags < `flag_threshold` to the value `set_flag` for the records of
    precipitation and temperature and for selected stations.

    :param conn: db connection object
    :param stations_ids: list of station ids (if None, no filter is applied by stations)
    :param flag_threshold: max value of the flag that will not be reset
    :param set_flag: value of the flag to be set
    :param dry_run: True only for testing
    :return the list of SQL executed
    """
    executed = []
    sql_prec = """
      UPDATE dailypdbanpacarica.ds__preci SET ( 
      ((prec24).flag).wht,
      ((prec01).flag).wht,
      ((prec06).flag).wht,
      ((prec12).flag).wht
      ) = (%s, %s, %s, %s)
      WHERE ((prec24).flag).wht < %s""" % (set_flag, set_flag, set_flag, set_flag, flag_threshold)
    sql_temp_min = """
      UPDATE dailypdbanpacarica.ds__t200  SET ((tmngg).flag).wht = %s 
      WHERE ((tmngg).flag).wht < %s""" % (set_flag, flag_threshold)
    sql_temp_max = """
      UPDATE dailypdbanpacarica.ds__t200  SET ((tmxgg).flag).wht = %s 
      WHERE ((tmxgg).flag).wht < %s""" % (set_flag, flag_threshold)
    for sql in [sql_prec, sql_temp_min, sql_temp_max]:
        if stations_ids:
            sql += " AND cod_staz IN %s" % repr(tuple(stations_ids))
        if not dry_run:
            with conn.begin():
                conn.execute(sql)
        executed.append(sql)
    return executed


def set_prec_flags(conn, records, set_flag, schema='dailypdbanpacarica', dry_run=False):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field prec24
    of the table dailypdbanpacarica.ds__preci.
    It assumes each record has attributes data_i and cod_staz

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param set_flag: value of the flag
    :param schema: database schema to use
    :param dry_run: True only for testing
    :return the list of SQL executed
    """
    executed = []
    for record in records:
        sql = """
            UPDATE %s.ds__preci SET (
            prec24.flag.wht,
            prec01.flag.wht,
            prec06.flag.wht,
            prec12.flag.wht
            ) = (%s, %s, %s, %s)
            WHERE data_i = '%s' AND cod_staz = %s""" \
              % (schema, set_flag, set_flag, set_flag, set_flag, record[1], record[0])
        if not dry_run:
            conn.execute(sql)
        executed.append(sql)
    return executed


def set_temp_flags(conn, records, var, set_flag, schema='dailypdbanpacarica', dry_run=False):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field prec24
    of the table dailypdbanpacarica.ds__preci.
    It assumes each record has attributes data_i and cod_staz

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param var: 'Tmax' or 'Tmin'
    :param set_flag: value of the flag
    :param schema: database schema to use
    :param dry_run: True only for testing
    :return the list of SQL executed
    """
    executed = []
    var2dbfield_map = {
        'Tmax': 'tmxgg',
        'Tmin': 'tmngg',
    }
    db_field = var2dbfield_map[var]
    for record in records:
        sql = """
            UPDATE %s.ds__t200 SET %s.flag.wht = %s
            WHERE data_i = '%s' AND cod_staz = %s""" \
              % (schema, db_field, set_flag, record[1], record[0])
        if not dry_run:
            conn.execute(sql)
        executed.append(sql)
    return executed
