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


@functools.lru_cache(maxsize=None)
def lookup_table_desc(conn, id_field, id_value, desc_field):
    """
    return the value of the field `desc_field` of a table `table` where field `id_field`=`id_value`

    :param conn: db connection object
    :param id_field: field object used to filter one record
    :param id_value: value used to filter one record
    :param desc_field: field object used to get the attribute from the record filtered
    :return: the field `desc_field` where `id_field`=`id_value`
    """
    clause = id_field == id_value
    results = conn.execute(select([desc_field]).where(clause)).fetchall()
    if results:
        return results[0][0]
    return None


def reset_flags(conn, stations_ids, flag_threshold=-10, set_flag=1):
    """
    Reset all the flags < `flag_threshold` to the value `set_flag` for the records of
    precipitation and temperature and for selected stations.

    :param conn: db connection object
    :param stations_ids: list of station ids (if None, no filter is applied by stations)
    :param flag_threshold: max value of the flag that will not be reset
    :param set_flag: value of the flag to be set
    """
    sqls = []
    sql_prec = """
        UPDATE dailypdbanpacarica.ds__preci SET ((prec24).flag).wht = %s 
        WHERE ((prec24).flag).wht < %s""" % (set_flag, flag_threshold)
    sqls.append(sql_prec)
    if stations_ids:
        sql_prec += "AND cod_staz IN %s" % repr(tuple(stations_ids))
    sql_temp_min = """
        UPDATE dailypdbanpacarica.ds__t200  SET ((tmngg).flag).wht = %s 
        WHERE ((tmngg).flag).wht < %s""" % (set_flag, flag_threshold)
    sql_temp_max = """
        UPDATE dailypdbanpacarica.ds__t200  SET ((tmxgg).flag).wht = %s 
        WHERE ((tmxgg).flag).wht < %s""" % (set_flag, flag_threshold)
    for sql in [sql_prec, sql_temp_min, sql_temp_max]:
        if stations_ids:
            sql += "AND cod_staz IN %s" % repr(tuple(stations_ids))
        with conn.begin():
            conn.execute(sql)


def set_prec_flags(conn, records, set_flag):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field prec24
    of the table dailypdbanpacarica.ds__preci

    :param conn: db connection object
    :param records: iterable of the records
    :param set_flag: value of the flag
    """
    for record in records:
        sql = """
            UPDATE dailypdbanpacarica.ds__preci SET ((prec24).flag).wht = %s 
            WHERE data_i = %s and cod_staz = %s""" % (set_flag, record.data_i, record.cod_staz)
        conn.execute(sql)
