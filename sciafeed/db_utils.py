"""
This module contains functions and utilities to interface with a database
"""
import functools

from sqlalchemy.sql import select
from sqlalchemy import engine_from_config, MetaData, Table


USER = 'siniscalchi'
PASSWORD = ''
ADDRESS = 'localhost'
PORT = '5432'
# DB_NAME = 'sciapg'
DB_NAME = 'sciadevridotto'

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
    if '.' in table_name:
        schema, table_name = table_name.split('.', 1)
    try:
        table_obj = Table(table_name, meta, autoload=True, autoload_with=engine, schema=schema)
    except:
        return []
    return [c.name for c in table_obj.columns]


@functools.lru_cache(maxsize=None)
def lookup_table_desc(conn, table, id_field, id_value, desc_field):
    """
    return the value of the field `desc_field` of a table `table` where field `id_field`=`id_value`

    :param conn: db connection object
    :param table: the table name
    :param id_field: field used to filter one record
    :param id_value: value used to filter one record
    :param desc_field: field used to get the attribute from the record filtered
    :return: the field `desc_field` where `id_field`=`id_value`
    """
    s = select([table.c.desc_field]).where(id_field == id_value)
    sql = "SELECT %s FROM %s WHERE %s='%s'" % (desc_field, table, id_field, id_value)
    results = conn.execute(sql).fetchall()
    if not results:
        return None
    else:
        return results[0][0]
