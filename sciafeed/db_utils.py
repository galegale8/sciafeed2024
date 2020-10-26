"""
This module contains functions and utilities to interface with a database
"""

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


def get_table_columns(table_name, schema='public'):
    """
    Return the list of column names of a table named `table_name`.

    :param table_name: the table name
    :param schema: the database schema
    :return: the list of column names
    """
    meta = MetaData()
    engine = ensure_engine()
    try:
        table_obj = Table(table_name, meta, autoload=True, autoload_with=engine, schema=schema)
    except:
        return []
    return [c.name for c in table_obj.columns]
