"""
This module contains functions and utilities to interface with a database
"""

from sqlalchemy import engine_from_config, MetaData, Table


USER = 'postgres'
PASSWORD = 'piero859'
ADDRESS = '10.158.252.181'
PORT = '5434'
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

# import sqlalchemy.pool as pool
# import psycopg2
#
# def getconn():
#     c = psycopg2.connect(user=USER, host=ADDRESS, dbname=DB_NAME, password=PASSWORD, port=PORT)
#     return c
#
# mypool = pool.QueuePool(getconn, max_overflow=10, pool_size=5)


def ensure_engine(db_uri='sqlite:///:memory:'):
    """
    Return a sqlalchemy engine object. If not configured,
    the engine is bound to the memory.

    :return: the engine object
    """
    if ENGINE is None:
        configure(db_uri=db_uri)
    return ENGINE


def ensure_connection(db_uri='sqlite:///:memory:'):
    """
    Return a sqlalchemy connection object for read and write database.
    If not configured, the connection is bound to the memory.

    :return: the connection object
    """
    engine = ensure_engine(db_uri)
    conn = engine.connect()
    return conn


def get_safe_memory_read_connection(conn):
    """
    Return a sqlalchemy connection object memory-optimized for select query only.
    It return the input connection `conn` if it is already optimized.

    :param conn: the input connection object
    :return: the connection object memory-optimized for select query only
    """
    if conn._execution_options.get('stream_results'):
        return conn
    new_conn = conn.engine.connect()
    new_conn = new_conn.execution_options(stream_results=True)
    return new_conn


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


def create_flag_map(records, flag_index=3):
    """
    Return a dictionary that maps the tuple (cod_staz, data_i) with a flag.
    Assume (cod_staz, data_i, flag) are at indexes (0, 1, `flag_index`).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param flag_index: index of the flag to change in the record
    :return: dictionary {(cod_staz, data_i): flag}
    """
    flag_map = dict()
    for record in records:
        flag_map[(record[0], record[1])] = record[flag_index]
    return flag_map


def force_flags(records, flag_map, flag_index=3, flags=(-9, 5)):
    """
    Update the flags of input `records` according to a dictionary that maps (station, date)
    with the flag to apply. The flag is at the index `flag_index` for each record.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param flag_map: dictionary of changes to apply: {(cod_staz,data_i): flag}
    :param flag_index: index of the flag to change in the record
    :param flags: iterable of flag values that returned records must have
    :return: (only) records that were updated
    """
    if not flag_map:
        return []
    ret_value = []
    for record in records:
        key = (record[0], record[1])
        if key in flag_map:
            record[flag_index] = flag_map[key]
            ret_value.append(record)
    return ret_value


def results_list(results):
    """
    Make a list over each item of an input iterable.
    This is usually useful for a sqlalchemy query result object,
    to make each result record a list instead of a tuple.

    :param results: an iterable object
    :return: an iterable of lists
    """
    for result in results:
        result = list(result)
        yield result
