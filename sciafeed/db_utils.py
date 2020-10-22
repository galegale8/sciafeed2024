"""
This module contains functions and utilities to interface with a database
"""
import logging

from sqlalchemy import engine_from_config, MetaData, Table
from sciafeed import LOG_NAME


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


def update_prec_flags(conn, records, schema='dailypdbanpacarica', logger=None):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field prec24
    of the table dailypdbanpacarica.ds__preci.
    It assumes each record has attributes data_i and cod_staz

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param schema: database schema to use
    :param logger: logging object where to report actions
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info('start process for update of PREC flags')
    pre_sql_cmds = [
        'DROP TABLE IF EXISTS updates_table',
        '''
    CREATE TABLE IF NOT EXISTS updates_table (
        cod_staz integer NOT NULL,
        data_i timestamp without time zone NOT NULL,
        flag integer,
        PRIMARY KEY (cod_staz, data_i)
        )''',
    ]
    for cmd in pre_sql_cmds:
        conn.execute(cmd)
    logger.debug('created temp folder')
    meta = MetaData()
    table_obj = Table('updates_table', meta, autoload=True, autoload_with=conn.engine)
    data = [{'cod_staz': r[0], 'data_i': r[1], 'flag': r[3]} for r in records]
    conn.execute(table_obj.insert(), data)
    logger.debug('filled temp folder')
    update_sql = """
        UPDATE %s.ds__preci t SET (
            prec24.flag.wht,
            prec01.flag.wht,
            prec06.flag.wht,
            prec12.flag.wht
            ) = (u.flag, u.flag, u.flag, u.flag)
        FROM updates_table u
        WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i AND ((t.prec24).flag).wht <> u.flag
    """ % schema
    result = conn.execute(update_sql)
    num_of_updates = result.rowcount
    logger.info('update completed: %s flags updated' % num_of_updates)
    post_cmd = 'DROP TABLE updates_table'
    conn.execute(post_cmd)
    logger.debug('temp folder removed')


def update_temp_flags(conn, records, schema='dailypdbanpacarica', db_field='tmxgg', flag_index=2,
                      logger=None):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field prec24
    of the table dailypdbanpacarica.ds__t200.
    It assumes each record has attributes data_i and cod_staz

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param schema: database schema to use
    :param db_field: name of the database field related to the flag
    :param flag_index: index of the flag value in each record
    :param logger: logging object where to report actions
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info('start process for update of TEMP flags (%s)' % db_field)
    pre_sql_cmds = [
        'DROP TABLE IF EXISTS updates_table2',
        '''
    CREATE TABLE IF NOT EXISTS updates_table2 (
        cod_staz integer NOT NULL,
        data_i timestamp without time zone NOT NULL,
        flag integer,
        PRIMARY KEY (cod_staz, data_i)
        )''',
    ]
    for cmd in pre_sql_cmds:
        conn.execute(cmd)
    logger.debug('created temp folder')
    meta = MetaData()
    table_obj = Table('updates_table2', meta, autoload=True, autoload_with=conn.engine)
    data = [{'id_record': i, 'cod_staz': r[0], 'data_i': r[1], 'flag': r[flag_index]}
            for i, r in enumerate(records, 1)]
    conn.execute(table_obj.insert(), data)
    logger.debug('filled temp folder')
    update_sql = """
        UPDATE %s.ds__t200 t SET %s.flag.wht = u.flag
        FROM updates_table2 u
        WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i AND ((t.%s).flag).wht <> u.flag
    """ % (schema, db_field, db_field)
    result = conn.execute(update_sql)
    num_of_updates = result.rowcount
    logger.info('update completed: %s flags updated for %s' % (num_of_updates, db_field))
    post_cmd = 'DROP TABLE updates_table2'
    conn.execute(post_cmd)
    logger.debug('temp folder removed')
