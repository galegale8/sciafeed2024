"""
This module contains functions and utilities that extracts information from SCIA `database`.
"""
from sqlalchemy import engine_from_config, Table, MetaData

USER = 'scia'
PASSWORD = 'scia'
ADDRESS = 'localhost'
PORT = '5432'
DB_NAME = 'sciapg'

DEFAULT_DB_URI = "postgresql://%s:%s@%s:%s/%s" % (USER, PASSWORD, ADDRESS, PORT, DB_NAME)
ENGINE = None


def configure(db_uri=None):
    """
    Configure the connection to the database, setting the value of the ENGINE
    global variable.

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


def ensure_engine():
    """
    Return a sqlalchemy engine object. If not configured,
    the engine is bound to the memory.

    :return: the engine object
    """
    if ENGINE is None:
        configure(db_uri='sqlite:///:memory:')
    return ENGINE


def extract_station_id(conn, metadata):  # pragma: no cover
    """
    Return the station ID from the station properties.
    Currently it assumes that station_props containing the keys 'cod_rete' and 'cod_utente'.

    :param conn: db connection object
    :param metadata: dictionary of metadata extracted from data
    :return: the station ID (or None if not found)
    """
    cod_rete = metadata.get('cod_rete')
    cod_utente = metadata.get('cod_utente') + metadata.get('cod_utente_prefix', '')
    if cod_rete and cod_utente:
        sql = "SELECT id_staz FROM sciapgm.anag__stazioni WHERE cod_utente='%s' AND cod_rete='%s'"\
               % (cod_utente, cod_rete)
        results = conn.execute(sql).fetchall()
        if len(results) == 1:
            return results[0][0]
    return None


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


def find_new_stations(data_folder, dburi, stations_path, report_path=None):
    """
    Find stations from a set of CSV files inside a folder `data_folder`, that are not
    present in the database and creates a CSV with the list.

    :param data_folder: folder path where CSV files are in
    :param dburi: db connection URI
    :param stations_path: path of the CSV with the stations not found.
    :param report_path: file path of the report of the operations done.
    :return: ([list of report messages], [list of not found stations])
    """
    # TODO
    msgs = []
    not_found_stations = []
    return msgs, not_found_stations
