
from sqlalchemy import MetaData, Table

from sciafeed import db_utils
from sciafeed import export
from sciafeed import querying


def upsert_stations(dburi, stations_path):
    """
    Load a list of stations from a CSV file located at `stations_path` and insert them
    in the database located at `dburi`. If stations already exists, they will be updated
    according to the CSV file.
    Returns a list of report messages and the ids of the updated/inserted stations.

    :param dburi: db connection string
    :param stations_path: CSV path of the input stations
    :return: msgs, num_inserted, num_updated
    """
    msgs = []
    upserted_ids = []
    engine = db_utils.ensure_engine(dburi)
    meta = MetaData()
    anag_table = Table('anag__stazioni', meta, autoload=True, autoload_with=engine,
                       schema='dailypdbadmclima')
    update_obj = anag_table.update()
    insert_obj = anag_table.insert()
    conn = engine.connect()
    try:
        stations = export.csv2items(
            stations_path,
            ['nome', 'cod_utente', 'cod_rete', 'cod_entep', 'cod_entef', 'cod_enteg'],
            ignore_fields=['source'])
    except ValueError as err:
        msgs = [str(err)]
        return msgs, upserted_ids
    new_stations = []
    num_updated_stations = 0
    for station in stations:
        db_station = querying.get_db_station(
            conn, anag_table, cod_rete=station['cod_rete'], cod_utente=station['cod_utente'])
        if db_station:
            where_clause = anag_table.c.id_staz == db_station['id_staz']
            station['id_staz'] = db_station['id_staz']
            conn.execute(update_obj.where(where_clause).values(**station))
            num_updated_stations += 1
            msgs.append('updated existing station id_staz=%s' % db_station['id_staz'])
        else:
            new_stations.append(station)
    if new_stations:
        conn.execute(insert_obj.values(new_stations))
    num_inserted_stations = len(new_stations)
    msgs.append('inserted %i new stations' % num_inserted_stations)
    return msgs, num_inserted_stations, num_updated_stations


def upsert_from_csv_table(dburi, csv_table_path, report_path=None):
    """
    Load data from a CSV file and insert it in the table with the same name

    :param dburi:
    :param csv_table_path:
    :param report_path:
    :return:
    """
    # TODO
    # for each row of the CSV:
    #   open a transaction
    #   find the primary key pk (date, id_staz)
    #   query db to decide if to do an insert or an update
    #   if update:
    #      find the list of values not None of the record to be updated
    #        (to avoid to set None the not-null fields for the existing records)
    #        (avoid in general to set values None, in case do not insert the record at all)
    #      create and execute the update sql
    #   else:  (insert)
    #      create the insert sql from the csv row and execute it
    #   close transaction

