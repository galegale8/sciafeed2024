
import functools
import itertools
import logging

from sqlalchemy import MetaData, Table

from sciafeed import LOG_NAME
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


def upsert_items(conn, items, policy, schema, table_name, logger=None):
    """
    Prepare items to the insert into the database

    :param conn: db connection object
    :param items: iterable of records of a db table
    :param policy: 'onlyinsert' or 'upsert'
    :param schema: database schema to use
    :param table_name: name of the table
    :param logger: logging object where to report actions
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    meta = MetaData()
    anag_table = Table('anag__stazioni', meta, autoload=True, autoload_with=conn.engine,
                       schema='dailypdbadmclima')

    items.sort(key=lambda x: (x['cod_staz'], x['data_i']))
    group_by_station = lambda x: x['cod_staz']
    group_by_date = lambda x: x['data_i']
    pkey_constraint_name = '%s_pkey' % table_name
    if policy == 'onlyinsert':
        action = 'NOTHING'
    else:
        action = "UPDATE SET (%(fields)s) = %(values)s " \
                 "WHERE excluded.cod_staz = '%(cod_staz)s' AND excluded.data_i = '%(data_i)s'"
    for station, station_records in itertools.groupby(items, group_by_station):
        cod_utente, cod_rete = station.split('--', 2)
        stat_obj = querying.get_db_station(
            conn, anag_table, cod_rete=cod_rete, cod_utente=cod_utente)
        if not stat_obj:
            logger.error("station cod_rete=%s, cod_utente=%s not found. Records ignored."
                         % (cod_rete, cod_utente))
            continue
        cod_staz = stat_obj.id_staz
        import pdb; pdb.set_trace()
        for day, day_records in itertools.groupby(station_records, group_by_date):
            # day_obj = datetime.strptime(day, '%Y-%m-%d')
            record = functools.reduce(lambda a, b: a.update(b) or a, day_records, {})
            record['cod_staz'] = cod_staz
            record = expand_fields(record)
            fields, values = list(zip(*record.items()))
            fields = ','.join(fields)
            values = repr(values).replace("'NULL'", 'NULL')
            action = action % {'fields': fields, 'values': values,
                               'cod_staz': cod_staz, 'data_i': day}
            sql = "INSERT INTO %s.%s (%s) VALUES %s ON CONFLICT ON CONSTRAINT %s DO %s" \
                  % (schema, table_name, fields, values, pkey_constraint_name, action)
            logger.debug(sql)
            conn.execute(sql)


def expand_fields(record):
    fields_map = {
        'bagna': ['bagna.flag.ndati', 'bagna.flag.wht', 'bagna.val_md',
                  'bagna.val_vr', 'bagna.val_mx', 'bagna.val_tot'],
        # TODO: add others
    }
    record_cp = record.copy()
    for field, value in record_cp.items():
        if field in fields_map:
            value = value.replace('(', '').replace(')', '')
            new_values = [r.strip() not in ('', 'None') and r.strip() or 'NULL'
                          for r in value.replace('(', '').replace(')', '').split(',')]
            for i, extra_field in enumerate(fields_map[field]):
                record[extra_field] = new_values[i]
            del record[field]
    return record
