
import functools
import itertools
import logging

from sqlalchemy import MetaData, Table

from sciafeed import LOG_NAME
from sciafeed import db_utils
from sciafeed import export
from sciafeed import querying


field2class_map = {
    'bagna': 'dailypdbanpacarica.bagna1_obj',
    'elio': 'dailypdbanpacarica.stat2_obj',
    'prec24': 'dailypdbanpacarica.prec24_obj',
    'cl_prec24': 'dailypdbanpacarica.classi_prec_obj',
    'prec01': 'dailypdbanpacarica.prec_obj',
    'prec06': 'dailypdbanpacarica.prec_obj',
    'cl_prec06': 'dailypdbanpacarica.classi_prec_obj',
    'prec12': 'dailypdbanpacarica.prec_obj',
    'cl_prec12': 'dailypdbanpacarica.classi_prec_obj',
    'ggneve': 'dailypdbanpacarica.nxxx_obj',
    'storm': 'dailypdbanpacarica.nxxx_obj',
    'ggstorm': 'dailypdbanpacarica.nxxx_obj',
    'press': 'dailypdbanpacarica.stat0_obj',
    'radglob': 'dailypdbanpacarica.stat0_obj',
    'tmxgg': 'dailypdbanpacarica.estremi_t_obj',
    'cl_tmxgg': 'dailypdbanpacarica.classi_tmx_obj',
    'tmngg': 'dailypdbanpacarica.estremi_t_obj',
    'cl_tmngg': 'dailypdbanpacarica.classi_tmn_obj',
    'tmdgg': 'dailypdbanpacarica.stat1_obj',
    'tmdgg1': 'dailypdbanpacarica.stat1_obj',
    'deltagg': 'dailypdbanpacarica.stat0_obj',
    'day_gelo': 'dailypdbanpacarica.nxxx_obj',
    'cl_tist': 'dailypdbanpacarica.classi_tist_obj',
    't00': 'dailypdbanpacarica.stat0_obj',
    't01': 'dailypdbanpacarica.stat0_obj',
    't02': 'dailypdbanpacarica.stat0_obj',
    't03': 'dailypdbanpacarica.stat0_obj',
    't04': 'dailypdbanpacarica.stat0_obj',
    't05': 'dailypdbanpacarica.stat0_obj',
    't06': 'dailypdbanpacarica.stat0_obj',
    't07': 'dailypdbanpacarica.stat0_obj',
    't08': 'dailypdbanpacarica.stat0_obj',
    't09': 'dailypdbanpacarica.stat0_obj',
    't10': 'dailypdbanpacarica.stat0_obj',
    't11': 'dailypdbanpacarica.stat0_obj',
    't12': 'dailypdbanpacarica.stat0_obj',
    't13': 'dailypdbanpacarica.stat0_obj',
    't14': 'dailypdbanpacarica.stat0_obj',
    't15': 'dailypdbanpacarica.stat0_obj',
    't16': 'dailypdbanpacarica.stat0_obj',
    't17': 'dailypdbanpacarica.stat0_obj',
    't18': 'dailypdbanpacarica.stat0_obj',
    't19': 'dailypdbanpacarica.stat0_obj',
    't20': 'dailypdbanpacarica.stat0_obj',
    't21': 'dailypdbanpacarica.stat0_obj',
    't22': 'dailypdbanpacarica.stat0_obj',
    't23': 'dailypdbanpacarica.stat0_obj',
    'ur': 'dailypdbanpacarica.stat01_obj',
    'ur00': 'dailypdbanpacarica.stat1_obj',
    'ur01': 'dailypdbanpacarica.stat1_obj',
    'ur02': 'dailypdbanpacarica.stat1_obj',
    'ur03': 'dailypdbanpacarica.stat1_obj',
    'ur04': 'dailypdbanpacarica.stat1_obj',
    'ur05': 'dailypdbanpacarica.stat1_obj',
    'ur06': 'dailypdbanpacarica.stat1_obj',
    'ur07': 'dailypdbanpacarica.stat1_obj',
    'ur08': 'dailypdbanpacarica.stat1_obj',
    'ur09': 'dailypdbanpacarica.stat1_obj',
    'ur10': 'dailypdbanpacarica.stat1_obj',
    'ur11': 'dailypdbanpacarica.stat1_obj',
    'ur12': 'dailypdbanpacarica.stat1_obj',
    'ur13': 'dailypdbanpacarica.stat1_obj',
    'ur14': 'dailypdbanpacarica.stat1_obj',
    'ur15': 'dailypdbanpacarica.stat1_obj',
    'ur16': 'dailypdbanpacarica.stat1_obj',
    'ur17': 'dailypdbanpacarica.stat1_obj',
    'ur18': 'dailypdbanpacarica.stat1_obj',
    'ur19': 'dailypdbanpacarica.stat1_obj',
    'ur20': 'dailypdbanpacarica.stat1_obj',
    'ur21': 'dailypdbanpacarica.stat1_obj',
    'ur22': 'dailypdbanpacarica.stat1_obj',
    'ur23': 'dailypdbanpacarica.stat1_obj',
    'cl_ur06': 'dailypdbanpacarica.classi_ur_obj',
    'cl_ur12': 'dailypdbanpacarica.classi_ur_obj',
    'vntmxgg': 'dailypdbanpacarica.vntmxgg_obj',
    'vnt': 'dailypdbanpacarica.vnt16_obj',
    'prs_ff': 'dailypdbanpacarica.prs_ff_obj',
    'prs_dd': 'dailypdbanpacarica.prs_dd_obj',
    'vntmd': 'dailypdbanpacarica.vntmd_obj',
}
class2subfields_map = {
    'dailypdbanpacarica.bagna1_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_mx', 'val_tot'],
    'dailypdbanpacarica.stat2_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_mx'],
    'dailypdbanpacarica.prec_obj': ['flag.ndati', 'flag.wht', 'val_mx', 'data_mx'],
    'dailypdbanpacarica.prec24_obj':
        ['flag.ndati', 'flag.wht', 'val_tot', 'val_mx', 'data_mx'],
    'dailypdbanpacarica.classi_prec_obj':
        ['dry', 'wet_01', 'wet_02', 'wet_03', 'wet_04', 'wet_05'],
    'dailypdbanpacarica.nxxx_obj': ['flag.ndati', 'flag.wht', 'num'],
    'dailypdbanpacarica.stat0_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_mx', 'val_mn'],
    'dailypdbanpacarica.estremi_t_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_x', 'data_x'],
    'dailypdbanpacarica.classi_tmx_obj':
        ['cl_01', 'cl_02', 'cl_03', 'cl_04', 'cl_05', 'cl_06', 'cl_07',
         'cl_08', 'cl_09', 'cl_10', 'cl_11'],
    'dailypdbanpacarica.classi_tmn_obj':
        ['cl_01', 'cl_02', 'cl_03', 'cl_04', 'cl_05', 'cl_06', 'cl_07', 'cl_08', 'cl_09'],
    'dailypdbanpacarica.stat1_obj': ['flag.ndati', 'flag.wht', 'val_md', 'val_vr'],
    'dailypdbanpacarica.classi_tist_obj': [
        'flag.ndati', 'flag.wht', 'cl_01', 'cl_02', 'cl_03', 'cl_04', 'cl_05', 'cl_06',
        'cl_07', 'cl_08', 'cl_09', 'cl_10', 'cl_11', 'cl_12', 'cl_13', 'cl_14'],
    'dailypdbanpacarica.stat01_obj': [
        'flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'flag1.ndati', 'flag1.wht',
        'val_mx', 'val_mn'],
    'dailypdbanpacarica.classi_ur_obj': ['cl_01', 'cl_02', 'cl_03', 'cl_04'],
    'dailypdbanpacarica.vntmxgg_obj': ['flag.ndati', 'flag.wht', 'ff', 'dd'],
    'dailypdbanpacarica.vnt16_obj': [
        'flag.ndati', 'flag.wht', 'frq_calme',
        'frq_s01c1', 'frq_s01c2', 'frq_s01c3', 'frq_s01c4',
        'frq_s02c1', 'frq_s02c2', 'frq_s02c3', 'frq_s02c4',
        'frq_s03c1', 'frq_s03c2', 'frq_s03c3', 'frq_s03c4',
        'frq_s04c1', 'frq_s04c2', 'frq_s04c3', 'frq_s04c4',
        'frq_s05c1', 'frq_s05c2', 'frq_s05c3', 'frq_s05c4',
        'frq_s06c1', 'frq_s06c2', 'frq_s06c3', 'frq_s06c4',
        'frq_s07c1', 'frq_s07c2', 'frq_s07c3', 'frq_s07c4',
        'frq_s08c1', 'frq_s08c2', 'frq_s08c3', 'frq_s08c4',
        'frq_s09c1', 'frq_s09c2', 'frq_s09c3', 'frq_s09c4',
        'frq_s10c1', 'frq_s10c2', 'frq_s10c3', 'frq_s10c4',
        'frq_s11c1', 'frq_s11c2', 'frq_s11c3', 'frq_s11c4',
        'frq_s12c1', 'frq_s12c2', 'frq_s12c3', 'frq_s12c4',
        'frq_s13c1', 'frq_s13c2', 'frq_s13c3', 'frq_s13c4',
        'frq_s14c1', 'frq_s14c2', 'frq_s14c3', 'frq_s14c4',
        'frq_s15c1', 'frq_s15c2', 'frq_s15c3', 'frq_s15c4',
        'frq_s16c1', 'frq_s16c2', 'frq_s16c3', 'frq_s16c4'],
    'dailypdbanpacarica.prs_ff_obj': ['n0', 'n1', 'n2', 'n3', 'n4'],
    'dailypdbanpacarica.prs_dd_obj': [
        'n01', 'ff01', 'n02', 'ff02', 'n03', 'ff03', 'n04', 'ff04',
        'n05', 'ff05', 'n06', 'ff06', 'n07', 'ff07', 'n08', 'ff08',
        'n09', 'ff09', 'n10', 'ff10', 'n11', 'ff11', 'n12', 'ff12',
        'n13', 'ff13', 'n14', 'ff14', 'n15', 'ff15', 'n16', 'ff16'],
    'dailypdbanpacarica.vntmd_obj': ['flag.ndati', 'flag.wht', 'ff']
}


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


def update_prec_flags(conn, records, schema='dailypdbanpacarica', logger=None):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field prec24
    of the table dailypdbanpacarica.ds__preci.
    It assumes each record has attributes data_i and cod_staz

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, value, flag, ...]
    :param schema: database schema to use
    :param logger: logging object where to report actions
    :return number of updates
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
    return num_of_updates


def update_temp_flags(conn, records, schema='dailypdbanpacarica', db_field='tmxgg', flag_index=3,
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
    :return number of updates
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
    return num_of_updates


def expand_fields(record):
    record_cp = record.copy()
    for field, value in record_cp.items():
        if field in field2class_map:
            klass = field2class_map[field]
            subfields = class2subfields_map[klass]
            new_values = [
                r.strip() not in ('', 'None') and r.strip() or 'NULL'
                for r in value.replace('(', '').replace(')', '').replace("'", "").
                replace('"', '').split(',')]
            for i, subfield in enumerate(subfields):
                record["%s.%s" % (field, subfield)] = new_values[i]
            del record[field]
    return record


def get_record_items(record, insert_mode=True):
    if insert_mode:
        # don't consider to set NULL values for empty fields
        record = {k: v for k, v in record.items()
                  if v is not None and list(filter(lambda r: r.isdigit(), str(v)))}
    record = expand_fields(record)
    fields, values = list(zip(*record.items()))
    fields = ','.join(fields)
    values = repr(values)[1:-1].replace("'NULL'", 'NULL')
    return fields, values


def upsert_items(conn, items, policy, schema, table_name, logger=None):
    """
    Insert (or update if not exists) items into the database

    :param conn: db connection object
    :param items: iterable of records of a db table. Each record is a dictionary
    :param policy: 'onlyinsert' or 'upsert'
    :param schema: database schema to use
    :param table_name: name of the table
    :param logger: logging object where to report actions
    :return number of updates
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
    num_of_updates = 0
    if policy == 'onlyinsert':
        action = 'NOTHING'
    else:
        action = "UPDATE SET (%(fields)s) = (%(values)s) " \
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
        for day, day_records in itertools.groupby(station_records, group_by_date):
            # day_obj = datetime.strptime(day, '%Y-%m-%d')
            record = functools.reduce(lambda a, b: a.update(b) or a, day_records, {})
            record['cod_staz'] = cod_staz
            fields, values = get_record_items(record, insert_mode=False)
            action = action % {'fields': fields, 'values': values,
                               'cod_staz': cod_staz, 'data_i': day}
            sql = "INSERT INTO %s.%s (%s) VALUES (%s) ON CONFLICT ON CONSTRAINT %s DO %s" \
                  % (schema, table_name, fields, values, pkey_constraint_name, action)
            logger.debug(sql)
            result = conn.execute(sql)
            num_of_updates += result.rowcount
    return num_of_updates


def merge_records(records, master_field):
    """
    Return a merged version of the input records (ignoring null values or flagged as invalid).
    In case of conflict of values, the winner value belongs to the first of the input
    records that has a valid value.
    The key 'cod_stazprinc' is assigned to the first id_staz that has the 'master field' in the
    merged result.

    :param records: iterable of input records to merge
    :param master_field:
    :return: the merged record
    """
    records = list(records)[::-1]

    def merge_function(a, b):
        a2 = {k: v for k, v in a.items() if v is not None and not str(v).startswith('("(0,')}
        b2 = {k: v for k, v in b.items() if v is not None and not str(v).startswith('("(0,')}
        return a2.update(b2) or a2

    merged = functools.reduce(merge_function, records, {})
    master_value = merged.get(master_field)
    if master_value is None:
        return None
    for record in records:
        if record[master_field] == master_value:
            merged['cod_stazprinc'] = record['cod_staz']
    return merged


def load_unique_data(conn, startschema, targetschema, logger, only_tables=None):
    """
    Load data from `startschema` to `targetschema`, merging data from duplicate stations.

    :param conn: db connection object
    :param startschema: db schema where to find input data tables
    :param targetschema: db schema where to put merged records
    :param logger: logger object for reporting
    :param only_tables: if not None, list of names of data tables to work on
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    gruppi_tname = 'tabgruppistazioni'
    gruppi_tschema = 'dailypdbanpacarica'
    logger.info("loading tabgruppistazioni")
    group2mainstation = querying.load_main_station_groups(conn, gruppi_tname, gruppi_tschema)
    group_funct = lambda r: (r[0], r[1])  # idgruppo, data_i
    tables = [
        ('ds__preci', 'prec24'),
        ('ds__t200', 'tmxgg'),  # TODO: ASK: master field?
        ('ds__bagna', 'bagna'),
        ('ds__elio', 'elio'),
        ('ds__press', 'press'),
        ('ds__urel', 'ur'),
        ('ds__radglob', 'radglob'),
        ('ds__vnt10', 'vntmd'),  # TODO: ASK: master field?
    ]
    if only_tables is not None:
        tables = [t for t in tables if t[0] in tables]
    for table_name, master_field in tables:
        logger.info('* start working on table %s' % table_name)
        sql = """SELECT idgruppo, data_i::varchar, %s.%s.* FROM %s.%s JOIN %s.%s ON (id_staz=cod_staz)
                 ORDER BY (idgruppo, data_i, progstazione)""" \
              % (startschema, table_name, gruppi_tschema, gruppi_tname, startschema, table_name)
        results = conn.execute(sql)
        inserted = 0
        for group_attrs, group_records in itertools.groupby(results, group_funct):
            groupid, data_i = group_attrs
            main_station = group2mainstation[groupid]
            new_record = merge_records(group_records, master_field)
            if new_record:
                del new_record['idgruppo']
                new_record['cod_staz'] = main_station
                new_record['data_i'] = data_i
                fields, values = get_record_items(new_record)
                sql = "INSERT INTO %s.%s (%s) VALUES (%s)" \
                      % (targetschema, table_name, fields, values)
                conn.execute(sql)
                inserted += 1
        logger.info('inserted %s records' % inserted)
