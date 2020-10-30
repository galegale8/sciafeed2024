
import functools
import itertools
import logging
import time

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
    logger.debug('start db update of PREC flags')
    tmp_table_name = "updates_preci%s" % round(time.time())
    pre_sql_cmds = [
        'DROP TABLE IF EXISTS %s' % tmp_table_name,
        '''
    CREATE TABLE IF NOT EXISTS %s (
        cod_staz integer NOT NULL,
        data_i timestamp without time zone NOT NULL,
        flag integer,
        PRIMARY KEY (cod_staz, data_i)
        )''' % tmp_table_name,
    ]
    for cmd in pre_sql_cmds:
        conn.execute(cmd)
    logger.debug('created temp folder')
    meta = MetaData()
    table_obj = Table(tmp_table_name, meta, autoload=True, autoload_with=conn.engine)
    # TODO try/except/finally clause? (to ensure the drop of temp folder)
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
        FROM %s u
        WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i AND ((t.prec24).flag).wht <> u.flag
    """ % (schema, tmp_table_name)
    result = conn.execute(update_sql)
    num_of_updates = result.rowcount

    logger.info('update completed: %s flags updated' % num_of_updates)
    post_cmd = 'DROP TABLE %s' % tmp_table_name
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
    logger.debug('start db update of TEMP flags (%s)' % db_field)
    tmp_table_name = "updates_temp%s" % round(time.time())
    pre_sql_cmds = [
        'DROP TABLE IF EXISTS %s' % tmp_table_name,
        '''
    CREATE TABLE IF NOT EXISTS %s (
        cod_staz integer NOT NULL,
        data_i timestamp without time zone NOT NULL,
        flag integer,
        PRIMARY KEY (cod_staz, data_i)
        )''' % tmp_table_name,
    ]
    for cmd in pre_sql_cmds:
        conn.execute(cmd)
    logger.debug('created temp folder')
    meta = MetaData()
    table_obj = Table(tmp_table_name, meta, autoload=True, autoload_with=conn.engine)
    data = [{'id_record': i, 'cod_staz': r[0], 'data_i': r[1], 'flag': r[flag_index]}
            for i, r in enumerate(records, 1)]
    conn.execute(table_obj.insert(), data)
    logger.debug('filled temp folder')
    update_sql = """
        UPDATE %s.ds__t200 t SET %s.flag.wht = u.flag
        FROM %s u
        WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i AND ((t.%s).flag).wht <> u.flag
    """ % (schema, db_field, tmp_table_name, db_field)
    result = conn.execute(update_sql)
    num_of_updates = result.rowcount
    logger.info('update completed: %s flags updated' % num_of_updates)
    post_cmd = 'DROP TABLE %s' % tmp_table_name
    conn.execute(post_cmd)
    logger.debug('temp folder removed')
    return num_of_updates


def expand_record(record):
    """
    Get an input record (dictionary) and return a new record with the not null values specified
    by the complete name of the key.
    For example: {'prec24': '("(1,2)",,,)'} -> {'prec24.flag.ndati':1, 'prec24.flag.wht':2}
    Values blank or 'None' are returned with the string 'NULL'.

    :param record: input record (as dictionary)
    :return: new record with not null values expanded.
    """
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


def expand_fields(fields):
    """
    Get an input list of fields and return a list of fields with the complete names specified.
    For example: ['prec24'] -> ['prec24.flag.ndati', 'prec24.flag.wht', ...]

    :param fields: iterable of the fields
    :return: new list with the complete names
    """
    new_fields = []
    for field in fields:
        if field in field2class_map:
            klass = field2class_map[field]
            subfields = ["%s.%s" % (field, subfield) for subfield in class2subfields_map[klass]]
            new_fields.extend(subfields)
        else:
            new_fields.append(field)
    return new_fields


def create_insert(table_name, schema, fields, data):
    if not data or not fields:
        return
    fields_str = ','.join(fields)
    values_str = ''
    for item in data:
        cur_values_str = '('
        for field in fields:
            if field in item:
                value = "'%s'," % item[field]
            else:
                value = 'NULL,'
            cur_values_str += value
        values_str += cur_values_str[:-1] + '),'
    values_str = values_str[:-1]
    sql = "INSERT INTO %s.%s (%s) VALUES %s" % (schema, table_name, fields_str, values_str)
    return sql


def create_upsert(table_name, schema, fields, data, policy):
    if not data or not fields:
        return
    insert_sql = create_insert(table_name, schema, fields, data)
    insert_sql += " ON CONFLICT ON CONSTRAINT %s_pkey DO " % table_name
    if policy == 'onlyinsert':
        insert_sql += 'NOTHING'
        return insert_sql
    insert_sql += 'UPDATE SET (%s) = ' % (','.join(fields))

    fields2 = []
    for field in fields:
        tokens = tuple(field.split('.'))
        tokens_num = len(tokens)
        if tokens_num == 1:
            fields2.append("EXCLUDED.%s" % field)
        elif tokens_num == 2:
            fields2.append("(EXCLUDED.%s).%s" % tokens)
        else:
            fields2.append("((EXCLUDED.%s).%s).%s" % tokens)
    insert_sql += '(%s) ' % (','.join(fields2))
    insert_sql += "WHERE %s.cod_staz = EXCLUDED.cod_staz AND %s.data_i = EXCLUDED.data_i" \
                  % (table_name, table_name)
    return insert_sql


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
    upserted = 0
    cols = db_utils.get_table_columns(table_name, schema)
    fields = expand_fields(cols)

    for station, station_records in itertools.groupby(items, group_by_station):
        data = []
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
            record = expand_record(record)
            record = {
                k: v for k, v in record.items()
                if v not in (None, 'NULL') and list(filter(lambda r: r.isdigit(), str(v)))
            }
            data.append(record)
            upserted += 1
        sql = create_upsert(table_name, schema, fields, data, policy)
        conn.execute(sql)
    return upserted


def choose_main_record(records, master_field):
    """
    Choose the first one of the input records that has the field `master_field` valid and not null.
    The key 'cod_stazprinc' is assigned to the chosen id_staz.

    :param records: iterable of input records to merge
    :param master_field:
    :return: the merged record
    """
    result = None
    master_flag = "%s.flag.wht" % master_field.rsplit('.', 1)[0]
    for record in records:
        if master_flag in record:
            flag = str(record[master_flag])
            if flag.isdigit() and int(flag) > 0 and record.get(master_field) not in (None, 'NULL'):
                result = record.copy()
                result['cod_stazprinc'] = record['cod_staz']
                break
        elif record.get(master_field) not in (None, 'NULL'):
            result = record.copy()
            result['cod_stazprinc'] = record['cod_staz']
            break
    return result


def load_unique_data(conn, startschema, targetschema, logger=None, only_tables=None):
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
        ('ds__preci', 'prec24.val_tot'),
        ('ds__t200', 'tmxgg.val_md'),
        ('ds__bagna', 'bagna.val_md'),
        ('ds__elio', 'elio.val_md'),
        ('ds__press', 'press.val_md'),
        ('ds__urel', 'ur.val_md'),
        ('ds__radglob', 'radglob.val_md'),
        ('ds__vnt10', 'vntmd.ff'),
    ]
    if only_tables is not None:
        tables = [t for t in tables if t[0] in only_tables]
    for table_name, master_field in tables:
        logger.info('* start working on table %s' % table_name)
        logger.info(' selecting data')
        sql = """SELECT idgruppo, data_i::varchar, %s.%s.* 
                 FROM %s.%s JOIN %s.%s ON (id_staz=cod_staz)
                 ORDER BY (idgruppo, data_i, progstazione)""" \
              % (startschema, table_name, gruppi_tschema, gruppi_tname, startschema, table_name)
        results = conn.execute(sql)
        inserted = 0
        logger.info(' start merge&insert')
        cols = db_utils.get_table_columns(table_name, targetschema)
        fields = expand_fields(cols)
        data = []
        for group_attrs, group_records in itertools.groupby(results, group_funct):
            groupid, data_i = group_attrs
            main_station = group2mainstation[groupid]
            expanded_group_records = [expand_record(dict(r)) for r in group_records]
            main_record = choose_main_record(expanded_group_records, master_field)
            if main_record:
                del main_record['idgruppo']
                main_record['cod_staz'] = main_station
                main_record['data_i'] = data_i
                main_record = {k: v for k, v in main_record.items() if v not in (None, 'NULL')}
                data.append(main_record)
                inserted += 1
                if divmod(inserted, 10000)[1] == 0:  # flush in blocks of 10000
                    sql = create_insert(table_name, targetschema, fields, data)
                    conn.execute(sql)
                    data = []
        if data:
            sql = create_insert(table_name, targetschema, fields, data)
            conn.execute(sql)
        logger.info('inserted %s records' % inserted)


def sync_flags(conn, flags=(-9, 5), sourceschema='dailypdbanpaclima',
               targetschema='dailypdbanpacarica', logger=None):
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    # PRECI
    logger.info('querying source table %s.ds__preci for flags %r' % (sourceschema, flags))
    sql_fields = "cod_stazprinc, data_i, (prec24).val_tot, ((prec24).flag).wht"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=None, schema=sourceschema,
        include_flag_values=(-9, 5), exclude_null=True, no_order=True)
    prec_flag_map = db_utils.create_flag_map(prec_records, flag_index=3)

    logger.info('querying table %s.ds__preci to be updated' % targetschema)
    sql_fields = "cod_staz, data_i, (prec24).val_tot, ((prec24).flag).wht"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=None, schema=targetschema, exclude_null=True,
        no_order=True)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map)
    logger.info('update flags of destination table %s.ds__preci' % targetschema)
    update_prec_flags(conn, prec_records, schema=targetschema, logger=logger)

    # TMAX
    logger.info('querying source table %s.ds__t200 (TMAX) for flags %r' % (sourceschema, flags))
    sql_fields = "cod_stazprinc, data_i, (tmxgg).val_md, ((tmxgg).flag).wht"
    tmax_records = querying.select_temp_records(
        conn, fields=['tmxgg'], sql_fields=sql_fields, stations_ids=None, schema=sourceschema,
        include_flag_values=(-9, 5), exclude_null=True, no_order=True)
    tmax_flag_map = db_utils.create_flag_map(tmax_records, flag_index=3)

    logger.info('querying table %s.ds__t200 (TMAX) to be updated' % targetschema)
    sql_fields = "cod_staz, data_i, (tmxgg).val_md, ((tmxgg).flag).wht"
    tmax_records = querying.select_temp_records(
        conn, fields=['tmxgg'], sql_fields=sql_fields, stations_ids=None, schema=targetschema,
        exclude_null=True, no_order=True)
    tmax_records = db_utils.force_flags(tmax_records, tmax_flag_map)
    logger.info('update flags of table %s.ds__t200 (TMAX)' % targetschema)
    update_temp_flags(conn, tmax_records, schema=targetschema, db_field='tmxgg', logger=logger)

    # TMIN
    logger.info('querying source table %s.ds__t200 (TMIN) for flags %r' % (sourceschema, flags))
    sql_fields = "cod_stazprinc, data_i, (tmngg).val_md, ((tmngg).flag).wht"
    tmin_records = querying.select_temp_records(
        conn, fields=['tmngg'], sql_fields=sql_fields, stations_ids=None, schema=sourceschema,
        include_flag_values=(-9, 5), exclude_null=True, no_order=True)
    tmin_flag_map = db_utils.create_flag_map(tmin_records, flag_index=3)

    logger.info('querying table %s.ds__t200 (TMIN) to be updated' % targetschema)
    sql_fields = "cod_staz, data_i, (tmngg).val_md, ((tmngg).flag).wht"
    tmin_records = querying.select_temp_records(
        conn, fields=['tmngg'], sql_fields=sql_fields, stations_ids=None, schema=targetschema,
        exclude_null=True, no_order=True)
    tmin_records = db_utils.force_flags(tmin_records, tmin_flag_map)
    logger.info('update flags of table %s.ds__t200 (TMIN)' % targetschema)
    update_temp_flags(conn, tmin_records, schema=targetschema, db_field='tmngg', logger=logger)