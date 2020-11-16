"""
This module contains functions and utilities that update the SCIA database
"""
import functools
import itertools
import logging
import time
import traceback

from sqlalchemy import MetaData, Table

from sciafeed import LOG_NAME
from sciafeed import db_utils
from sciafeed import export
from sciafeed import querying


field2class_map = {
    'prs_t200mx': 'pers_tmx_obj',
    'prs_t200mn': 'pers_tmn_obj',
    'ifs': 'nxxx_obj',
    'ifu': 'nxxx_obj',
    'ics': 'nxxx_obj',
    'icu': 'nxxx_obj',
    'sharl': 'bio_idx_obj',
    'ifu1': 'bio_idx_obj',
    'etp': 'stat0_obj',
    'bagna': 'bagna1_obj',
    'elio': 'stat2_obj',
    'prec24': 'prec24_obj',
    'cl_prec24': 'classi_prec_obj',
    'prec01': 'prec_obj',
    'prec06': 'prec_obj',
    'cl_prec06': 'classi_prec_obj',
    'prec12': 'prec_obj',
    'cl_prec12': 'classi_prec_obj',
    'ggneve': 'nxxx_obj',
    'storm': 'nxxx_obj',
    'ggstorm': 'nxxx_obj',
    'press': 'stat0_obj',
    'radglob': 'stat0_obj',
    'tmxgg': 'estremi_t_obj',
    'cl_tmxgg': 'classi_tmx_obj',
    'tmngg': 'estremi_t_obj',
    'cl_tmngg': 'classi_tmn_obj',
    'tmdgg': 'stat1_obj',
    'tmdgg1': 'stat1_obj',
    'deltagg': 'stat0_obj',
    'day_gelo': 'nxxx_obj',
    'cl_tist': 'classi_tist_obj',
    't00': 'stat0_obj',
    't01': 'stat0_obj',
    't02': 'stat0_obj',
    't03': 'stat0_obj',
    't04': 'stat0_obj',
    't05': 'stat0_obj',
    't06': 'stat0_obj',
    't07': 'stat0_obj',
    't08': 'stat0_obj',
    't09': 'stat0_obj',
    't10': 'stat0_obj',
    't11': 'stat0_obj',
    't12': 'stat0_obj',
    't13': 'stat0_obj',
    't14': 'stat0_obj',
    't15': 'stat0_obj',
    't16': 'stat0_obj',
    't17': 'stat0_obj',
    't18': 'stat0_obj',
    't19': 'stat0_obj',
    't20': 'stat0_obj',
    't21': 'stat0_obj',
    't22': 'stat0_obj',
    't23': 'stat0_obj',
    'ur': 'stat01_obj',
    'ur00': 'stat1_obj',
    'ur01': 'stat1_obj',
    'ur02': 'stat1_obj',
    'ur03': 'stat1_obj',
    'ur04': 'stat1_obj',
    'ur05': 'stat1_obj',
    'ur06': 'stat1_obj',
    'ur07': 'stat1_obj',
    'ur08': 'stat1_obj',
    'ur09': 'stat1_obj',
    'ur10': 'stat1_obj',
    'ur11': 'stat1_obj',
    'ur12': 'stat1_obj',
    'ur13': 'stat1_obj',
    'ur14': 'stat1_obj',
    'ur15': 'stat1_obj',
    'ur16': 'stat1_obj',
    'ur17': 'stat1_obj',
    'ur18': 'stat1_obj',
    'ur19': 'stat1_obj',
    'ur20': 'stat1_obj',
    'ur21': 'stat1_obj',
    'ur22': 'stat1_obj',
    'ur23': 'stat1_obj',
    'cl_ur06': 'classi_ur_obj',
    'cl_ur12': 'classi_ur_obj',
    'vntmxgg': 'vntmxgg_obj',
    'vnt': 'vnt16_obj',
    'prs_ff': 'prs_ff_obj',
    'prs_dd': 'prs_dd_obj',
    'vntmd': 'vntmd_obj',
    'prs_prec': 'pers_prec_obj',
    'grgg': 'grgg_obj',
    'deltaidro': 'stat0_obj',
}
class2subfields_map = {
    'pers_tmx_obj': ['flag.ndati', 'flag.wht', 'num01', 'data01_ini', 'num02', 'data02_ini',
                     'num03', 'data03_ini', 'num04', 'data04_ini', 'num05', 'data05_ini', 'num06',
                     'data06_ini', 'num07', 'data07_ini', 'num08', 'data08_ini', 'num09',
                     'data09_ini', 'num10', 'data10_ini', 'num11', 'data11_ini'],
    'pers_tmn_obj': ['flag.ndati', 'flag.wht', 'num01', 'data01_ini', 'num02', 'data02_ini',
                     'num03', 'data03_ini', 'num04', 'data04_ini', 'num05', 'data05_ini', 'num06',
                     'data06_ini', 'num07', 'data07_ini', 'num08', 'data08_ini', 'num09',
                     'data09_ini'],
    'bio_idx_obj': ['flag.ndati', 'flag.wht', 'num_01', 'num_02', 'num_03'],
    'pers_prec_obj': [
        'flag.ndati', 'flag.wht',
        'ndry_01', 'datadry_01', 'ndry_02', 'datadry_02', 'ndry_03', 'datadry_03',
        'nwet_01', 'totwet_01', 'datawet_01', 'nwet_02', 'totwet_02', 'datawet_02',
        'nwet_03', 'totwet_03', 'datawet_03',
        'nsnow_01', 'totsnow_01', 'datasnow_01', 'nsnow_02', 'totsnow_02', 'datasnow_02',
        'nsnow_03', 'totsnow_03', 'datasnow_03',
    ],
    'grgg_obj': [
        'flag.ndati', 'flag.wht', 'tot00', 'tot05', 'tot10', 'tot15', 'tot21',
    ],
    'bagna1_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_mx', 'val_mn', 'val_tot'],
    'stat2_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_mx'],
    'prec_obj': ['flag.ndati', 'flag.wht', 'val_mx', 'data_mx'],
    'prec24_obj':
        ['flag.ndati', 'flag.wht', 'val_tot', 'val_mx', 'data_mx'],
    'classi_prec_obj':
        ['dry', 'wet_01', 'wet_02', 'wet_03', 'wet_04', 'wet_05'],
    'nxxx_obj': ['flag.ndati', 'flag.wht', 'num'],
    'stat0_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_mx', 'val_mn'],
    'estremi_t_obj':
        ['flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'val_x', 'data_x'],
    'classi_tmx_obj':
        ['cl_01', 'cl_02', 'cl_03', 'cl_04', 'cl_05', 'cl_06', 'cl_07',
         'cl_08', 'cl_09', 'cl_10', 'cl_11'],
    'classi_tmn_obj':
        ['cl_01', 'cl_02', 'cl_03', 'cl_04', 'cl_05', 'cl_06', 'cl_07', 'cl_08', 'cl_09'],
    'stat1_obj': ['flag.ndati', 'flag.wht', 'val_md', 'val_vr'],
    'classi_tist_obj': [
        'flag.ndati', 'flag.wht', 'cl_01', 'cl_02', 'cl_03', 'cl_04', 'cl_05', 'cl_06',
        'cl_07', 'cl_08', 'cl_09', 'cl_10', 'cl_11', 'cl_12', 'cl_13', 'cl_14'],
    'stat01_obj': [
        'flag.ndati', 'flag.wht', 'val_md', 'val_vr', 'flag1.ndati', 'flag1.wht',
        'val_mx', 'val_mn'],
    'classi_ur_obj': ['cl_01', 'cl_02', 'cl_03', 'cl_04'],
    'vntmxgg_obj': ['flag.ndati', 'flag.wht', 'ff', 'dd'],
    'vnt16_obj': [
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
    'prs_ff_obj': ['n0', 'n1', 'n2', 'n3', 'n4'],
    'prs_dd_obj': [
        'n01', 'ff01', 'n02', 'ff02', 'n03', 'ff03', 'n04', 'ff04',
        'n05', 'ff05', 'n06', 'ff06', 'n07', 'ff07', 'n08', 'ff08',
        'n09', 'ff09', 'n10', 'ff10', 'n11', 'ff11', 'n12', 'ff12',
        'n13', 'ff13', 'n14', 'ff14', 'n15', 'ff15', 'n16', 'ff16'],
    'vntmd_obj': ['flag.ndati', 'flag.wht', 'ff']
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
    try:
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
    except:
        error = traceback.format_exc()
        msgs.append(error)
        return msgs, 0, 0
    num_inserted_stations = len(new_stations)
    msgs.append('inserted %i new stations' % num_inserted_stations)
    msgs.append('updated %i new stations' % num_updated_stations)
    return msgs, num_inserted_stations, num_updated_stations


def update_prec_flags(conn, records, schema='dailypdbanpacarica', logger=None):
    """
    Set the flag for each record of the `records` iterable for the field prec24
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
    if not list(records):
        return 0
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
    num_of_updates = 0
    try:
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
            WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i 
            AND ((t.prec24).flag).wht <> u.flag
        """ % (schema, tmp_table_name)
        result = conn.execute(update_sql)
        num_of_updates = result.rowcount
        logger.info('update completed: %s flags updated' % num_of_updates)
    except:
        logger.error('update not completed: something went wrong')
    finally:
        post_cmd = 'DROP TABLE %s' % tmp_table_name
        conn.execute(post_cmd)
        logger.debug('temp folder removed')
    return num_of_updates


def update_vntmd_flags(conn, records, schema='dailypdbanpacarica', flag_index=3, logger=None):
    """
    Set the flag for each record of the `records` iterable for the field vntmd
    of the table dailypdbanpacarica.ds__vnt10.
    It assumes each record has attributes data_i and cod_staz

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param schema: database schema to use
    :param flag_index: index of the flag in the input records
    :param logger: logging object where to report actions

    :return number of updates
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    if not list(records):
        return 0
    logger.debug('start db update of PREC flags')
    tmp_table_name = "updates_vnt%s" % round(time.time())
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
    num_of_updates = 0
    try:
        data = [{'cod_staz': r[0], 'data_i': r[1], 'flag': r[flag_index]} for r in records]
        conn.execute(table_obj.insert(), data)
        logger.debug('filled temp folder')
        update_sql = """
            UPDATE %s.ds__vnt10 t SET (
                vntmxgg.flag.wht,
                vnt.flag.wht
                ) = (u.flag, u.flag)
            FROM %s u
            WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i 
            AND ((t.vntmxgg).flag).wht <> u.flag
        """ % (schema, tmp_table_name)
        result = conn.execute(update_sql)
        num_of_updates = result.rowcount
        logger.info('update completed: %s flags updated' % num_of_updates)
    except:
        logger.exception('update not completed: something went wrong')
    finally:
        post_cmd = 'DROP TABLE %s' % tmp_table_name
        conn.execute(post_cmd)
        logger.debug('temp folder removed')
    return num_of_updates


def update_flags(conn, records, table, schema='dailypdbanpacarica', db_field='tmxgg', flag_index=3,
                 logger=None):
    """
    Set the flag to `set_flag` for each record of the `records` iterable for the field `db_field`
    of the table with name `table` of the schema `schema`.
    It assumes each record has attributes data_i and cod_staz.

    :param conn: db connection object
    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param table: db table name to use
    :param schema: database schema to use
    :param db_field: name of the database field related to the flag
    :param flag_index: index of the flag value in each record
    :param logger: logging object where to report actions
    :return number of updates
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    if not list(records):
        return 0
    logger.debug('start db update of flags (%s)' % db_field)
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
    num_of_updates = 0
    try:
        data = [{'id_record': i, 'cod_staz': r[0], 'data_i': r[1], 'flag': r[flag_index]}
                for i, r in enumerate(records, 1)]
        conn.execute(table_obj.insert(), data)
        logger.debug('filled temp folder')
        update_sql = """
            UPDATE %s.%s t SET %s.flag.wht = u.flag
            FROM %s u
            WHERE t.cod_staz = u.cod_staz AND t.data_i = u.data_i AND ((t.%s).flag).wht <> u.flag
        """ % (schema, table, db_field, tmp_table_name, db_field)
        result = conn.execute(update_sql)
        num_of_updates = result.rowcount
        logger.info('update completed: %s flags updated' % num_of_updates)
    except:
        logger.exception('update not completed: something went wrong')
    finally:
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
            if value is None:
                new_values = ['NULL'] * len(subfields)
            else:
                new_values = [r.strip() not in ('', 'None') and r.strip() or 'NULL'
                              for r in value.replace('(', '').replace(')', '').replace("'", "").
                              replace('"', '').replace('[', '').replace(']', '').split(',')]
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
    # NOTE: fields not included in data[i] keys will be set to null for upsert policy
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


def upsert_items(conn, items, policy, schema, table_name, logger=None, find_cod_staz=False):
    """
    Insert (or update if not exists) items into the database.
    It assumes all items are dictionaries with the same keys. Object fields must be represented
    as strings.
    The 'upsert' policy update to None also the empty/null values, if they are present
    in the item keys.
    It is used after loading items from CSV files' rows (values are always strings).
    Rows of same (station, day) are merged.

    :param conn: db connection object
    :param items: list of dictionaries to be upserted. They must be the same for keys
    :param policy: 'onlyinsert' or 'upsert'
    :param schema: database schema to use
    :param table_name: name of the table
    :param logger: logging object where to report actions
    :param find_cod_staz: if True, assumes cod_staz in items is ('cod_utente--cod_gruppo')

    :return number of updates
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    if not items:
        logger.warning('no items to upsert')
        return 0
    meta = MetaData()
    anag_table = Table('anag__stazioni', meta, autoload=True, autoload_with=conn.engine,
                       schema='dailypdbadmclima')
    items.sort(key=lambda x: (x['cod_staz'], x['data_i']))
    group_by_station = lambda x: x['cod_staz']
    group_by_date = lambda x: x['data_i']
    upserted = 0
    # cols = db_utils.get_table_columns(table_name, schema)
    cols = list(items[0].keys())
    fields = expand_fields(cols)

    for station, station_records in itertools.groupby(items, group_by_station):
        data = []
        cod_staz = station
        if find_cod_staz:
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
            # this one so that empty values are empty strings in the sql
            record = {
                k: v for k, v in record.items()
                if v not in (None, 'NULL') and list(filter(lambda r: r.isdigit(), str(v)))
            }
            data.append(record)
            upserted += 1
        sql = create_upsert(table_name, schema, fields, data, policy)
        if sql:
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


def load_unique_data_table(dburi, table_name, master_field, startschema, targetschema,
                           gruppi_tschema, gruppi_tname, group2mainstation, logger_name):
    # note: master_field is always validated by "%s.flag.wht" % master_field.rsplit('.', 1)[0]
    # (if present), otherwise the first record that has a not not value for master_field

    # engine_multiprocessing = create_engine(db_utils.DEFAULT_DB_URI, pool=db_utils.mypool)
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    logger = logging.getLogger(logger_name)
    group_funct = lambda r: (r[0], r[1])  # idgruppo, data_i

    logger.info('* start working on table %s' % table_name)
    logger.info(' selecting data on table %s' % table_name)
    sql = """SELECT idgruppo, data_i, %s.%s.* 
             FROM %s.%s JOIN %s.%s ON (id_staz=cod_staz)
             ORDER BY (idgruppo, data_i, progstazione)""" \
          % (startschema, table_name, gruppi_tschema, gruppi_tname, startschema, table_name)
    results = conn.execute(sql)
    inserted = 0
    logger.info(' start merge&insert on table %s' % table_name)
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
            main_record = {k: v for k, v in main_record.items() if v not in (None, 'NULL')}
            data.append(main_record)
            inserted += 1
            if divmod(inserted, 10000)[1] == 0:  # flush in blocks of 100000
                sql = create_insert(table_name, targetschema, fields, data)
                conn.execute(sql)
                data = []
    if data:
        sql = create_insert(table_name, targetschema, fields, data)
        conn.execute(sql)
    logger.info('inserted %s records on table %s' % (inserted, table_name))
    conn.close()


def load_unique_data(dburi, startschema, targetschema, logger=None, only_tables=None):
    """
    Load data from `startschema` to `targetschema`, merging data from duplicate stations.

    :param dburi: db connection string
    :param startschema: db schema where to find input data tables
    :param targetschema: db schema where to put merged records
    :param logger: logger object for reporting
    :param only_tables: if not None, list of names of data tables to work on
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)

    logger.info("loading tabgruppistazioni")
    gruppi_tname = 'tabgruppistazioni'
    gruppi_tschema = 'dailypdbanpacarica'
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    group2mainstation = querying.load_main_station_groups(
        conn, gruppi_tname, gruppi_tschema)
    conn.close()
    tables = [
        ('ds__preci', 'prec24.val_tot'),
        ('ds__t200', 'tmxgg.val_md'),
        ('ds__bagna', 'bagna.val_md'),
        ('ds__elio', 'elio.val_md'),
        ('ds__press', 'press.val_md'),
        ('ds__urel', 'ur.val_md'),
        ('ds__radglob', 'radglob.val_md'),
        ('ds__vnt10', 'vntmd.ff'),
        ('ds__delta_idro', 'deltaidro.val_md'),
        ('ds__etp', 'etp.val_md'),
        ('ds__grgg', 'grgg.tot00'),
    ]
    if only_tables is not None:
        tables = [t for t in tables if t[0] in only_tables]

    # single process solution
    logger_name = logger.name
    for table_name, master_field in tables:
        load_unique_data_table(
            dburi, table_name, master_field, startschema, targetschema, gruppi_tschema,
            gruppi_tname, group2mainstation, logger_name)

    # multiprocessing
    # import multiprocessing as mp
    # pool = mp.Pool(mp.cpu_count())
    # logger_name = logger.name
    # results = pool.starmap(
    #     load_unique_data_table,
    #     [(
    #         dburi, table_name, master_field, startschema, targetschema,
    #         gruppi_tschema, gruppi_tname, group2mainstation, logger_name
    #     ) for table_name, master_field in tables]
    # )
    # pool.close()


def sync_flags(conn, flags=(-9, 5), sourceschema='dailypdbanpaclima',
               targetschema='dailypdbanpacarica', logger=None):
    """
    Transfert list of `flags` from the records of the db schema `sourceschema` to the
    corresponding reports of the db schema `targetchema`

    :param conn: db connection object
    :param flags: list of flags to query and set
    :param sourceschema: db schema where to find input data tables
    :param targetschema: db schema where to put output records
    :param logger: logger object for reporting
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    # PRECI
    logger.info('querying source table %s.ds__preci for flags %r' % (sourceschema, flags))
    sql_fields = "cod_stazprinc, data_i, (prec24).val_tot, ((prec24).flag).wht"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=None, schema=sourceschema,
        include_flag_values=flags, exclude_null=True, no_order=True)
    prec_flag_map = db_utils.create_flag_map(prec_records)

    logger.info('querying table %s.ds__preci to be updated' % targetschema)
    sql_fields = "cod_staz, data_i, (prec24).val_tot, ((prec24).flag).wht"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=None, schema=targetschema, exclude_null=True,
        no_order=True)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map)
    logger.info('update flags of destination table %s.ds__preci' % targetschema)
    flag_records = [r for r in prec_records if r[3] in flags]
    update_prec_flags(conn, flag_records, schema=targetschema, logger=logger)

    # OTHER TABLES
    for table, main_field, sub_field in [
        # sub_field is the main subfield for condition of not null
        ('ds__t200', 'tmxgg', 'val_md'),
        ('ds__t200', 'tmngg', 'val_md'),
        ('ds__t200', 'tmdgg', 'val_md'),
        ('ds__bagna', 'bagna', 'val_md'),
        ('ds__elio', 'elio', 'val_md'),
        ('ds__radglob', 'radglob', 'val_md'),
        ('ds__urel', 'ur', 'val_md'),
        ('ds__urel', 'ur', 'val_mx'),
        ('ds__urel', 'ur', 'val_mn'),
        ('ds__vnt10', 'vntmd', 'ff'),
        ('ds__vnt10', 'vntmxgg', 'ff'),
        ('ds__vnt10', 'vntmxgg', 'dd'),
    ]:
        logger.info('querying source table %s.%s for flags %r (field %s.%s)'
                    % (sourceschema, table, flags, main_field, sub_field))
        sql_fields = "cod_stazprinc, data_i, (%s).%s, ((%s).flag).wht" \
                     % (main_field, sub_field, main_field)
        where_sql = '(%s).%s IS NOT NULL' % (main_field, sub_field)
        table_records = querying.select_records(
            conn, table, fields=[main_field], sql_fields=sql_fields, stations_ids=None,
            schema=sourceschema, include_flag_values=flags, where_sql=where_sql, no_order=True)
        table_flag_map = db_utils.create_flag_map(table_records)

        logger.info('querying table %s.%s to be updated' % (targetschema, table))
        sql_fields = "cod_staz, data_i, (%s).%s, ((%s).flag).wht" \
                     % (main_field, sub_field, main_field)
        table_records = querying.select_records(
            conn, table, fields=[main_field], sql_fields=sql_fields, stations_ids=None,
            schema=targetschema, where_sql=where_sql, no_order=True)
        table_records = db_utils.force_flags(table_records, table_flag_map)
        logger.info('update flags of table %s.%s (field %s.%s)'
                    % (targetschema, table, main_field, sub_field))
        flag_records = [r for r in table_records if r[3] in flags]
        update_flags(conn, flag_records, table, schema=targetschema, db_field=main_field,
                     logger=logger)
