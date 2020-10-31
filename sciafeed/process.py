"""
This module contains functions and utilities that involve more components of sciafeed.
"""

import logging
import operator
from os import listdir
from os.path import isfile, join, splitext

from sciafeed import LOG_NAME
from sciafeed import checks
from sciafeed import compute
from sciafeed import db_utils
from sciafeed import export
from sciafeed import parsing
from sciafeed import querying
from sciafeed import utils
from sciafeed import upsert


def make_report(in_filepath, report_filepath=None, outdata_filepath=None, do_checks=True,
                parameters_filepath=None, limiting_params=None):
    """
    Read a file located at `in_filepath` and generate a report on the parsing.
    If `report_filepath` is defined, the report string is written on a file.
    If the path `outdata_filepath` is defined, a file with the data parsed is created at the path.
    Return the list of report strings and the data parsed.

    :param in_filepath: input file
    :param report_filepath: path of the output report
    :param outdata_filepath: path of the output file containing data
    :param do_checks: True if must do checks, False otherwise
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (report_strings, data_parsed)
    """
    format_label, format_module = parsing.guess_format(in_filepath)
    if not format_module:
        msgs, data_parsed = ["file %r has unknown format" % in_filepath, ''], None
        return msgs, data_parsed
    if not parameters_filepath:
        parameters_filepath = getattr(format_module, 'PARAMETERS_FILEPATH')
    if limiting_params is None:
        limiting_params = getattr(format_module, 'LIMITING_PARAMETERS')
    msgs = []
    msg = "START OF ANALYSIS OF %s FILE %r" % (format_label, in_filepath)
    msgs.append(msg)
    msgs.append('=' * len(msg))
    msgs.append('')

    parse_f = getattr(format_module, 'parse')
    load_parameter_thresholds_f = getattr(format_module, 'load_parameter_thresholds')
    par_thresholds = load_parameter_thresholds_f(parameters_filepath)
    # 1. parsing
    data, err_msgs = parse_f(in_filepath, parameters_filepath)
    if do_checks:
        # 2. weak climatologic check
        wcc_err_msgs, data = checks.data_weak_climatologic_check(data, par_thresholds)
        # 3. internal consistence check
        icc_err_msgs, data = checks.data_internal_consistence_check(data, limiting_params)
        err_msgs += wcc_err_msgs + icc_err_msgs

    if not err_msgs:
        msg = "No errors found"
        msgs.append(msg)
    else:
        for row_index, err_msg in err_msgs:
            msgs.append("Row %s: %s" % (row_index, err_msg))

    if outdata_filepath:
        msgs.append('')
        export.export2csv(data, outdata_filepath)
        msg = "Data saved on file %r" % outdata_filepath
        msgs.append(msg)

    msgs.append('')
    msg = "END OF ANALYSIS OF %s FILE" % format_label
    msgs.append(msg)
    msgs.append('=' * len(msg))
    msgs.append('')

    if report_filepath:
        with open(report_filepath, 'a') as fp:
            for msg in msgs:
                fp.write(msg + '\n')

    return msgs, data


def compute_daily_indicators(data_folder, indicators_folder=None, report_path=None):
    """
    Read each file located inside `data_folder` and generate indicators
    and a report of the processing.
    If the path `indicators_folder` is defined, a file with the indicators
    is created at the path.
    Return the the report strings (list) and the computed indicators (dictionary).

    :param data_folder: folder path containing input data
    :param indicators_folder: path of the output data
    :param report_path: path of the output file containing data
    :return: (report_strings, computed_indicators)
    """
    msgs = []
    computed_indicators = dict()
    writers = utils.open_csv_writers(indicators_folder, compute.INDICATORS_TABLES)
    for file_name in listdir(data_folder):
        csv_path = join(data_folder, file_name)
        if not isfile(csv_path) or splitext(file_name.lower())[1] != '.csv':
            continue
        msg = "ANALYSIS OF FILE %r" % csv_path
        msgs.append(msg)
        # msgs.append('=' * len(msg))
        # msgs.append('')
        try:
            data = export.csv2data(csv_path)
        except:
            msg = 'CSV file %r not parsable' % csv_path
            msgs.append(msg)
            msgs.append('')
            continue

        comp_msgs, computed_indicators = compute.compute_and_store(
            data, writers, compute.INDICATORS_TABLES)
        msgs.extend(comp_msgs)
        # msgs.append('')
        # msg = "END OF ANALYSIS OF FILE"
        # msgs.append(msg)
        # msgs.append('=' * len(msg))
        # msgs.append('')

        if report_path:
            with open(report_path, 'a') as fp:
                for msg in msgs:
                    fp.write(msg + '\n')

    utils.close_csv_writers(writers)
    return msgs, computed_indicators


def check_chain(dburi, stations_ids=None, schema='dailypdbanpacarica', logger=None):
    """
    Start a chain of checks on records of the database from a set of monitoring stations selected.

    :param dburi: db connection URI
    :param stations_ids: primary keys of the stations (if None: no filtering by stations)
    :param schema: database schema to use
    :param logger: logging object where to report actions
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info('== Start process ==')
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()

    logger.info('* synchronization of flags +5 and -9 loading from schema dailypdbanpaclima...')
    upsert.sync_flags(conn, flags=(-9, 5), sourceschema='dailypdbanpaclima', targetschema=schema,
                      logger=logger)

    logger.info('* initial query to get records of PREC...')
    sql_fields = "cod_staz, data_i, (prec24).val_tot, " \
                 "case when ((prec24).flag).wht=5 then 5 else 1 end"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=stations_ids, schema=schema,
        exclude_flag_interval=(-9, 0), exclude_null=True)

    logger.info('* initial query to get records of temperature...')
    sql_fields = "cod_staz, data_i, " \
                 "(tmxgg).val_md, case when ((tmxgg).flag).wht=5 then 5 else 1 end, " \
                 "(tmngg).val_md, case when ((tmngg).flag).wht=5 then 5 else 1 end"
    temp_records = querying.select_temp_records(
        conn, fields=['tmxgg', 'tmngg'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0), exclude_null=False)

    logger.info("== STARTING CHECK CHAIN ==")
    logger.info("* 'controllo valori ripetuti = 0' for variable PREC")
    prec_records = checks.check1(prec_records, logger=logger)

    logger.info("* 'controllo valori ripetuti' for variable PREC")
    prec_records = checks.check2(prec_records, exclude_values=(0, None), logger=logger)
    logger.info("* 'controllo valori ripetuti' for variable Tmax")
    temp_records = checks.check2(temp_records, exclude_values=(None, ), logger=logger)
    logger.info("* 'controllo valori ripetuti' for variable Tmin")
    temp_records = checks.check2(temp_records, exclude_values=(None, ), val_index=4, logger=logger)

    logger.info("* 'controllo mesi duplicati (stesso anno)' for variable PREC")
    prec_records = checks.check3(prec_records, min_not_null=5, logger=logger)
    logger.info("* 'controllo mesi duplicati (stesso anno)' for variable Tmax")
    temp_records = checks.check3(temp_records, logger=logger)
    logger.info("* 'controllo mesi duplicati (stesso anno)' for variable Tmin")
    temp_records = checks.check3(temp_records, val_index=4, logger=logger)

    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable PREC")
    prec_records = checks.check4(prec_records, min_not_null=5, logger=logger)
    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable Tmax")
    temp_records = checks.check4(temp_records, logger=logger)
    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable Tmin")
    temp_records = checks.check4(temp_records, val_index=4, logger=logger)

    logger.info("* controllo TMAX=TMIN")
    temp_records = checks.check5(temp_records, logger=logger)

    logger.info("* controllo TMAX=TMIN=0")
    temp_records = checks.check6(temp_records, logger=logger)

    logger.info("* 'controllo world excedence' for PREC")
    prec_records = checks.check7(prec_records, max_threshold=800, logger=logger)
    logger.info("* 'controllo world excedence' for Tmax")
    temp_records = checks.check7(temp_records, min_threshold=-30, max_threshold=50, logger=logger)
    logger.info("* 'controllo world excedence' for Tmin")
    temp_records = checks.check7(temp_records, min_threshold=-40, max_threshold=40,
                                 val_index=4, logger=logger)

    logger.info('* controllo gap checks  precipitazione')
    prec_records = checks.check8(prec_records, threshold=300, logger=logger)
    logger.info("* 'controllo gap checks temperatura' for Tmax")
    temp_records = checks.check8(temp_records, threshold=10, split=True, logger=logger)
    logger.info("* 'controllo gap checks temperatura' for Tmin")
    temp_records = checks.check8(temp_records, threshold=10, split=True, val_index=4,
                                 logger=logger)

    logger.info("* 'controllo z-score checks temperatura' for Tmax")
    temp_records = checks.check9(temp_records, logger=logger)
    logger.info("* 'controllo z-score checks temperatura' for Tmin")
    temp_records = checks.check9(temp_records, val_index=4, logger=logger)

    logger.info("* 'controllo z-score checks precipitazione'")
    pos_temp_days, neg_temp_days = checks.split_days_by_average_temp(temp_records)
    prec_records = checks.check10(prec_records, pos_temp_days, logger=logger)
    logger.info("* 'controllo z-score checks precipitazione ghiaccio'")
    prec_records = checks.check10(prec_records, neg_temp_days, times_perc=5, flag=-26,
                                  logger=logger)

    logger.info("* 'controllo jump checks' for Tmax")
    temp_records = checks.check11(temp_records, logger=logger)
    logger.info("* 'controllo jump checks' for Tmin")
    temp_records = checks.check11(temp_records, val_index=4, logger=logger)

    logger.info("* 'controllo Tmax < Tmin'")
    temp_records = checks.check12(temp_records, logger=logger)

    logger.info("* 'controllo dtr (diurnal temperature range)' for Tmax")
    operators = max, operator.ge
    temp_records = checks.check13(temp_records, operators, logger=logger)
    logger.info("* 'controllo dtr (diurnal temperature range)' for Tmin")
    operators = min, operator.le
    temp_records = checks.check13(temp_records, operators, logger=logger)

    logger.info('* final set of flags on database for PREC and TEMP...')
    upsert.update_prec_flags(conn, prec_records, schema=schema)
    upsert.update_flags(conn, temp_records, 'ds__t200', schema=schema, db_field='tmxgg')
    upsert.update_flags(conn, temp_records, 'ds__t200', schema=schema, db_field='tmngg',
                        flag_index=5)

    # some checks of kind "check7" for other indicators
    pmedia_series = []
    vntmd_series = []
    vntmxgg_ff_series = []
    vntmxgg_dd_series = []
    for table, main_field, sub_field, min_threshold, max_threshold in [
        ('ds__bagna', 'bagna', 'val_md', -1, 25),
        ('ds__elio', 'elio', 'val_md', -1, 19),
        ('ds__radglob', 'radglob', 'val_md', -1, 601),
        ('ds__t200', 'tmdgg', 'val_md', -36, 46),
        ('ds__press', 'press', 'val_md', 959, 1061),
        ('ds__press', 'press', 'val_mx', 959, 1061),
        ('ds__press', 'press', 'val_mn', 959, 1061),
        ('ds__urel', 'ur', 'val_md', -1, 101),
        ('ds__urel', 'ur', 'val_mx', -1, 101),
        ('ds__urel', 'ur', 'val_mn', -1, 101),
        ('ds__vnt10', 'vntmd', 'ff', -1, 103),
        ('ds__vnt10', 'vntmxgg', 'ff', -1, 103),
        ('ds__vnt10', 'vntmxgg', 'dd', -1, 361),
    ]:
        logger.info('* initial query to get records of %s...' % table)
        sql_fields = "cod_staz, data_i, (%s).%s, case when ((%s).flag).wht=5 then 5 else 1 end" \
                     % (main_field, sub_field, main_field)
        where_sql = '(%s).%s IS NOT NULL' % (main_field, sub_field)
        table_records = querying.select_records(
            conn, table, fields=[main_field], sql_fields=sql_fields, stations_ids=stations_ids,
            schema=schema, exclude_flag_interval=(-9, 0), where_sql=where_sql)
        # saved some series to save time after
        if (table, main_field, sub_field) == ('ds__press', 'press', 'val_md'):
            pmedia_series = list(table_records)
        elif (table, main_field, sub_field) == ('ds__vnt10', 'vntmd', 'ff'):
            vntmd_series = list(table_records)
        elif (table, main_field, sub_field) == ('ds__vnt10', 'vntmxgg', 'ff'):
            vntmxgg_ff_series = list(table_records)
        elif (table, main_field, sub_field) == ('ds__vnt10', 'vntmxgg', 'ff'):
            vntmxgg_dd_series = list(table_records)

        logger.info("* 'controllo world excedence' for %s" % main_field)
        table_records = checks.check7(
            table_records, min_threshold=-1, max_threshold=25, logger=logger)
        logger.info('* final set of flags on database for %s...' % table)
        if main_field == 'vntmd':
            upsert.update_vntmd_flags(conn, table_records, schema=schema)
        else:
            upsert.update_flags(conn, table_records, table, schema=schema, db_field=main_field)

    # some additional checks for series pressures: valori ripetuti + consistence
    logger.info("* initial query to get records for 'valori ripetuti' check PRESS...")
    pmedia_series = checks.check2(
        pmedia_series, len_threshold=10, exclude_values=(None, ), logger=logger)
    logger.info('* final set of flags on database for PRESS valori ripetuti')
    upsert.update_flags(conn, pmedia_series, 'ds__press', schema=schema, db_field='press')
    logger.info('* initial query to get records of %s for consistence check PRESS...')
    sql_fields = "cod_staz, data_i,(press).val_mn,(press).val_md,(press).val_mx,((press).flag).wht"
    press_records = querying.select_records(
        conn, 'ds__press', fields=['press'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0))
    logger.info("* 'controllo consistenza PRESS'")
    press_records = checks.check_consistency(press_records, (2, 3, 4), 5, flag=-10, logger=logger)
    logger.info('* final set of flags on database for PRESS consistence...')
    upsert.update_flags(
        conn, press_records, 'ds__press', schema=schema, db_field='press', flag_index=5)

    # some additional checks for series umidity: consistence
    logger.info('* initial query to get records of %s for consistence check UR...')
    sql_fields = "cod_staz, data_i,(ur).val_mn,(ur).val_md,(ur).val_mx,((ur).flag).wht"
    ur_records = querying.select_records(
        conn, 'ds__urel', fields=['ur'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0))
    logger.info("* 'controllo consistenza UR'")
    ur_records = checks.check_consistency(ur_records, (2, 3, 4), 5, flag=-10, logger=logger)
    logger.info('* final set of flags on database for UR consistence...')
    upsert.update_flags(conn, ur_records, 'ds__urel', schema=schema, db_field='ur', flag_index=5)

    # some additional checks for series wind: valori ripetuti + consistence
    logger.info("* check 'valori ripetuti' for FFmedia...")
    filter_funct = lambda r: r[2] > 2
    vntmd_series = checks.check2(
        vntmd_series, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    logger.info('* final set of flags on database for FFmedia valori ripetuti')
    upsert.update_vntmd_flags(conn, vntmd_series, schema=schema)

    logger.info("* check 'valori ripetuti' for FFmax...")
    filter_funct = lambda r: r[2] > 2
    vntmxgg_ff_series = checks.check2(
        vntmxgg_ff_series, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    logger.info('* final set of flags on database for FFmax valori ripetuti')
    upsert.update_flags(conn, vntmxgg_ff_series, table="ds__vnt10", schema=schema,
                        db_field='vntmxgg')

    logger.info("* check 'valori ripetuti' for DD...")
    filter_funct = lambda r: r[2] > 0.5
    vntmxgg_dd_series = checks.check2(
        vntmxgg_dd_series, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    logger.info('* final set of flags on database for FFmax valori ripetuti')
    upsert.update_flags(conn, vntmxgg_dd_series, table='ds__vnt10', schema=schema,
                        db_field='vntmxgg')

    logger.info('* initial query to get records of %s for consistence check FF...')
    sql_fields = "cod_staz, data_i,(vntmxgg).ff,((vntmxgg).flag).wht,(vntmd).ff,((vntmd).flag).wht"
    wind_records = querying.select_records(
        conn, 'ds__vnt10', fields=['vntmxgg', 'vntmd'], sql_fields=sql_fields,
        stations_ids=stations_ids, schema=schema, exclude_flag_interval=(-9, 0))
    logger.info("* 'controllo consistenza WIND'")
    wind_records = checks.check12(wind_records, min_diff=0, logger=logger)
    logger.info('* final set of flags on database for WIND consistence...')
    upsert.update_vntmd_flags(conn, wind_records, schema=schema, flag_index=5, logger=logger)
    upsert.update_flags(
        conn, wind_records, 'ds__vnt10', schema=schema, db_field='vntmxgg', flag_index=3)

    logger.info('== End process ==')


def compute_daily_indicators2(conn, schema, logger):
    """
    Compute secondary indicators.

    :param conn: db connection object
    :param schema: db schema to consider
    :param logger: logger object for reporting
    :return:
    """
    logger.info('* querying ds__t200 for compute temperature indicators...')
    sql = """
    SELECT cod_staz, data_i, (tmxgg).val_md, (tmngg).val_md, (tmdgg).val_md, lat
    FROM %s.ds__t200 LEFT JOIN dailypdbadmclima.anag__stazioni ON cod_staz=id_staz
    WHERE (tmxgg).val_md IS NOT NULL AND ((tmxgg).flag).wht > 0 AND
    (tmngg).val_md IS NOT NULL AND ((tmngg).flag).wht > 0 """ % schema
    temp_records = map(list, conn.execute(sql))

    logger.info('* computing temperature indicators...')
    temp_items = []
    grgg_items = []
    etp_items = []
    for record in temp_records:
        # data_i_str = record[1].strftime('%Y-%m-%d 00:00:00')
        tmax, tmin, tmedia, lat = record[2:6]
        jd = int(record[1].strftime('%j'))
        deltagg = tmax - tmin
        tmdgg1 = deltagg / 2
        grgg = compute.compute_grgg(tmedia)
        etp = compute.compute_etp(deltagg, tmax, tmin, lat, jd)
        temp_item = {'data_i': record[1], 'cod_staz': record[0], 'tmdgg1': tmdgg1,
                     'deltagg': deltagg}
        temp_items.append(temp_item)
        grgg_item = {'data_i': record[1], 'cod_staz': record[0], 'grgg': grgg}
        grgg_items.append(grgg_item)
        etp_item = {'data_i': record[1], 'cod_staz': record[0], 'etp': etp}
        etp_items.append(etp_item)
    for table_name, fields, data in [
        ('ds__t200', ['data_i', 'cod_staz', 'tmdgg1', 'deltagg'], temp_items),
        ('ds__etp', ['data_i', 'cod_staz', 'etp'], etp_items),
        ('ds__grgg', ['data_i', 'cod_staz', 'grgg'], grgg_items),
    ]:
        sql = upsert.create_upsert(table_name, schema, fields, data, 'upsert')
        if sql:
            logger.info('updating temperature indicators on table %s.%s' % (schema, table_name))
            conn.execute(sql)
    logger.info('* computing bilancio idrico...')
    sql = """
    SELECT cod_staz, data_i, (prec24).val_tot, (etp).val_md
    FROM %s.ds__etp a JOIN %s.ds__preci b USING (cod_staz,data_i) 
    WHERE ((prec24).flag).wht > 0 AND ((etp).flag).wht > 0""" % (schema, schema)
    idro_records = map(list, conn.execute(sql))
    idro_items = []
    for idro_record in idro_records:
        prec24, etp = idro_record[2:4]
        deltaidro = compute.compute_deltaidro(prec24, etp)
        idro_item = {'data_i': idro_record[1], 'cod_staz': idro_record[0], 'deltaidro': deltaidro}
        idro_items.append(idro_item)
    for table_name, fields, data in [
        ('ds__delta_idro', ['data_i', 'cod_staz', 'deltaidro'], idro_items),
    ]:
        sql = upsert.create_upsert(table_name, schema, fields, data, 'upsert')
        if sql:
            logger.info('updating bilancio idrico on table %s.%s' % (schema, table_name))
            conn.execute(sql)

