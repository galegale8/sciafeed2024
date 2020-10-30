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
    sql_fields = "cod_staz, data_i, (prec24).val_tot, 1 as flag"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=stations_ids, schema=schema,
        exclude_flag_interval=(-9, 0), exclude_null=True)
    sql5_fields = "cod_staz, data_i, (prec24).val_tot, ((prec24).flag).wht"
    prec_records5 = querying.select_prec_records(
        conn, sql_fields=sql5_fields, stations_ids=stations_ids, schema=schema,
        include_flag_values=(5, ), exclude_null=True)
    prec_flag_map5 = db_utils.create_flag_map(prec_records5)

    logger.info('* initial query to get records of temperature...')
    sql_fields = "cod_staz, data_i, (tmxgg).val_md, 1 as flag_tmxgg, " \
                 "(tmngg).val_md, 1 as flag_tmngg, (tmdgg).val_md, 1 as flag_tmdgg"
    temp_records = querying.select_temp_records(
        conn, fields=['tmxgg', 'tmngg'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0), exclude_null=False)

    tmax_records5 = querying.select_temp_records(
        conn, fields=['tmxgg'], sql_fields="cod_staz, data_i, (tmxgg).val_md, ((tmxgg).flag).wht",
        stations_ids=stations_ids, schema=schema, include_flag_values=(5, ), exclude_null=True)
    tmax_flag_map5 = db_utils.create_flag_map(tmax_records5)
    tmin_records5 = querying.select_temp_records(
        conn, fields=['tmngg'], sql_fields="cod_staz, data_i, (tmngg).val_md, ((tmngg).flag).wht",
        stations_ids=stations_ids, schema=schema, include_flag_values=(5, ), exclude_null=True)
    tmin_flag_map5 = db_utils.create_flag_map(tmin_records5)

    logger.info("== STARTING CHECK CHAIN ==")
    logger.info("* 'controllo valori ripetuti = 0' for variable PREC")
    prec_records = checks.check1(prec_records, logger=logger)
    logger.info("forcing flags of prec...")
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)

    logger.info("* 'controllo valori ripetuti' for variable PREC")
    prec_records = checks.check2(prec_records, exclude_values=(0, None), logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)
    logger.info("* 'controllo valori ripetuti' for variable Tmax")
    temp_records = checks.check2(temp_records, exclude_values=(None, ), logger=logger)
    logger.info("forcing flags of tmax...")
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo valori ripetuti' for variable Tmin")
    temp_records = checks.check2(temp_records, exclude_values=(None, ), val_index=4, logger=logger)
    logger.info("forcing flags of tmin...")
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo mesi duplicati (stesso anno)' for variable PREC")
    prec_records = checks.check3(prec_records, min_not_null=5, logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)
    logger.info("* 'controllo mesi duplicati (stesso anno)' for variable Tmax")
    temp_records = checks.check3(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo mesi duplicati (stesso anno)' for variable Tmin")
    temp_records = checks.check3(temp_records, val_index=4, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable PREC")
    prec_records = checks.check4(prec_records, min_not_null=5, logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)
    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable Tmax")
    temp_records = checks.check4(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable Tmin")
    temp_records = checks.check4(temp_records, val_index=4, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* controllo TMAX=TMIN")
    temp_records = checks.check5(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* controllo TMAX=TMIN=0")
    temp_records = checks.check6(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo world excedence' for PREC")
    prec_records = checks.check7(prec_records, max_threshold=800, logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)
    logger.info("* 'controllo world excedence' for Tmax")
    temp_records = checks.check7(temp_records, min_threshold=-30, max_threshold=50, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo world excedence' for Tmin")
    temp_records = checks.check7(temp_records, min_threshold=-40, max_threshold=40,
                                 val_index=4, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info('* controllo gap checks  precipitazione')
    prec_records = checks.check8(prec_records, threshold=300, logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)
    logger.info("* 'controllo gap checks temperatura' for Tmax")
    temp_records = checks.check8(temp_records, threshold=10, split=True, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo gap checks temperatura' for Tmin")
    temp_records = checks.check8(temp_records, threshold=10, split=True, val_index=4,
                                 logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo z-score checks temperatura' for Tmax")
    temp_records = checks.check9(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo z-score checks temperatura' for Tmin")
    temp_records = checks.check9(temp_records, val_index=4, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo z-score checks precipitazione'")
    pos_temp_days, neg_temp_days = checks.split_days_by_average_temp(temp_records)
    prec_records = checks.check10(prec_records, pos_temp_days, logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)
    logger.info("* 'controllo z-score checks precipitazione ghiaccio'")
    prec_records = checks.check10(prec_records, neg_temp_days, times_perc=5, flag=-26,
                                  logger=logger)
    prec_records = db_utils.force_flags(prec_records, prec_flag_map5)

    logger.info("* 'controllo jump checks' for Tmax")
    temp_records = checks.check11(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    logger.info("* 'controllo jump checks' for Tmin")
    temp_records = checks.check11(temp_records, val_index=4, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo Tmax < Tmin'")
    temp_records = checks.check12(temp_records, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

    logger.info("* 'controllo dtr (diurnal temperature range)' for Tmax")
    operators = max, operator.ge
    temp_records = checks.check13(temp_records, operators, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)
    logger.info("* 'controllo dtr (diurnal temperature range)' for Tmin")
    operators = min, operator.le
    temp_records = checks.check13(temp_records, operators, logger=logger)
    temp_records = db_utils.force_flags(temp_records, tmax_flag_map5)
    temp_records = db_utils.force_flags(temp_records, tmin_flag_map5, flag_index=5)

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
    field_flag_maps = dict()
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
        sql_fields = "cod_staz, data_i, (%s).%s, 1 as flag" % (main_field, sub_field)
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
        if main_field not in field_flag_maps:
            sql5_fields = "cod_staz, data_i, (%s).%s, ((%s).flag).wht" \
                          % (main_field, sub_field, main_field)
            table_records5 = querying.select_records(
                conn, table, fields=[main_field], sql_fields=sql5_fields,
                stations_ids=stations_ids,  schema=schema, include_flag_values=(5, ),
                where_sql=where_sql)
            table_flag_map5 = db_utils.create_flag_map(table_records5)
            field_flag_maps[main_field] = table_flag_map5

        logger.info("* 'controllo world excedence' for %s" % main_field)
        table_records = checks.check7(
            table_records, min_threshold=-1, max_threshold=25, logger=logger)
        table_records = db_utils.force_flags(table_records, field_flag_maps[main_field])
        logger.info('* final set of flags on database for %s...' % table)
        if main_field == 'vntmd':
            upsert.update_vntmd_flags(conn, table_records, schema=schema)
        else:
            upsert.update_flags(conn, table_records, table, schema=schema, db_field=main_field)

    # some additional checks for series pressures: valori ripetuti + consistence
    logger.info("* initial query to get records for 'valori ripetuti' check PRESS...")
    pmedia_series = checks.check2(
        pmedia_series, len_threshold=10, exclude_values=(None, ), logger=logger)
    pmedia_series = db_utils.force_flags(pmedia_series, field_flag_maps['press'])
    logger.info('* final set of flags on database for PRESS valori ripetuti')
    upsert.update_flags(conn, pmedia_series, 'ds__press', schema=schema, db_field='press')
    logger.info('* initial query to get records of %s for consistence check PRESS...')
    sql_fields = "cod_staz, data_i,(press).val_mn,(press).val_md,(press).val_mx,((press).flag).wht"
    press_records = querying.select_records(
        conn, 'ds__press', fields=['press'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0))
    logger.info("* 'controllo consistenza PRESS'")
    press_records = checks.check_consistency(press_records, (2, 3, 4), 5, flag=-10, logger=logger)
    press_records = db_utils.force_flags(press_records, field_flag_maps['press'])
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
    ur_records = db_utils.force_flags(ur_records, field_flag_maps['ur'])
    logger.info('* final set of flags on database for UR consistence...')
    upsert.update_flags(conn, ur_records, 'ds__urel', schema=schema, db_field='ur', flag_index=5)

    # some additional checks for series wind: valori ripetuti + consistence
    logger.info("* check 'valori ripetuti' for FFmedia...")
    filter_funct = lambda r: r[2] > 2
    vntmd_series = checks.check2(
        vntmd_series, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    vntmd_series = db_utils.force_flags(vntmd_series, field_flag_maps['vntmd'])
    logger.info('* final set of flags on database for FFmedia valori ripetuti')
    upsert.update_vntmd_flags(conn, vntmd_series, schema=schema)

    logger.info("* check 'valori ripetuti' for FFmax...")
    filter_funct = lambda r: r[2] > 2
    vntmxgg_ff_series = checks.check2(
        vntmxgg_ff_series, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    vntmxgg_ff_series = db_utils.force_flags(vntmxgg_ff_series, field_flag_maps['vntmxgg'])
    logger.info('* final set of flags on database for FFmax valori ripetuti')
    upsert.update_flags(conn, vntmxgg_ff_series, table="ds__vnt10", schema=schema,
                        db_field='vntmxgg')

    logger.info("* check 'valori ripetuti' for DD...")
    filter_funct = lambda r: r[2] > 0.5
    vntmxgg_dd_series = checks.check2(
        vntmxgg_dd_series, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    vntmxgg_dd_series = db_utils.force_flags(vntmxgg_dd_series, field_flag_maps['vntmxgg'])
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
    wind_records = db_utils.force_flags(wind_records, field_flag_maps['vntmd'], flag_index=5)
    wind_records = db_utils.force_flags(wind_records, field_flag_maps['vntmxgg'])
    logger.info('* final set of flags on database for WIND consistence...')
    upsert.update_vntmd_flags(conn, wind_records, schema=schema, flag_index=5, logger=logger)
    upsert.update_flags(
        conn, wind_records, 'ds__vnt10', schema=schema, db_field='vntmxgg', flag_index=3)

    logger.info('== End process ==')
