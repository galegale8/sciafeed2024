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
from sciafeed import dma
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


def compute_daily_indicators(conn, data_folder, indicators_folder=None, logger=None):
    """
    Read each file located inside `data_folder` and generate indicators
    and a report of the processing.
    If the path `indicators_folder` is defined, a file with the indicators
    is created at the path.
    Return the the report strings (list) and the computed indicators (dictionary).

    :param conn: db connection object
    :param data_folder: folder path containing input data
    :param indicators_folder: path of the output data
    :param logger: logging object where to report actions
    :return: computed_indicators
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    computed_indicators = dict()
    writers = utils.open_csv_writers(indicators_folder, compute.INDICATORS_TABLES)
    block_data = []
    for i, file_name in enumerate(listdir(data_folder), 1):
        csv_path = join(data_folder, file_name)
        if not isfile(csv_path) or splitext(file_name.lower())[1] != '.csv':
            continue
        logger.info("reading data from %r" % csv_path)
        try:
            data = export.csv2data(csv_path)
        except:
            logger.error('CSV file %r not parsable' % csv_path)
            continue
        block_data.extend(data)
    logger.info("computing daily indicators...")
    if block_data:
        computed_indicators = compute.compute_and_store(
            conn, block_data, writers, compute.INDICATORS_TABLES, logger)
    utils.close_csv_writers(writers)
    return computed_indicators


def process_checks_preci(conn, stations_ids, schema, logger, temp_records=None):
    logger.info('== initial process chain for PRECI ==')

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, (prec24).val_tot, " \
                 "case when ((prec24).flag).wht=5 then 5 else 1 end"
    prec_records = querying.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=stations_ids, schema=schema,
        exclude_flag_interval=(-9, 0), exclude_null=True)

    if temp_records is None:
        logger.info('* get records of temperature...')
        sql_fields = "cod_staz, data_i, " \
                     "(tmxgg).val_md, case when ((tmxgg).flag).wht=5 then 5 else 1 end, " \
                     "(tmngg).val_md, case when ((tmngg).flag).wht=5 then 5 else 1 end, " \
                     "(tmdgg).val_md, case when ((tmdgg).flag).wht=5 then 5 else 1 end"
        temp_records = querying.select_temp_records(
            conn, fields=['tmxgg', 'tmngg', 'tmdgg'], sql_fields=sql_fields,
            stations_ids=stations_ids, schema=schema, exclude_null=False)
        temp_records = list(temp_records)
        for val_index in [2, 4, 6]:
            for temp_record in temp_records:
                if temp_record[val_index + 1] is None or temp_record[val_index+1] < -9:
                    temp_record[val_index+1] = 1
                elif -9 <= temp_record[val_index+1] <= 0:
                    # set None to values flagged as invalid (flag in [-9, 0])
                    temp_record[val_index] = None
                # else temp_record[3] > 0: flag 1 or 5 remains unset

    logger.info("* 'controllo valori ripetuti = 0'")
    prec_records = checks.check1(prec_records, logger=logger)
    logger.info("* 'controllo valori ripetuti'")
    prec_records = checks.check2(prec_records, exclude_values=(0, None), logger=logger)
    logger.info("* 'controllo mesi duplicati (stesso anno)'")
    prec_records = checks.check3(prec_records, min_not_null=5, logger=logger)
    logger.info("* 'controllo mesi duplicati (anni differenti)'")
    prec_records = checks.check4(prec_records, min_not_null=5, logger=logger)
    logger.info("* 'controllo world excedence'")
    prec_records = checks.check7(prec_records, max_threshold=800, logger=logger)
    logger.info('* controllo gap checks')
    prec_records = checks.check8(prec_records, threshold=300, logger=logger)
    logger.info("* 'controllo z-score checks'")
    pos_temp_days, neg_temp_days = checks.split_days_by_average_temp(temp_records)
    prec_records = checks.check10(prec_records, pos_temp_days, logger=logger)
    logger.info("* 'controllo z-score checks ghiaccio'")
    prec_records = checks.check10(prec_records, neg_temp_days, times_perc=5, flag=-26,
                                  logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in prec_records if r[3] and r[3] <= -10]
    upsert.update_prec_flags(conn, flag_records, schema=schema)
    logger.info('== end process chain for PRECI ==')
    return prec_records


def process_checks_t200(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for T200 ==')

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, " \
                 "(tmxgg).val_md, ((tmxgg).flag).wht, " \
                 "(tmngg).val_md, ((tmngg).flag).wht, " \
                 "(tmdgg).val_md, ((tmdgg).flag).wht"
    temp_records = querying.select_temp_records(
        conn, fields=['tmxgg', 'tmngg', 'tmdgg'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_null=False)
    temp_records = list(temp_records)

    for val_index in [2, 4, 6]:
        for temp_record in temp_records:
            if temp_record[val_index + 1] is None or temp_record[val_index+1] < -9:
                temp_record[val_index+1] = 1
            elif -9 <= temp_record[val_index+1] <= 0:
                # set None to values flagged as invalid (flag in [-9, 0])
                temp_record[val_index] = None
            # else temp_record[3] > 0: flag 1 or 5 remains unset

    logger.info("* 'controllo valori ripetuti' (Tmax)")
    temp_records = checks.check2(temp_records, exclude_values=(None,), logger=logger)
    logger.info("* 'controllo valori ripetuti'  (Tmin)")
    temp_records = checks.check2(temp_records, exclude_values=(None,), val_index=4, logger=logger)
    logger.info("* 'controllo mesi duplicati (stesso anno)' (Tmax)")
    temp_records = checks.check3(temp_records, logger=logger)
    logger.info("* 'controllo mesi duplicati (stesso anno)' (Tmin)")
    temp_records = checks.check3(temp_records, val_index=4, logger=logger)
    logger.info("* 'controllo mesi duplicati (anni differenti)' for variable Tmax")
    temp_records = checks.check4(temp_records, logger=logger)
    logger.info("* 'controllo mesi duplicati (anni differenti)' (Tmin)")
    temp_records = checks.check4(temp_records, val_index=4, logger=logger)
    logger.info("* controllo TMAX=TMIN")
    temp_records = checks.check5(temp_records, logger=logger)
    logger.info("* controllo TMAX=TMIN=0")
    temp_records = checks.check6(temp_records, logger=logger)
    logger.info("* 'controllo world excedence' for Tmax")
    temp_records = checks.check7(temp_records, min_threshold=-30, max_threshold=50, logger=logger)
    logger.info("* 'controllo world excedence' for Tmin")
    temp_records = checks.check7(temp_records, min_threshold=-40, max_threshold=40,
                                 val_index=4, logger=logger)
    logger.info("* 'controllo gap checks temperatura' for Tmax")
    temp_records = checks.check8(temp_records, threshold=10, split=True, logger=logger)
    logger.info("* 'controllo gap checks temperatura' Tmin")
    temp_records = checks.check8(temp_records, threshold=10, split=True, val_index=4,
                                 logger=logger)
    logger.info("* 'controllo z-score checks temperatura' for Tmax")
    temp_records = checks.check9(temp_records, logger=logger)
    logger.info("* 'controllo z-score checks temperatura' (Tmin)")
    temp_records = checks.check9(temp_records, val_index=4, logger=logger)
    logger.info("* 'controllo jump checks' for Tmax")
    temp_records = checks.check11(temp_records, logger=logger)
    logger.info("* 'controllo jump checks' (Tmin)")
    temp_records = checks.check11(temp_records, val_index=4, logger=logger)
    logger.info("* 'controllo Tmax < Tmin'")
    temp_records = checks.check12(temp_records, logger=logger)
    logger.info("* 'controllo dtr (diurnal temperature range)' (Tmax)")
    operators = max, operator.ge
    temp_records = checks.check13(temp_records, operators, logger=logger)
    logger.info("* 'controllo dtr (diurnal temperature range)' (Tmin)")
    operators = min, operator.le
    temp_records = checks.check13(temp_records, operators, logger=logger, jump=-35,
                                  val_indexes=(4,2))
    logger.info("* 'controllo world excedence' (tmdgg)")
    temp_records = checks.check7(
        temp_records, min_threshold=-36, max_threshold=46, val_index=6, logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in temp_records if r[3] and r[3] <= -10]
    upsert.update_flags(conn, flag_records, 'ds__t200', schema=schema, db_field='tmxgg')
    flag_records = [r for r in temp_records if r[5] and r[5] <= -10]
    upsert.update_flags(conn, flag_records, 'ds__t200', schema=schema, db_field='tmngg',
                        flag_index=5)
    flag_records = [r for r in temp_records if r[7] and r[7] <= -10]
    upsert.update_flags(conn, flag_records, 'ds__t200', schema=schema, db_field='tmdgg',
                        flag_index=7)
    logger.info('== end process chain for T200 ==')
    return temp_records


def process_checks_bagna(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for BAGNA ==')
    table, main_field, sub_field, min_threshold, max_threshold = \
        ('ds__bagna', 'bagna', 'val_md', -1, 25)

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, (%s).%s, case when ((%s).flag).wht=5 then 5 else 1 end" \
                 % (main_field, sub_field, main_field)
    where_sql = '(%s).%s IS NOT NULL' % (main_field, sub_field)
    table_records = querying.select_records(
        conn, table, fields=[main_field], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0), where_sql=where_sql)

    logger.info("* 'controllo world excedence' (%s.%s)" % (main_field, sub_field))
    table_records = checks.check7(
        table_records, min_threshold, max_threshold, logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in table_records if r[3] and r[3] <= -10]
    upsert.update_flags(conn, flag_records, table, schema=schema, db_field=main_field)

    logger.info('== end process chain for BAGNA ==')
    return table_records


def process_checks_elio(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for ELIOFANIA ==')
    table, main_field, sub_field, min_threshold, max_threshold = \
        ('ds__elio', 'elio', 'val_md', -1, 19)

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, (%s).%s, case when ((%s).flag).wht=5 then 5 else 1 end" \
                 % (main_field, sub_field, main_field)
    where_sql = '(%s).%s IS NOT NULL' % (main_field, sub_field)
    table_records = querying.select_records(
        conn, table, fields=[main_field], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0), where_sql=where_sql)

    logger.info("* 'controllo world excedence' (%s.%s)" % (main_field, sub_field))
    table_records = checks.check7(
        table_records, min_threshold, max_threshold, logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in table_records if r[3] and r[3] <= -10]
    upsert.update_flags(conn, flag_records, table, schema=schema, db_field=main_field)

    logger.info('== end process chain for ELIOFANIA ==')
    return table_records


def process_checks_radglob(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for RADIAZIONE GLOBALE ==')
    table, main_field, sub_field, min_threshold, max_threshold = \
        ('ds__radglob', 'radglob', 'val_md', -1, 601)

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, (%s).%s, case when ((%s).flag).wht=5 then 5 else 1 end" \
                 % (main_field, sub_field, main_field)
    where_sql = '(%s).%s IS NOT NULL' % (main_field, sub_field)
    table_records = querying.select_records(
        conn, table, fields=[main_field], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema, exclude_flag_interval=(-9, 0), where_sql=where_sql)

    logger.info("* 'controllo world excedence' (%s.%s)" % (main_field, sub_field))
    table_records = checks.check7(
        table_records, min_threshold, max_threshold, logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in table_records if r[3] and r[3] <= -10]
    upsert.update_flags(conn, flag_records, table, schema=schema, db_field=main_field)

    logger.info('== end process chain for RADIAZIONE GLOBALE ==')
    return table_records


def process_checks_press(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for PRESS ==')

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, " \
                 "(press).val_md, ((press).flag).wht, " \
                 "(press).val_mx, ((press).flag).wht, " \
                 "(press).val_mn, ((press).flag).wht"
    table_records = querying.select_records(
        conn, 'ds__press', fields=['press'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema)
    table_records = list(table_records)

    # note: only flag index = 3 is set
    for val_index in [2, ]:
        for table_record in table_records:
            if table_record[val_index + 1] is None or table_record[val_index+1] < -9:
                table_record[val_index+1] = 1
            elif -9 <= table_record[val_index+1] <= 0:
                # set None to values flagged as invalid (flag in [-9, 0])
                table_record[val_index] = None
            # else record[3] > 0: flag 1 or 5 remains unset

    for main_field, sub_field, min_threshold, max_threshold, val_index in [
            ('press', 'val_md', 959, 1061, 2),
            ('press', 'val_mx', 959, 1061, 4),
            ('press', 'val_mn', 959, 1061, 6),
    ]:
        logger.info("* 'controllo world excedence' (%s.%s)" % (main_field, sub_field))
        table_records = checks.check7(
            table_records, min_threshold, max_threshold, val_index=val_index, logger=logger,
            flag_index=3)
    logger.info("* 'controllo valori ripetuti' Pmedia")
    table_records = checks.check2(
        table_records, len_threshold=10, exclude_values=(None, ), val_index=2, logger=logger)
    logger.info("* 'controllo consistenza'")
    table_records = checks.check_consistency(table_records, (6, 2, 4), 3, flag=-10, logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in table_records if r[3] and r[3] <= -10]
    upsert.update_flags(conn, flag_records, 'ds__press', schema=schema, db_field='press')

    logger.info('== end process chain for PRESS ==')
    return table_records


def process_checks_urel(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for UREL ==')
    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, " \
                 "(ur).val_md, ((ur).flag).wht, " \
                 "(ur).val_mx, ((ur).flag).wht, " \
                 "(ur).val_mn, ((ur).flag).wht"
    table_records = querying.select_records(
        conn, 'ds__urel', fields=['ur'], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema)
    table_records = list(table_records)

    # note: only flag index = 3 is set
    for val_index in [2, ]:
        for table_record in table_records:
            if table_record[val_index + 1] is None or table_record[val_index+1] < -9:
                table_record[val_index+1] = 1
            elif -9 <= table_record[val_index+1] <= 0:
                # set None to values flagged as invalid (flag in [-9, 0])
                table_record[val_index] = None
            # else record[3] > 0: flag 1 or 5 remains unset

    for main_field, sub_field, min_threshold, max_threshold, val_index in [
        ('ur', 'val_md', -1, 101, 2),
        ('ur', 'val_mx', -1, 101, 4),
        ('ur', 'val_mn', -1, 101, 6),
    ]:
        logger.info("* 'controllo world excedence' (%s.%s)" % (main_field, sub_field))
        table_records = checks.check7(
            table_records, min_threshold, max_threshold, logger=logger, flag_index=3)
    logger.info("* 'controllo consistenza UR'")
    table_records = checks.check_consistency(table_records, (6, 2, 4), 3, flag=-10, logger=logger)

    logger.info('* final set of flags on database...')
    flag_records = [r for r in table_records if r[3] and r[3] <= -10]
    upsert.update_flags(conn, flag_records, 'ds__urel', schema=schema, db_field='ur')

    logger.info('== end process chain for UREL ==')
    return table_records


def process_checks_wind(conn, stations_ids, schema, logger):
    logger.info('== initial process chain for vnt10 ==')

    logger.info('* query to get records...')
    sql_fields = "cod_staz, data_i, " \
                 "(vntmd).ff, ((vntmd).flag).wht, " \
                 "(vntmxgg).dd, ((vntmxgg).flag).wht, " \
                 "(vntmxgg).ff, ((vntmxgg).flag).wht"
    table_records = querying.select_records(
        conn, 'ds__vnt10', fields=[], sql_fields=sql_fields, stations_ids=stations_ids,
        schema=schema)
    table_records = list(table_records)
    for val_index in [2, 4, 6]:
        for table_record in table_records:
            if table_record[val_index + 1] is None or table_record[val_index+1] < -9:
                table_record[val_index+1] = 1
            elif -9 <= table_record[val_index+1] <= 0:
                # set None to values flagged as invalid (flag in [-9, 0])
                table_record[val_index] = None
            # else record[3] > 0: flag 1 or 5 remains unset

    for main_field, sub_field, min_threshold, max_threshold, val_index in [
        ('vntmd', 'ff', -1, 103, 2),
        ('vntmxgg', 'dd', -1, 361, 4),
        ('vntmxgg', 'ff', -1, 103, 6),
    ]:
        logger.info("* 'controllo world excedence' (%s.%s)" % (main_field, sub_field))
        table_records = checks.check7(
            table_records, min_threshold, max_threshold, logger=logger, val_index=val_index)

    # some additional checks for series wind: valori ripetuti + consistence
    logger.info("* check 'valori ripetuti' for vntmd.ff...")
    filter_funct = lambda r: r[2] > 2
    table_records = checks.check2(
        table_records, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger)
    # logger.info('* final set of flags on database for FFmedia valori ripetuti')
    # upsert.update_vntmd_flags(conn, vntmd_series, schema=schema)

    logger.info("* check 'valori ripetuti' for vntmxgg.ff...")
    filter_funct = lambda r: r[2] > 2
    table_records = checks.check2(
        table_records, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger, val_index=6)

    logger.info("* check 'valori ripetuti' for vntmxgg.dd...")
    filter_funct = lambda r: r[2] > 0.5
    table_records = checks.check2(
        table_records, len_threshold=10, exclude_values=(None, ), filter_funct=filter_funct,
        logger=logger, val_index=4)

    logger.info("* 'controllo consistenza WIND'")
    table_records = checks.check12(table_records, min_diff=0, logger=logger, val_indexes=(6, 2))
    logger.info('* final set of flags on database...')

    # vntmd.flag setta anche quello di vnt.flag, per questo uso update_vntmd_flags
    flag_records = [r for r in table_records if r[3] and r[3] <= -10]
    upsert.update_vntmd_flags(conn, flag_records, schema=schema, logger=logger)
    flag_records = [r for r in table_records if r[5] and r[5] <= -10]
    upsert.update_flags(
        conn, flag_records, 'ds__vnt10', schema=schema, db_field='vntmxgg', flag_index=5)
    return table_records


def process_checks_chain(dburi, stations_ids=None, schema='dailypdbanpacarica', logger=None,
                         omit_flagsync=False):
    """
    Start a chain of checks on records of the database from a set of monitoring stations selected.

    :param dburi: db connection URI
    :param stations_ids: primary keys of the stations (if None: no filtering by stations)
    :param schema: database schema to use
    :param omit_flagsync: if False (default), omits the synchronization for flags -9, +5
    :param logger: logging object where to report actions
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info('== Start process ==')
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()

    if not omit_flagsync:
        logger.info(
            '* synchronization of flags +5 and -9 loading from schema dailypdbanpaclima...')
        upsert.sync_flags(conn, flags=(-9, 5), sourceschema='dailypdbanpaclima',
                          targetschema=schema, logger=logger)
        logger.info('* end of synchronization of flags +5 and -9')

    temp_records = process_checks_t200(conn, stations_ids, schema, logger)
    process_checks_preci(conn, stations_ids, schema, logger, temp_records)
    process_checks_bagna(conn, stations_ids, schema, logger)
    process_checks_elio(conn, stations_ids, schema, logger)
    process_checks_radglob(conn, stations_ids, schema, logger)
    process_checks_press(conn, stations_ids, schema, logger)
    process_checks_urel(conn, stations_ids, schema, logger)
    process_checks_wind(conn, stations_ids, schema, logger)

    logger.info('== End process ==')


def compute_daily_indicators2(conn, schema, logger):
    """
    Compute secondary indicators.

    :param conn: db connection object
    :param schema: db schema to consider
    :param logger: logger object for reporting
    :return:
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info('* querying ds__t200 for compute temperature indicators...')
    sql = """
    SELECT cod_staz, data_i, lat,
    (tmxgg).val_md, ((tmxgg).flag).wht,
    (tmngg).val_md, ((tmngg).flag).wht,
    (tmdgg).val_md, ((tmdgg).flag).wht
    FROM %s.ds__t200 LEFT JOIN dailypdbadmclima.anag__stazioni ON cod_staz=id_staz""" % schema

    temp_records = map(list, conn.execute(sql))

    logger.info('* computing temperature indicators...')
    temp_items = []
    grgg_items = []
    etp_items = []

    for record in temp_records:
        cod_staz, data_i, lat = record[0:3]
        base_item = {'data_i': data_i, 'cod_staz': cod_staz,  'cod_aggr': 4}
        tmax, tmax_flag = record[3:5]
        tmin, tmin_flag = record[5:7]
        tmedia, tmedia_flag = record[7:9]
        allow_compute_grgg = tmedia is not None and tmedia_flag is not None and tmedia_flag > 0
        if allow_compute_grgg:
            grgg_flag, grgg1, grgg2, grgg3, grgg4, grgg5 = compute.compute_grgg(tmedia)
            grgg_item = base_item.copy()
            grgg_item.update({
                'grgg.flag.wht': grgg_flag[1], 'grgg.tot00': grgg1, 'grgg.tot05': grgg2,
                'grgg.tot10': grgg3, 'grgg.tot15': grgg4, 'grgg.tot21': grgg5})
            grgg_items.append(grgg_item)
        allow_compute_deltagg = tmax is not None and tmax_flag is not None and tmax_flag > 0 or \
            tmin is not None and tmin_flag is not None and tmin_flag > 0
        if allow_compute_deltagg:
            deltagg = tmax - tmin
            tmdgg1 = deltagg / 2
            temp_item = base_item.copy()
            temp_item.update({'tmdgg1.val_md': tmdgg1, 'tmdgg1.flag.wht': 1,
                              'deltagg.val_md': deltagg, 'deltagg.flag.wht': 1})
            temp_items.append(temp_item)
        if allow_compute_grgg and allow_compute_deltagg and lat is not None:
            jd = int(data_i.strftime('%j'))
            etp = compute.compute_etp(tmedia, tmax, tmin, lat, jd)
            etp_item = base_item.copy()
            etp_item.update({'etp.val_md': etp[1], 'etp.flag.wht': etp[0][1]})
            etp_items.append(etp_item)

    temp_fields = []
    if temp_items:
        temp_fields = list(temp_items[0].keys())
    etp_fields = []
    if etp_items:
        etp_fields = list(etp_items[0].keys())
    grgg_fields = []
    if grgg_items:
        grgg_fields = list(grgg_items[0].keys())
    for table_name, fields, data in [
        ('ds__t200', temp_fields, temp_items),
        ('ds__etp', etp_fields, etp_items),
        ('ds__grgg', grgg_fields, grgg_items),
    ]:
        logger.info('updating temperature indicators on table %s.%s' % (schema, table_name))
        sql = upsert.create_upsert(table_name, schema, fields, data, 'upsert')
        if sql:
            conn.execute(sql)

    logger.info('* computing bilancio idrico...')
    sql = """
    SELECT cod_staz, data_i, (prec24).val_tot, (etp).val_md 
    FROM %s.ds__etp a JOIN %s.ds__preci b USING (cod_staz,data_i) 
    WHERE ((prec24).flag).wht > 0 AND ((etp).flag).wht > 0 
    AND (prec24).val_tot IS NOT NULL AND (etp).val_md IS NOT NULL
    """ % (schema, schema)
    idro_records = map(list, conn.execute(sql))
    idro_items = []
    for idro_record in idro_records:
        cod_staz, data_i, lat = idro_record[0:3]
        base_item = {'data_i': data_i, 'cod_staz': cod_staz, 'cod_aggr': 4}
        prec24, etp = idro_record[2:4]
        deltaidro = compute.compute_deltaidro(prec24, etp)
        idro_item = base_item.copy()
        idro_item.update({'deltaidro.flag.wht': deltaidro[0][1], 'deltaidro.val_md': deltaidro[1]})
        idro_items.append(idro_item)

    logger.info('updating bilancio idrico on table %s.%s' % (schema, 'ds__delta_idro'))
    idro_fields = []
    if idro_items:
        idro_fields = list(idro_items[0].keys())
    sql = upsert.create_upsert('ds__delta_idro', schema, idro_fields, idro_items, 'upsert')
    if sql:
        conn.execute(sql)


def process_dma(conn, startschema, targetschema, policy, logger):

    if logger is None:
        logger = logging.getLogger(LOG_NAME)

    dma.process_dma_bagnatura(conn, startschema, targetschema, policy, logger)
    dma.process_dma_bilancio_idrico(conn, startschema, targetschema, policy, logger)
    dma.process_dma_eliofania(conn, startschema, targetschema, policy, logger)
    dma.process_dma_radiazione_globale(conn, startschema, targetschema, policy, logger)
    dma.process_dma_evapotraspirazione(conn, startschema, targetschema, policy, logger)
    dma.process_dma_gradi_giorno(conn, startschema, targetschema, policy, logger)
    dma.process_dma_pressione(conn, startschema, targetschema, policy, logger)
    dma.process_dma_umidita_relativa(conn, startschema, targetschema, policy, logger)
    dma.process_dma_bioclimatologia(conn, startschema, targetschema, policy, logger)
    dma.process_dma_precipitazione(conn, startschema, targetschema, policy, logger)
    dma.process_dma_vento(conn, startschema, targetschema, policy, logger)
    dma.process_dma_temperatura(conn, startschema, targetschema, policy, logger)
