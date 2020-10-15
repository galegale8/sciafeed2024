"""
This module contains functions and utilities that involve more components of sciafeed.
"""
import operator
from os import listdir
from os.path import isfile, join, splitext

from sciafeed import checks
from sciafeed import compute
from sciafeed import db_utils
from sciafeed import export
from sciafeed import parsing
from sciafeed import utils


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


def check_chain(dburi, stations_ids=None, report_fp=None):
    """
    Start a chain of checks on records of the database from a set of monitoring stations selected.

    :param dburi: db connection URI
    :param stations_ids: primary keys of the stations (if None: no filtering by stations)
    :param report_fp: report file pointer
    """
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()

    report_fp.write('* initial query to get records of PREC...' + '\n')
    sql_fields = "cod_staz, data_i, (prec24).val_tot, 1 as flag"
    valid_prec_records = db_utils.select_prec_records(
        conn, sql_fields=sql_fields, stations_ids=stations_ids,
        flag_threshold=None, exclude_null=True)
    invalid_prec_records = []

    report_fp.write('* initial query to get records of temperature...' + '\n')
    sql_fields = "cod_staz, data_i, (tmxgg).val_md, 1 as flag_tmxgg, " \
                 "(tmngg).val_md, 1 as flag_tmngg, (tmdgg).val_md, 1 as flag_tmdgg"
    fields = ['Tmax', 'Tmin']
    valid_temp_records = db_utils.select_temp_records(
        conn, fields, sql_fields=sql_fields, stations_ids=stations_ids,
        flag_threshold=None, exclude_null=False)
    invalid_temp_records = []

    report_fp.write("== STARTING CHECK CHAIN ==")
    report_fp.write("* 'controllo valori ripetuti = 0' for variable PREC")
    valid_prec_records, current_invalid_prec, msgs = checks.check1(
        valid_prec_records, len_threshold=180, flag=-12)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_prec_records.extend(current_invalid_prec)

    report_fp.write("* 'controllo valori ripetuti' for variable PREC")
    valid_prec_records, current_invalid_prec, msgs = checks.check2(
        valid_prec_records, len_threshold=20, flag=-13, exclude_values=(0, ))
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_prec_records.extend(current_invalid_prec)

    report_fp.write("* 'controllo valori ripetuti' for variable Tmax")
    valid_temp_records, current_invalid_temp, msgs = checks.check2(
        valid_temp_records, len_threshold=20, flag=-13, exclude_values=(None, ))
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* 'controllo valori ripetuti' for variable Tmin")
    valid_temp_records, current_invalid_temp, msgs = checks.check2(
        valid_temp_records, len_threshold=20, flag=-13, exclude_values=(None, ), val_index=4)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* 'controllo mesi duplicati (stesso anno)' for variable PREC")
    valid_prec_records, current_invalid_prec, msgs = checks.check3(
        valid_prec_records, flag=-15, min_not_null=5)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_prec_records.extend(current_invalid_prec)

    report_fp.write("* 'controllo mesi duplicati (stesso anno)' for variable Tmax")
    valid_temp_records, current_invalid_temp, msgs = checks.check3(valid_temp_records, flag=-15)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* 'controllo mesi duplicati (stesso anno)' for variable Tmin")
    valid_temp_records, current_invalid_temp, msgs = checks.check3(
        valid_temp_records, flag=-15, val_index=4)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* 'controllo mesi duplicati (anni differenti)' for variable PREC")
    valid_prec_records, current_invalid_prec, msgs = checks.check4(
        valid_prec_records, flag=-17, min_not_null=5)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_prec_records.extend(current_invalid_prec)

    report_fp.write("* 'controllo mesi duplicati (anni differenti)' for variable Tmax")
    valid_temp_records, current_invalid_temp, msgs = checks.check4(valid_temp_records, flag=-17)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* 'controllo mesi duplicati (anni differenti)' for variable Tmin")
    valid_temp_records, current_invalid_temp, msgs = checks.check4(
        valid_temp_records, flag=-17, val_index=4)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* controllo TMAX=TMIN")
    valid_temp_records, current_invalid_temp, msgs = checks.check5(
        valid_temp_records, len_threshold=10, flag=-19)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write("* controllo TMAX=TMIN=0")
    valid_temp_records, current_invalid_temp, msgs = checks.check6(valid_temp_records, flag=-20)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
    invalid_temp_records.extend(current_invalid_temp)

    report_fp.write('* start check7 for PREC' + '\n')
    valid_prec_records, current_invalid_prec, msgs = checks.check7(
        valid_prec_records, min_threshold=800, flag=-21)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check7 for Tmax' + '\n')
    valid_temp_records, current_invalid_temp, msgs = checks.check7(
        valid_temp_records, min_threshold=-30, max_threshold=50, flag=-21)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check7 for Tmin' + '\n')
    valid_temp_records, current_invalid_temp, msgs = checks.check7(
        valid_temp_records, min_threshold=-40, max_threshold=40, flag=-21, val_index=4)
    for msg in msgs:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    # from now on: FIXME
    report_fp.write('* start check8 for PREC' + '\n')
    msgs8_1 = checks.check8(conn, stations_ids, 'PREC', threshold=300, split=False, flag_sup=-23)
    for msg in msgs8_1:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check8 for Tmax' + '\n')
    msgs8_2 = checks.check8(conn, stations_ids, 'Tmax', threshold=10, split=True,
                            flag_sup=-23, flag_inf=-24)
    for msg in msgs8_2:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check8 for Tmin' + '\n')
    msgs8_3 = checks.check8(conn, stations_ids, 'Tmin', threshold=10, split=True,
                            flag_sup=-23, flag_inf=-24)
    for msg in msgs8_3:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check9 for Tmax' + '\n')
    msgs9_1 = checks.check9(conn, stations_ids, 'Tmax', num_dev_std=6, window_days=15,
                            min_num=100, flag=-25)
    for msg in msgs9_1:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check9 for Tmin' + '\n')
    msgs9_2 = checks.check9(conn, stations_ids, 'Tmin', num_dev_std=6, window_days=15,
                            min_num=100, flag=-25)
    for msg in msgs9_2:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check10 for PREC' + '\n')
    msgs10_1 = checks.check10(
        conn, stations_ids, 'PREC', where_sql_on_temp="(tmdgg).val_md >= 0", times_perc=9,
        percentile=95, window_days=29, min_num=20, flag=-25)
    for msg in msgs10_1:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check10 for PREC (ice)' + '\n')
    msgs10_2 = checks.check10(
        conn, stations_ids, 'PREC', where_sql_on_temp="(tmdgg).val_md < 0", times_perc=5,
        percentile=95, window_days=29, min_num=20, flag=-26)
    for msg in msgs10_2:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check11 for Tmax' + '\n')
    msgs11_1 = checks.check11(conn, stations_ids, 'Tmax', max_diff=18, flag=-27)
    for msg in msgs11_1:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check11 for Tmin' + '\n')
    msgs11_2 = checks.check11(conn, stations_ids, 'Tmin', max_diff=18, flag=-27)
    for msg in msgs11_2:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check12 for Tmax and Tmin' + '\n')
    msgs12 = checks.check12(conn, stations_ids, ['Tmax', 'Tmin'], min_diff=5, flag=-29)
    for msg in msgs12:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check13 for [Tmax, Tmin] (policy: >= max)' + '\n')
    operators = max, operator.ge
    msgs13_1 = checks.check13(conn, stations_ids, ['Tmax', 'Tmin'], operators, jump=35, flag=-31)
    for msg in msgs13_1:
        report_fp.write(msg + '\n')
    report_fp.write('\n')

    report_fp.write('* start check13 for [Tmin, Tmax] (policy: <= min)' + '\n')
    operators = min, operator.le
    msgs13_2 = checks.check13(conn, stations_ids, ['Tmin', 'Tmax'], operators, jump=-35, flag=-31)
    for msg in msgs13_2:
        report_fp.write(msg + '\n')
    report_fp.write('\n')
