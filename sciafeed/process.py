"""
This module contains functions and utilities that involve more components of sciafeed.
"""
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


def check_chain(dburi, stations_ids=None):
    """
    Start a chain of checks on records of the database from a set of monitoring stations selected.

    :param dburi: db connection URI
    :param stations_ids: primary keys of the stations (if None: no filtering by stations)
    :return: the list of report messages
    """
    engine = db_utils.ensure_engine(dburi)
    conn = engine.connect()
    msgs = []
    # NOTE: 'PREC' -> check on ds__preci.prec24 and set flag of prec24, prec01, prec06, prec12
    # NOTE: 'Tmax' -> check on ds__t200.tmxgg(val_md) and set flag on ds__t200.tmxgg(val_md)
    # NOTE: 'Tmin' -> check on ds__t200.tmngg(val_md) and set flag on ds__t200.tmngg(val_md)
    # NOTE: consider consecutive values,not consecutive days

    msgs += db_utils.reset_flags(conn, stations_ids, flag_threshold=-10, set_flag=1)

    msgs += checks.check1(conn, stations_ids, 'PREC', len_threshold=180, flag=-12)
    msgs += checks.check2(conn, stations_ids, 'PREC', len_threshold=20, flag=-13)
    msgs += checks.check2(conn, stations_ids, 'Tmax', len_threshold=20, flag=-13)
    msgs += checks.check2(conn, stations_ids, 'Tmin', len_threshold=20, flag=-13)
    msgs += checks.check3(conn, stations_ids, 'Tmax', len_threshold=5, flag=-14)
    msgs += checks.check3(conn, stations_ids, 'Tmin', len_threshold=5, flag=-14)
    msgs += checks.check4(conn, stations_ids, 'PREC', flag=-15, min_not_null=5)
    msgs += checks.check4(conn, stations_ids, 'Tmax', flag=-15)
    msgs += checks.check4(conn, stations_ids, 'Tmin', flag=-15)
    msgs += checks.check5(conn, stations_ids, 'PREC', flag=-17, min_not_null=5)
    msgs += checks.check5(conn, stations_ids, 'Tmax', flag=-17)
    msgs += checks.check5(conn, stations_ids, 'Tmin', flag=-17)
    msgs += checks.check6(conn, stations_ids, ['Tmax', 'Tmin'], len_threshold=10, flag=-19)
    msgs += checks.check7(conn, stations_ids, ['Tmax', 'Tmin'], flag=-20)
    msgs += checks.check8(conn, stations_ids, 'PREC', min=800, flag=-21)
    msgs += checks.check8(conn, stations_ids, 'Tmax', min=-30, max=50, flag=-21)
    msgs += checks.check8(conn, stations_ids, 'Tmin', min=-40, max=40, flag=-21)
    msgs += checks.check9(conn, stations_ids, 'PREC', max_threshold=300,
                          split_policy=(False, None), flag=-23)
    msgs += checks.check9(conn, stations_ids, 'Tmax', max_threshold=10,
                          split_policy=(True, 'top'), flag=-23)
    msgs += checks.check9(conn, stations_ids, 'Tmin', max_threshold=10,
                          split_policy=(True, 'bottom'), flag=-24)
    msgs += checks.check10(conn, stations_ids, 'Tmax', num_dev_std=6,
                           window_days=15, min_num=100, flag=-25)
    msgs += checks.check10(conn, stations_ids, 'Tmin', num_dev_std=6,
                           window_days=15, min_num=100, flag=-25)
    msgs += checks.check11(conn, stations_ids, 'PREC', num_dev_std=9*0.95,
                           window_days=29, min_num=20, flag=-25)
    msgs += checks.check12(conn, stations_ids, 'PREC', num_dev_std=5*0.95,
                           window_days=29, min_num=20, flag=-26)
    msgs += checks.check13(conn, stations_ids, 'Tmax', max_up=18, window_days=3, flag=-27)
    msgs += checks.check13(conn, stations_ids, 'Tmin', max_up=18, window_days=3, flag=-27)
    msgs += checks.check14(conn, stations_ids, ['Tmax', 'Tmin'], min_diff=5, flag=-29)
    msgs += checks.check15(conn, stations_ids, ['Tmax', 'Tmin'], window_days=3, jump=35,
                           policy=(max, 'greater'), flag=-31)
    msgs += checks.check15(conn, stations_ids, ['Tmin', 'Tmax'], window_days=3, jump=-35,
                           policy=(min, 'lower'), flag=-31)

