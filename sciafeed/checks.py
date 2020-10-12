"""
This module contains the functions and utilities to check climatologic data.
`data` is a python structure, of kind:
::

    [(metadata, datetime object, par_code, par_value, flag), ...]

- metadata: a python dictionary containing information to identify a station
- datetime object: a datetime.datetime instance that is the time of measurement
- par_code: the parameter code
- par_value: the parameter value
- flag: a boolean flag to consider valid or not the value
"""
from datetime import datetime, timedelta
import itertools
import statistics

import numpy as np

from sciafeed import db_utils
from sciafeed import querying
from sciafeed import utils


def data_internal_consistence_check(input_data, limiting_params=None):
    """
    Get the internal consistent check for an input data object.
    It assumes that `input_data` has an agreed structure, i.e.:
    ::

    [(metadata, date obj, par_code, par_value, par_flag), ....]

    Return the list of error messages, and the data with flags modified.
    The list of error messages is [(record_id, error string), ...].

    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param input_data: an object containing measurements
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_modified)
    """
    if limiting_params is None:
        limiting_params = dict()
    err_msgs = []
    data_modified = []
    input_data = sorted(input_data, key=utils.different_data_record_info)
    for (station_id, row_date), measures in itertools.groupby(
            input_data, key=utils.different_data_record_info):
        # here measures have all the same station and date
        props = {m[2]: (m[3], m[4], m[0]) for m in measures}
        for par_code, (par_value, par_flag, metadata) in props.items():
            if par_code not in limiting_params or not par_flag or par_value is None:
                # no check if the parameter is flagged invalid or no in the limiting_params
                measure = (metadata, row_date, par_code, par_value, par_flag)
                data_modified.append(measure)
                continue
            par_code_min, par_code_max = limiting_params[par_code]
            par_code_min_value, par_code_min_flag, md_min = props[par_code_min]
            par_code_max_value, par_code_max_flag, md_max = props[par_code_max]
            # check minimum
            if par_code_min_flag and par_code_min_value is not None:
                par_code_min_value = float(par_code_min_value)
                if par_value < par_code_min_value:
                    par_flag = False
                    row_id = metadata.get('row', 1)  # TODO: an ID when it comes from the db
                    err_msg = "The values of %r and %r are not consistent" \
                              % (par_code, par_code_min)
                    err_msgs.append((row_id, err_msg))
            # check maximum
            if par_code_max_flag and par_code_max_value is not None:
                par_code_max_value = float(par_code_max_value)
                if par_value > par_code_max_value:
                    par_flag = False
                    row_id = metadata.get('row', 1)  # TODO: an ID when it comes from the db
                    err_msg = "The values of %r and %r are not consistent" \
                              % (par_code, par_code_max)
                    err_msgs.append((row_id, err_msg))
            measure = (metadata, row_date, par_code, par_value, par_flag)
            data_modified.append(measure)
    return err_msgs, data_modified


def data_weak_climatologic_check(input_data, parameters_thresholds=None):
    """
    Get the weak climatologic check for an input data object, i.e. it flags
    as invalid a value if it is out of a defined range.
    It assumes that `input_data` has an agreed structure i.e.:
    ::

    [(metadata, date obj, par_code, par_value, par_flag), ....]

    Return the list of error messages, and the resulting data with flags updated.
    The list of error messages is [(record_id, error string), ...].
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param input_data: an object containing measurements
    :param parameters_thresholds: dictionary of thresholds for each parameter code
    :return: (err_msgs, data_modified)
    """
    if not parameters_thresholds:
        parameters_thresholds = dict()
    err_msgs = []
    data_modified = []
    for measure in input_data:
        metadata, row_date, par_code, par_value, par_flag = measure
        row_id = metadata.get('row', 1)  # TODO: an ID when it comes from the db
        if par_code not in parameters_thresholds or not par_flag or par_value is None:
            # no check if limiting parameters are flagged invalid or value is None
            data_modified.append(measure)
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            par_flag = False
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append((row_id, err_msg))
        new_measure = (metadata, row_date, par_code, par_value, par_flag)
        data_modified.append(new_measure)
    return err_msgs, data_modified


def check1(conn, stations_ids=None, var='PREC', len_threshold=180, flag=-12, use_records=None):
    """
    Check "controllo valori ripetuti = 0" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if var != 'PREC':
        raise NotImplementedError('check1 not implemented for variable %s' % var)
    if not use_records:
        sql_fields = "cod_staz, data_i, (prec24).val_tot"
        results = querying.select_prec_records(conn, sql_fields, stations_ids, flag_threshold=1)
    msg = "'controllo valori ripetuti = 0' for variable %s (len=%s)" \
          % (var, len_threshold)
    msgs.append(msg)
    block_index = 0
    block_records = []
    to_be_resetted = []
    i = 0
    prev_staz = None
    for i, result in enumerate(results):
        current_staz = result.cod_staz
        if current_staz == prev_staz and result.val_tot == 0:
            block_index += 1
            block_records.append(result)
            if block_index == len_threshold:
                to_be_resetted.extend(block_records)
            elif block_index > len_threshold:
                to_be_resetted.append(result)
        else:
            block_index = 0
            block_records = []
        prev_staz = current_staz
    msg = "Checked %i records" % i + 1
    msgs.append(msg)
    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_prec_flags(conn, to_be_resetted, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check2(conn, stations_ids, var, len_threshold=20, flag=-13, use_records=None,
           exclude_values=()):
    """
    Check "controllo valori ripetuti" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :param exclude_values: query excludes not none values in this iterable
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if var == 'PREC':
            sql_fields = "cod_staz, data_i, (prec24).val_tot"
            results = querying.select_prec_records(
                conn, sql_fields, stations_ids, exclude_values=exclude_values)
            field_to_check = 'val_tot'
        elif var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids, exclude_values=exclude_values)
            field_to_check = 'val_md'
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids, exclude_values=exclude_values)
            field_to_check = 'val_md'
        else:
            raise NotImplementedError('check2 not implemented for variable %s' % var)
    msg = "'controllo valori ripetuti' for variable %s (len=%s)" \
          % (var, len_threshold)
    msgs.append(msg)
    block_index = 0
    block_records = []
    to_be_resetted = []
    i = 0
    prev_staz = None
    prev_value = None
    for i, result in enumerate(results):
        current_staz = result.cod_staz
        current_value = result[field_to_check]
        if current_staz == prev_staz and current_value == prev_value:
            block_index += 1
            block_records.append(result)
            if block_index == len_threshold:
                to_be_resetted.extend(block_records)
            elif block_index > len_threshold:
                to_be_resetted.append(result)
        else:
            block_index = 0
            block_records = []
        prev_staz = current_staz
        prev_value = current_value
    msg = "Checked %i records" % i + 1
    msgs.append(msg)
    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    if var in ['Tmax', 'Tmin']:
        db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    else:
        db_utils.set_prec_flags(conn, to_be_resetted, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check3(conn, stations_ids, var, min_not_null=None, flag=-14, use_records=None):
    """
    Check "controllo mesi duplicati (mesi differenti appartenenti allo stesso anno)" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param min_not_null: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check3 not implemented for variable %s' % var)
    msg = "'controllo mesi duplicati (mesi differenti appartenenti allo stesso anno)' " \
          "for variable %s" % var
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    def group_by_year(record):
        return record.data_i.year

    def group_by_month(record):
        return record.data_i.month

    to_be_resetted = []
    for station, station_records in itertools.groupby(results, group_by_station):
        for year, year_records in itertools.groupby(station_records, group_by_year):
            year_dict = dict()
            for month, month_records in itertools.groupby(year_records, group_by_month):
                month_values = {g[1].day: g[2] for g in month_records if g[2] is not None}
                if month_values in year_dict.values():
                    if min_not_null is None or len(month_values.values()) >= min_not_null:
                        to_be_resetted += list(month_records)
                year_dict[month] = month_values

    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    if var in ['Tmax', 'Tmin']:
        db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    else:
        db_utils.set_prec_flags(conn, to_be_resetted, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check4(conn, stations_ids, var, min_not_null=None, flag=-17, use_records=None):
    """
    Check "controllo mesi duplicati (mesi uguali appartenenti ad anni differenti)" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param min_not_null: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check4 not implemented for variable %s' % var)
    msg = "'controllo mesi duplicati (mesi uguali appartenenti ad anni differenti)' " \
          "for variable %s" % var
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    def group_by_year(record):
        return record.data_i.year, record.data_i.month

    def group_by_month(record):
        return record.data_i.month

    to_be_resetted = []
    for station, station_records in itertools.groupby(results, group_by_station):
        months_dict = dict()
        for year, year_records in itertools.groupby(station_records, group_by_year):
            for month, month_records in itertools.groupby(year_records, group_by_month):
                if month not in months_dict:
                    months_dict[month] = []
                month_values = {g[1].day: g[2] for g in month_records if g[2] is not None}
                if month_values in months_dict[month]:
                    if min_not_null is None or len(month_values.values()) >= min_not_null:
                        to_be_resetted += list(month_records)
                else:
                    months_dict[month].append(month_values)

    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    if var in ['Tmax', 'Tmin']:
        db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    else:
        db_utils.set_prec_flags(conn, to_be_resetted, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check5(conn, stations_ids, variables, len_threshold=10, flag=-19, use_records=None):
    """
    Check "controllo TMAX=TMIN" for the `variables`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param variables: list of names of the variables to check
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if sorted(variables) != ['Tmax', 'Tmin']:
            raise NotImplementedError("check5 not implemented for variables != ['Tmax', 'Tmin']")
        sql_fields = "cod_staz, data_i, (tmngg).val_md, (tmxgg).val_md"
        results = querying.select_temp_records(conn, ['tmngg', 'tmxgg'], sql_fields, stations_ids)
    msg = "'controllo TMAX=TMIN' for variables %s (len=%s)" % (repr(vars), len_threshold)
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    to_be_resetted = []
    for station, station_records in itertools.groupby(results, group_by_station):
        block_index = 0
        block_records = []
        for station_record in station_records:
            if station_record[2] == station_record[3]:
                block_index += 1
                block_records.append(station_record)
                if block_index == len_threshold:
                    to_be_resetted.extend(block_records)
                elif block_index > len_threshold:
                    to_be_resetted.append(station_record)
            else:
                block_index = 0
                block_records = []

    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmin', flag)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmax', flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check6(conn, stations_ids, variables, flag=-20, use_records=None):
    """
    Check "controllo TMAX=TMIN=0" for the `variables`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param variables: list of names of the variables to check
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if sorted(variables) != ['Tmax', 'Tmin']:
            raise NotImplementedError("check6 not implemented for variables != ['Tmax', 'Tmin']")
        sql_fields = "cod_staz, data_i, (tmngg).val_md, (tmxgg).val_md"
        results = querying.select_temp_records(conn, ['tmngg', 'tmxgg'], sql_fields, stations_ids)
    msg = "'controllo TMAX=TMIN=0' for variables %s " % repr(variables)
    msgs.append(msg)

    to_be_resetted = []
    i = 0
    for i, station_record in enumerate(results):
        if station_record[2] == station_record[3] == 0:
            to_be_resetted.append(station_record)

    msg = "Checked %i records" % i + 1
    msgs.append(msg)
    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmin', flag)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmax', flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check7(conn, stations_ids, var, min_threshold=None, max_threshold=None, flag=-21,
           use_records=None):
    """
    Check "controllo world excedence" for the `variables`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param min_threshold: minimum value in the check
    :param max_threshold: maximum value in the check
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if var == 'PREC':
            sql_fields = "cod_staz, data_i, (prec24).val_tot"
            results = querying.select_prec_records(
                conn, sql_fields, stations_ids)
        elif var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check7 not implemented for variable %s' % var)
    msg = "'controllo world excedence' for variable %s " % var
    msgs.append(msg)

    to_be_resetted = []
    i = 0
    exclude_condition = lambda r: False
    if min_threshold is not None:
        exclude_condition = lambda r: r[2] <= min_threshold
    elif max_threshold is not None:
        exclude_condition = lambda r: r[2] >= max_threshold
    if min_threshold is not None and max_threshold is not None:
        exclude_condition = lambda r: r[2] <= min_threshold or r[2] >= max_threshold
    for i, station_record in enumerate(results):
        if exclude_condition(station_record):
            to_be_resetted.append(station_record)

    msg = "Checked %i records" % i
    msgs.append(msg)
    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmin', flag)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmax', flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def gap_top_checks(terms, threshold):
    """
    sort terms of kind (value, record) by index 0, check if gap between 2 consecutive values are
    over the threshold, and return the list of the corresponding record and all the following

    :param terms: list of tuple (value, record)
    :param threshold: the threshold to check
    :return: list of records
    """
    terms = sorted(terms)
    ret_values = []
    found_break = False
    for index in range(len(terms) - 1):
        if found_break:
            ret_values.append(terms[index][1])
        if abs(ret_values[index][0] - ret_values[index + 1][0]) > threshold:
            ret_values.append(ret_values[index][1])
            found_break = True
    return ret_values


def gap_bottom_checks(terms, threshold):
    """
    sort terms of kind (value, record) by index 0 reverse, check if gap between 2 consecutive
    values are lower than the threshold, and return the list of the corresponding record and
    all the following

    :param terms: list of tuple (value, record)
    :param threshold: the threshold to check
    :return: list of records
    """
    terms = sorted(terms, reverse=True)
    ret_values = []
    found_break = False
    for index in range(len(terms) - 2):
        if found_break:
            ret_values.append(terms[index][1])
        if abs(ret_values[index][0] - ret_values[index + 1][0]) > threshold:
            ret_values.append(ret_values[index + 1][1])
            found_break = True
    return ret_values


def check8(conn, stations_ids, var, threshold=None, split=False, flag_sup=-23, flag_inf=-24,
           use_records=None):
    """
    Check "controllo gap checks" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param threshold: value of the threshold in the check
    :param split: if False (default), don't split by median, and consider only flag_sup
    :param flag_sup: value of the flag to be set for found records with split=False, or with
                     split=True for the top part of the split
    :param flag_inf: value of the flag to be set for found records with split=True for the
                     bottom part of the split
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if var == 'PREC':
            sql_fields = "cod_staz, data_i, (prec24).val_tot"
            results = querying.select_prec_records(
                conn, sql_fields, stations_ids)
        elif var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check8 not implemented for variable %s' % var)
    msg = "'controllo gap checks  precipitazione' for variable %s " % var
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    def group_by_year(record):
        return record.data_i.year

    def group_by_month(record):
        return record.data_i.month

    to_be_resetted_flag_sup = []
    to_be_resetted_flag_inf = []
    if not split:
        for station, station_records in itertools.groupby(results, group_by_station):
            for year, year_records in itertools.groupby(station_records, group_by_year):
                for month, month_records in itertools.groupby(year_records, group_by_month):
                    month_values = [(g[2], g) for g in month_records if g[2] is not None]
                    to_be_resetted_flag_sup += gap_top_checks(month_values, threshold)
    else:
        for station, station_records in itertools.groupby(results, group_by_station):
            for year, year_records in itertools.groupby(station_records, group_by_year):
                for month, month_records in itertools.groupby(year_records, group_by_month):
                    month_values = [(g[2], g) for g in month_records if g[2] is not None].sort()
                    median = statistics.median([g[0] for g in month_values])
                    top_values = [g for g in month_values if g >= median]
                    to_be_resetted_flag_sup += gap_top_checks(top_values, threshold)
                    bottom_values = [g for g in month_values if g <= median]
                    to_be_resetted_flag_inf += gap_bottom_checks(bottom_values, threshold)

    for to_be_resetted, flag in [(to_be_resetted_flag_sup, flag_sup),
                                 (to_be_resetted_flag_inf, flag_inf)]:
        num_to_be_resetted = len(to_be_resetted)
        if num_to_be_resetted:
            msg = "Found %i records with flag %s to be reset" % (num_to_be_resetted, flag)
            msgs.append(msg)
            msg = "Resetting flags to value %s..." % flag
            msgs.append(msg)
            if var == 'PREC':
                db_utils.set_prec_flags(conn, to_be_resetted, flag)
            else:
                db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check9(conn, stations_ids, var, num_dev_std=6, window_days=15, min_num=100, flag=-25,
           use_records=None):
    """
    Check "controllo z-score checks temperatura" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param num_dev_std: times of the standard deviation to be considered in the check
    :param window_days: the time window to consider (in days)
    :param min_num: the minimum size of the values to be found inside the window
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not (window_days % 2):
        raise ValueError('window_days must be odd')
    if not use_records:
        if var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check9 not implemented for variable %s' % var)
    msg = "'controllo z-score checks temperatura' for variable %s " % var
    msgs.append(msg)

    to_be_resetted = []
    half_window = (window_days - 1) // 2

    def group_by_station(record):
        return record.cod_staz

    for station, station_records in itertools.groupby(results, group_by_station):
        check_date = datetime(2000, 1, 1)  # first of a leap year
        for i in range(366):
            reference_days = [check_date + timedelta(n)
                              for n in range(-half_window, half_window+1)]
            day_month_tuples = [(day.day, day.month) for day in reference_days]
            sample_records = querying.filter_by_day_patterns(station_records, day_month_tuples)
            if len(sample_records) >= min_num:
                sample_values = [r[2] for r in sample_records]
                average = statistics.mean(sample_values)
                dev_std_limit = statistics.stdev(sample_values) * num_dev_std
                check_records = querying.filter_by_day_patterns(
                    sample_records, [check_date.month, check_date.day])
                for check_record in check_records:
                    if abs(check_record[2] - average) > dev_std_limit:
                        to_be_resetted.append(check_record)
            check_date += timedelta(1)

    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check10(conn, stations_ids, var, where_sql_on_temp="(tmdgg).val_md >= 0", times_perc=9,
            percentile=95, window_days=29, min_num=20, flag=-25, use_records=None):
    """
    Check "controllo z-score checks precipitazione" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param where_sql_on_temp: add the selected where clause in sql for the temperature
    :param times_perc: number of times of the percentile to create the limit
    :param percentile: percentile value of the distribution to use
    :param window_days: the time window to consider (in days)
    :param min_num: the minimum size of the values to be found inside the window
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not (window_days % 2):
        raise ValueError('window_days must be odd')
    if not use_records:
        if var == 'PREC':
            fields = "(tmdgg).val_md"
            sql_fields = "cod_staz, data_i"
            temp_records = querying.select_temp_records(
                conn, fields, sql_fields=sql_fields,
                stations_ids=stations_ids, where_sql=where_sql_on_temp)
            temp_dict = dict()
            for temp_record in temp_records:
                staz, day = temp_record[:2]
                if staz not in temp_dict:
                    temp_dict[staz] = []
                temp_dict[staz].append(day)
            sql_fields = "cod_staz, data_i, (prec24).val_tot"
            results = querying.select_prec_records(
                conn, sql_fields, stations_ids=list(temp_dict.keys()), exclude_values=(0,))
            results = [r for r in results if r[1] in temp_dict[r[0]]]
        else:
            raise NotImplementedError('check10 not implemented for variable %s' % var)
    msg = "'controllo z-score checks precipitazione' for variable %s (%s)" \
          % (var, where_sql_on_temp)
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    to_be_resetted = []
    half_window = (window_days - 1) // 2
    for station, station_records in itertools.groupby(results, group_by_station):
        check_date = datetime(2000, 1, 1)  # first of a leap year
        for i in range(366):
            reference_days = [check_date + timedelta(n)
                              for n in range(-half_window, half_window+1)]
            day_month_tuples = [(day.day, day.month) for day in reference_days]
            sample_records = querying.filter_by_day_patterns(station_records, day_month_tuples)
            if len(sample_records) >= min_num:
                sample_values = np.array([r[2] for r in sample_records])
                percentile_limit = np.percentile(sample_values, percentile) * times_perc
                check_records = querying.filter_by_day_patterns(
                    sample_records, [check_date.month, check_date.day])
                for check_record in check_records:
                    if check_record[2] > percentile_limit:
                        to_be_resetted.append(check_record)
            check_date += timedelta(1)

    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check11(conn, stations_ids, var, max_diff=18, flag=-27, use_records=None):
    """
    Check "controllo jump checks" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param var: name of the variable to check
    :param max_diff: module of the threshold increase between consecutive days
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if var == 'Tmax':
            sql_fields = "cod_staz, data_i, (tmxgg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg'], sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check11 not implemented for variable %s' % var)
    msg = "'controllo jump checks' for variable %s (max diff: %s)" % (var, max_diff)
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    to_be_resetted = []
    for station, station_records in itertools.groupby(results, group_by_station):
        prev1_record = None
        prev2_record = None
        # load previous 2:
        for i, station_record in enumerate(station_records):
            prev2_record = prev1_record
            prev1_record = station_record
            if i == 1:
                break
        for station_record in station_records:
            day, value = station_record[1:3]
            prev1_rec_day, prev1_rec_value = prev1_record[1:3]
            prev2_rec_day, prev2_rec_value = prev2_record[1:3]
            if prev1_rec_day == day - timedelta(1) and prev2_rec_day == day - timedelta(2):
                if abs(prev1_rec_value - prev2_rec_value) > max_diff and \
                        abs(prev1_rec_value - value) > max_diff:
                    to_be_resetted.append(prev1_record)
            prev2_record = prev1_record
            prev1_record = station_record

    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, var, flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs


def check12(conn, stations_ids, variables, min_diff=5, flag=-29, use_records=None):
    """
    Check "controllo TMAX < TMIN" for the `var`.

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param variables: name of the variables to check
    :param min_diff: threshold difference between vars[0] and vars[1]
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if list(variables) == ['Tmax', 'Tmin']:
            sql_fields = "cod_staz, data_i, (tmxgg).val_md, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, ['tmxgg', 'tmngg'], sql_fields, stations_ids)
        else:
            raise NotImplementedError('check11 not implemented for variables %s' % repr(vars))
    msg = "'controllo jump checks' for variables %s (min diff: %s)" % (repr(vars), min_diff)
    msgs.append(msg)

    to_be_resetted = []
    i = 0
    for i, result in enumerate(results):
        if result[2] - result[3] < min_diff:
            to_be_resetted.append(result)

    msg = "Checked %i records" % i + 1
    msgs.append(msg)
    msg = "Found %i records with flags to be reset" % len(to_be_resetted)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmax', flag)
    db_utils.set_temp_flags(conn, to_be_resetted, 'Tmin', flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs

