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
import math
import operator
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


def check1(records, len_threshold=180, flag=-12, val_index=2):
    """
    Check "controllo valori ripetuti = 0".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    msgs = []
    msg = "starting check (parameters: %s, %s, %s)" % (len_threshold, flag, val_index)
    msgs.append(msg)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0]
    num_invalid_records = 0
    group_by_station = operator.itemgetter(0)

    val_getter = operator.itemgetter(val_index)

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for value, value_records in itertools.groupby(station_records, val_getter):
            value_records = list(value_records)
            if value == 0 and len(value_records) >= len_threshold:
                for v in value_records:
                    v[val_index+1] = flag
                    num_invalid_records += 1

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check2(records, len_threshold=20, flag=-13, val_index=2, exclude_values=()):
    """
    Check "controllo valori ripetuti" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param exclude_values: iterable of values to be excluded from the check for the input records
    :return: (new_records, msgs)
    """
    msgs = []
    msg = "starting check (parameters: %s, %s, %s)" % (len_threshold, flag, val_index)
    msgs.append(msg)

    group_by_station = operator.itemgetter(0)
    val_getter = operator.itemgetter(val_index)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records
                      if r[val_index+1] > 0 and r[val_index] not in exclude_values]
    num_invalid_records = 0

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for value, value_records in itertools.groupby(station_records, val_getter):
            value_records = list(value_records)
            if value not in exclude_values and len(value_records) >= len_threshold:
                for v in value_records:
                    v[val_index+1] = flag
                    num_invalid_records += 1

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check3(records, min_not_null=None, flag=-15, val_index=2):
    """
    Check "controllo mesi duplicati (mesi differenti appartenenti allo stesso anno)".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param min_not_null: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    # TODO: ASK: check in the same time series both prec and temp, or do
    #       a sequence of different checks?
    # TODO: ASK: of course, grouping by station, isn't it?
    msgs = []
    msg = "starting check (parameters: %s, %s, %s)" % (min_not_null, flag, val_index)
    msgs.append(msg)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    invalid_records = []

    group_by_station = operator.itemgetter(0)
    val_getter = operator.itemgetter(val_index)

    def group_by_year(record):
        return record[1].year

    def group_by_month(record):
        return record[1].month

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for year, year_records in itertools.groupby(station_records, group_by_year):
            year_values_dict = dict()
            year_records_dict = dict()
            for month, month_records in itertools.groupby(year_records, group_by_month):
                month_records = list(month_records)
                year_records_dict[month] = month_records
                month_values = {g[1].day: val_getter(g) for g in month_records}
                year_values_dict[month] = month_values
            all_month_values = list(year_values_dict.values())
            for month, month_values in year_values_dict.items():
                if all_month_values.count(month_values) > 1 and \
                         (min_not_null is None or len(month_values) >= min_not_null):
                    invalid_records += year_records_dict[month]

    for invalid_record in invalid_records:
        invalid_record[val_index+1] = flag

    num_invalid_records = len(invalid_records)
    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check4(records, min_not_null=None, flag=-17, val_index=2):
    """
    Check "controllo mesi duplicati (mesi uguali appartenenti ad anni differenti)".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param min_not_null: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    # TODO: ASK: check in the same time series both prec and temp, or do
    #       a sequence of different checks?
    # TODO: ASK: of course, grouping by station, isn't it?
    msgs = []
    msg = "starting check (parameters: %s, %s, %s)" % (min_not_null, flag, val_index)
    msgs.append(msg)

    group_by_station = operator.itemgetter(0)
    val_getter = operator.itemgetter(val_index)
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    invalid_records = []

    def group_by_year(record):
        return record[1].year

    def group_by_month(record):
        return record[1].month

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        months_values_dict = dict()
        months_records_dict = dict()
        for year, year_records in itertools.groupby(station_records, group_by_year):
            for month, month_records in itertools.groupby(year_records, group_by_month):
                month_records = list(month_records)
                if month not in months_values_dict:
                    months_values_dict[month] = dict()
                    months_records_dict[month] = dict()
                month_values = {g[1].day: val_getter(g) for g in month_records}
                months_values_dict[month][year] = month_values
                months_records_dict[month][year] = month_records

        for month in months_values_dict:
            months_values_for_years = list(months_values_dict[month].values())
            for year in months_values_dict[month]:
                months_values = months_values_dict[month][year]
                if months_values_for_years.count(months_values) > 1 and \
                        (min_not_null is None or len(months_values) >= min_not_null):
                    invalid_records += months_records_dict[month][year]

    for invalid_record in invalid_records:
        invalid_record[val_index+1] = flag
    num_invalid_records = len(invalid_records)
    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check5(records, len_threshold=10, flag=-19):
    """
    Check "controllo TMAX=TMIN".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param len_threshold: minimum lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :return: (new_records, msgs)
    """
    msgs = []
    msg = "starting check (parameters: %s, %s)" % (len_threshold, flag)
    msgs.append(msg)

    group_by_station = operator.itemgetter(0)
    val_getter = lambda x: x[2] == x[4]
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[3] > 0 and r[5] > 0 and
                      r[2] is not None and r[4 is not None]]
    num_invalid_records = 0

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for are_the_same, value_records in itertools.groupby(station_records, val_getter):
            if are_the_same:
                value_records = list(value_records)
                if len(value_records) >= len_threshold:
                    for v in value_records:
                        v[3] = flag
                        v[5] = flag
                        num_invalid_records += 1

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check6(records, flag=-20):
    """
    Check "controllo TMAX=TMIN=0"
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param flag: the value of the flag to set for found records
    :return: (new_records, msgs)
    """
    msgs = []
    msg = "starting check (parameters: %s)" % flag
    msgs.append(msg)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[3] > 0 and r[5] > 0]

    num_invalid_records = 0
    for record in records_to_use:
        if record[2] == record[4] == 0:
            record[3] = record[5] = flag
            num_invalid_records += 1

    msg = "Checked %s records" % len(new_records)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check7(records, min_threshold=None, max_threshold=None, flag=-21, val_index=2):
    """
    Check "controllo world excedence" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param flag: the value of the flag to set for found records
    :param min_threshold: minimum value in the check
    :param max_threshold: maximum value in the check
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    msgs = []
    msg = "starting check (parameters: %s, %s, %s, %s)" \
          % (min_threshold, max_threshold, flag, val_index)
    msgs.append(msg)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    num_invalid_records = 0
    val_getter = operator.itemgetter(val_index)

    exclude_condition = lambda r: False
    if min_threshold is not None:
        exclude_condition = lambda r: val_getter(r) <= min_threshold
    elif max_threshold is not None:
        exclude_condition = lambda r: val_getter(r) >= max_threshold
    if min_threshold is not None and max_threshold is not None:
        exclude_condition = lambda r: val_getter(r) <= min_threshold \
                                      or val_getter(r) >= max_threshold

    for record in records_to_use:
        if exclude_condition(record):
            record[val_index+1] = flag
            num_invalid_records += 1

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def gap_top_checks(terms, threshold):
    """
    sort a sequence of terms, checking the gaps between 2 consecutive values.
    Return the first term after the gap where the gap is greater than the threshold.
    If not found, return math.inf

    :param terms: list of tuple (value, record)
    :param threshold: the threshold to overcome
    :return: list of terms
    """
    terms = sorted(terms)
    for index in range(len(terms) - 1):
        if terms[index + 1] - terms[index] > threshold:
            return terms[index + 1]
    return math.inf


def gap_bottom_checks(terms, threshold):
    """
    sort a sequence of terms reverse, checking the gaps between 2 consecutive values.
    Return the first term after the gap where the gap is lower than the threshold.
    If not found, return -math.inf

    :param terms: list of tuple (value, record)
    :param threshold: the threshold to check
    :return: list of records
    """
    terms = sorted(terms, reverse=True)
    for index in range(len(terms) - 1):
        if terms[index] - terms[index + 1] > threshold:
            return terms[index + 1]
    return -math.inf


def check8(records, threshold=None, split=False, flag_sup=-23, flag_inf=-24, val_index=2):
    """
    Check "controllo gap checks" for the input records.
    If split = False: case of "controllo gap checks  precipitazione" (see documentation)
    If split = True: case of "controllo gap checks temperatura" (see documentation)
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param threshold: value of the threshold in the check
    :param split: if False (default), don't split by median, and consider only flag_sup
    :param flag_sup: value of the flag to be set for found records with split=False, or with
                     split=True for the top part of the split
    :param flag_inf: value of the flag to be set for found records with split=True for the
                     bottom part of the split
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    # TODO: ASK to accumulate distribution on all years of a month, or to do a month a time?
    # TODO: ASK split by median: '>=' and '<=', or '>' and '<'?
    msgs = []
    msg = "starting check (parameters: %s, %s, %s, %s, %s)" \
          % (threshold, split, flag_sup, flag_inf, val_index)
    msgs.append(msg)

    num_invalid_records_sup = 0
    num_invalid_records_inf = 0
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    val_getter = operator.itemgetter(val_index)
    group_by_station = operator.itemgetter(0)

    def group_by_year(r):
        return r[1].year

    def group_by_month(r):
        return r[1].month

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        station_records = list(station_records)
        months_values_dict = dict()
        # first loop to load months_values_dict
        for year, year_records in itertools.groupby(station_records, group_by_year):
            for month, month_records in itertools.groupby(year_records, group_by_month):
                if month not in months_values_dict:
                    months_values_dict[month] = []
                month_values = [val_getter(g) for g in month_records]
                months_values_dict[month].extend(month_values)

        # second loop to compute sup and inf thresholds for each month
        invalid_values_flag_sup = dict()
        invalid_values_flag_inf = dict()
        for month, month_values in months_values_dict.items():
            if split:
                median = statistics.median(month_values)
                top_values = [g for g in month_values if g >= median]
                bottom_values = [g for g in month_values if g <= median]
            else:
                top_values = month_values
                bottom_values = []
            invalid_values_flag_sup[month] = gap_top_checks(top_values, threshold)
            invalid_values_flag_inf[month] = gap_bottom_checks(bottom_values, threshold)

        # third loop to filter the invalid
        for station_record in station_records:
            threshold_sup = invalid_values_flag_sup.get(station_record[1].month, math.inf)
            threshold_inf = invalid_values_flag_inf.get(station_record[1].month, -math.inf)
            value = val_getter(station_record)
            if value >= threshold_sup:
                station_record[val_index+1] = flag_sup
                num_invalid_records_sup += 1
            elif value <= threshold_inf:
                station_record[val_index+1] = flag_inf
                num_invalid_records_inf += 1

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records_sup, flag_sup)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records_inf, flag_inf)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check9(records, num_dev_std=6, window_days=15, min_num=100, flag=-25, val_index=2):
    """
    Check "controllo z-score checks temperatura"
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param num_dev_std: times of the standard deviation to be considered in the check
    :param window_days: the time window to consider (in days)
    :param min_num: the minimum size of the values to be found inside the window
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    if not (window_days % 2):
        raise ValueError('window_days must be odd')

    msgs = []
    msg = "starting check (parameters: %s, %s, %s, %s, %s)" \
          % (num_dev_std, window_days, min_num, flag, val_index)
    msgs.append(msg)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    num_invalid_records = 0
    half_window = (window_days - 1) // 2
    group_by_station = operator.itemgetter(0)

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        check_date = datetime(2000, 1, 1)  # first of a leap year
        station_records = list(station_records)
        for i in range(366):
            reference_days = [check_date + timedelta(n)
                              for n in range(-half_window, half_window+1)]
            day_month_tuples = [(day.day, day.month) for day in reference_days]
            sample_records = querying.filter_by_day_patterns(station_records, day_month_tuples)
            if len(sample_records) >= min_num:
                sample_values = [r[val_index] for r in sample_records]
                average = statistics.mean(sample_values)
                dev_std_limit = statistics.stdev(sample_values) * num_dev_std
                check_records = querying.filter_by_day_patterns(
                    sample_records, [(check_date.day, check_date.month), ])
                for check_record in check_records:
                    if abs(check_record[val_index] - average) > dev_std_limit:
                        check_record[val_index+1] = flag
                        num_invalid_records += 1
            check_date += timedelta(1)

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def select_positive_average_temp(record):
    # TODO: to be used to filter temperature records before check10
    value, flag = record[6], record[7]
    if value is None or flag < 0 or (flag >= 0 and value >= 0):
        return True
    return False


def check10(records, filter_days, times_perc=9, percentile=95, window_days=29, min_num=20,
            flag=-25, val_index=2):
    """
    Check "controllo z-score checks precipitazione [ghiaccio]".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param filter_days: dictionary of kind {station: [days to consider]}
    :param times_perc: number of times of the percentile to create the limit
    :param percentile: percentile value of the distribution to use
    :param window_days: the time window to consider (in days)
    :param min_num: the minimum size of the values to be found inside the window
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    if not (window_days % 2):
        raise ValueError('window_days must be odd')
    msgs = []
    msg = "starting check (parameters: %s, %s, %s, %s, %s, %s)" \
          % (times_perc, percentile, window_days, min_num, flag, val_index)
    msgs.append(msg)

    group_by_station = operator.itemgetter(0)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0
                      and r[val_index] not in (None, 0)
                      and r[1] in filter_days.get(r[0], [])]
    num_invalid_records = 0
    half_window = (window_days - 1) // 2

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        check_date = datetime(2000, 1, 1)  # first of a leap year
        station_records = list(station_records)
        for i in range(366):
            reference_days = [check_date + timedelta(n)
                              for n in range(-half_window, half_window+1)]
            day_month_tuples = [(day.day, day.month) for day in reference_days]
            sample_records = querying.filter_by_day_patterns(station_records, day_month_tuples)
            if len(sample_records) >= min_num:
                sample_values = np.array([float(r[val_index]) for r in sample_records])
                percentile_limit = np.percentile(sample_values, percentile) * times_perc
                check_records = querying.filter_by_day_patterns(
                    sample_records, [(check_date.day, check_date.month)])
                for check_record in check_records:
                    if check_record[val_index] > percentile_limit:
                        check_record[val_index+1] = flag
                        num_invalid_records += 1
            check_date += timedelta(1)

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


def check11(records, max_diff=18, flag=-27, val_index=2):
    """
    Check "controllo jump checks" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are:
    the list of records (with flag updated), the list of log messages.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param max_diff: module of the threshold increase between consecutive days
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :return: (new_records, msgs)
    """
    msgs = []
    msg = "starting check (parameters: %s, %s, %s)" % (max_diff, flag, val_index)
    msgs.append(msg)

    group_by_station = operator.itemgetter(0)
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    num_invalid_records = 0

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        prev1_record = None
        prev2_record = None
        # load previous 2:
        for i, station_record in enumerate(station_records):
            prev2_record = prev1_record
            prev1_record = station_record
            if i == 1:
                break
        for station_record in station_records:
            day, value = station_record[1], station_record[val_index]
            prev1_rec_day, prev1_rec_value = prev1_record[1], prev1_record[val_index]
            prev2_rec_day, prev2_rec_value = prev2_record[1], prev2_record[val_index]
            if prev1_rec_day == day - timedelta(1) and prev2_rec_day == day - timedelta(2):
                if abs(prev1_rec_value - prev2_rec_value) > max_diff and \
                        abs(prev1_rec_value - value) > max_diff:
                    prev1_record[val_index+1] = flag
                    num_invalid_records += 1
            prev2_record = prev1_record
            prev1_record = station_record

    msg = "Checked %s records" % len(records_to_use)
    msgs.append(msg)
    msg = "Found %s records with flags reset to %s" % (num_invalid_records, flag)
    msgs.append(msg)
    msg = "Check completed"
    msgs.append(msg)
    return new_records, msgs


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
            raise NotImplementedError('check12 not implemented for variables %s' % repr(vars))
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


def check13(conn, stations_ids, variables, operators, jump=35, flag=-31, use_records=None):
    """
    Check "controllo dtr (diurnal temperature range)".
    Operators is applied in the formula:
        operator2(variables[0], operator1(variables[1]) + jump)
    example1:
        operator1, operator2 = max, operator.ge
        variables = [Tmax, Tmin]
        jump = +35
        -->
        Tmax >= min(Tmin) + 35
    example2:
        operator1, operator2 = min, operator.le
        variables = [Tmin, Tmax]
        jump = -35
        -->
        Tmin <= min(Tmax) - 35

    :param conn: db connection object
    :param stations_ids: list of stations id where to do the check
    :param variables: name of the variables to check
    :param operators: couple of operators to apply in
    :param jump: the jump to apply in the formula
    :param flag: the value of the flag to set for found records
    :param use_records: force check on these records (used for test)
    :return: list of log messages
    """
    msgs = []
    results = use_records
    if not use_records:
        if list(variables) == ['Tmax', 'Tmin']:
            sql_fields = "cod_staz, data_i, (tmxgg).val_md, (tmngg).val_md"
        elif list(variables) == ['Tmin', 'Tmax']:
            sql_fields = "cod_staz, data_i, (tmngg).val_md, (tmxgg).val_md"
        else:
            raise NotImplementedError('check13 not implemented for variables %s' % repr(vars))
        results = querying.select_temp_records(conn, ['tmxgg', 'tmngg'], sql_fields, stations_ids)
    msg = "'controllo dtr (diurnal temperature range)' for variables %s" % repr(vars)
    msgs.append(msg)

    def group_by_station(record):
        return record.cod_staz

    to_be_resetted_var1 = []
    to_be_resetted_var2 = []
    operator1, operator2 = operators
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
            day, val1, val2 = station_record[1:4]
            prev1_rec_day, prev1_rec_val1, prev1_rec_val2 = prev1_record[1:4]
            prev2_rec_day, prev2_rec_val1, prev2_rec_val2 = prev2_record[1:4]
            if prev1_rec_day == day - timedelta(1) and prev2_rec_day == day - timedelta(2):
                if operator2(prev1_rec_val1,
                             operator1(prev2_rec_val2, prev1_rec_val2, val2) + jump):
                    to_be_resetted_var1.append(prev1_record)
                    to_be_resetted_var2.append(prev2_record)
                    to_be_resetted_var2.append(prev1_record)
                    to_be_resetted_var2.append(station_record)
            prev2_record = prev1_record
            prev1_record = station_record

    msg = "Found %i records with flags to be reset" \
          % len(to_be_resetted_var1) + len(to_be_resetted_var2)
    msgs.append(msg)
    msg = "Resetting flags to value %s..." % flag
    msgs.append(msg)
    db_utils.set_temp_flags(conn, to_be_resetted_var1, variables[0], flag)
    db_utils.set_temp_flags(conn, to_be_resetted_var2, variables[1], flag)
    msg = "Check completed"
    msgs.append(msg)
    return msgs
