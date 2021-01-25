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
import calendar
from datetime import datetime, timedelta
import itertools
import logging
import math
import operator
import statistics

import numpy as np

from sciafeed import LOG_NAME
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


def check1(records, len_threshold=180, flag=-12, val_index=2, logger=None):
    """
    Check "controllo valori ripetuti = 0".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated)

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (len_threshold, flag, val_index))

    new_records = [r[:] for r in records if r[val_index] is not None]
    records_to_use = [r for r in new_records if r[val_index+1] > 0]
    num_invalid_records = 0
    group_by_station = operator.itemgetter(0)

    val_getter = operator.itemgetter(val_index)

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for value, value_records in itertools.groupby(station_records, val_getter):
            value_records = list(value_records)
            if value == 0 and len(value_records) >= len_threshold:
                for v in value_records:
                    if v[val_index+1] != 5:
                        v[val_index+1] = flag
                        num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check2(records, len_threshold=20, flag=-13, val_index=2, exclude_values=(), filter_funct=None,
           logger=None):
    """
    Check "controllo valori ripetuti" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated)

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param len_threshold: lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param exclude_values: iterable of values to be excluded from the check for the input records
    :param filter_funct: if not None, filter function to consider records to use
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (len_threshold, flag, val_index))

    group_by_station = operator.itemgetter(0)
    val_getter = operator.itemgetter(val_index)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records
                      if r[val_index+1] > 0 and r[val_index] not in exclude_values]
    if filter_funct is not None:
        records_to_use = [r for r in records_to_use if filter_funct(r)]
    num_invalid_records = 0

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for value, value_records in itertools.groupby(station_records, val_getter):
            value_records = list(value_records)
            if value not in exclude_values and len(value_records) >= len_threshold:
                for v in value_records:
                    if v[val_index+1] != 5:
                        v[val_index+1] = flag
                        num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def months_comparison(month_values1, month_values2, month1, month2, year1, year2,
                      min_not_zero=None):
    """
    Do a comparison of the values of 2 dictionaries of measures in a month.
    The comparison considers the max days applicable for both the months.
    The comparison return 1 if the months values are the same, 0 if not, and -1 if not applicable.

    :param month_values1: {day: value} for the first month
    :param month_values2: {day: value} for the second month
    :param month1: index (1 to 12) of the first month
    :param month2: index (1 to 12) of the second month
    :param year1: year of the first month
    :param year2: year of the second month
    :param min_not_zero: if not None, minimum number of values != 0 to make comparison applicable
    :return: 1, 0 or -1
    """
    if min_not_zero is not None:
        len1 = len([v for v in month_values1.values() if v != 0])
        len2 = len([v for v in month_values2.values() if v != 0])
        if len1 < min_not_zero or len2 < min_not_zero:
            return -1
    max_day = min(calendar.monthrange(year1, month1)[1], calendar.monthrange(year2, month2)[1])
    for k in range(1, max_day+1):
        if month_values1.get(k, None) != month_values2.get(k, None):
            return 0
    return 1


def check3(records, min_not_zero=None, flag=-15, val_index=2, logger=None):
    """
    Check "controllo mesi duplicati (mesi differenti appartenenti allo stesso anno)".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param min_not_zero: minimum number of the valid values != 0 required to consider a month
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (min_not_zero, flag, val_index))

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    invalid_records = []
    num_invalid_records = 0
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

            invalid_months = []
            for month1, month2 in itertools.combinations(year_values_dict.keys(), 2):
                month_values1 = year_values_dict[month1]
                month_values2 = year_values_dict[month2]
                if months_comparison(month_values1, month_values2, month1, month2, year, year,
                                     min_not_zero=min_not_zero) > 0:
                    invalid_months.append(month1)
                    invalid_months.append(month2)
            for invalid_month in set(invalid_months):
                invalid_records += year_records_dict[invalid_month]
    for invalid_record in invalid_records:
        if invalid_record[val_index+1] != 5:
            invalid_record[val_index+1] = flag
            num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check4(records, min_not_zero=None, flag=-17, val_index=2, logger=None):
    """
    Check "controllo mesi duplicati (mesi uguali appartenenti ad anni differenti)".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param min_not_zero: minimum number of the valid values != 0 required to consider a month
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (min_not_zero, flag, val_index))

    group_by_station = operator.itemgetter(0)
    val_getter = operator.itemgetter(val_index)
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    invalid_records = []
    num_invalid_records = 0

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
                if min_not_zero is not None:
                    num_not_zero = len([v for v in months_values.values() if v != 0])
                    if num_not_zero < min_not_zero:
                        continue
                if months_values_for_years.count(months_values) > 1:
                    invalid_records += months_records_dict[month][year]

    for invalid_record in invalid_records:
        if invalid_record[val_index+1] != 5:
            invalid_record[val_index+1] = flag
            num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check5(records, len_threshold=10, flag=-19, logger=None):
    """
    Check "controllo TMAX=TMIN".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param len_threshold: minimum lenght of the consecutive zeros to find
    :param flag: the value of the flag to set for found records
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s)" % (len_threshold, flag))

    group_by_station = operator.itemgetter(0)
    val_getter = lambda x: x[2] == x[4]
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[3] > 0 and r[5] > 0 and
                      r[2] is not None and r[4] is not None]
    num_invalid_records = 0

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        for are_the_same, value_records in itertools.groupby(station_records, val_getter):
            if are_the_same:
                value_records = list(value_records)
                if len(value_records) >= len_threshold:
                    for v in value_records:
                        if v[3] != 5:
                            v[3] = flag
                            num_invalid_records += 1
                        if v[5] != 5:
                            v[5] = flag
                            num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check6(records, flag=-20, logger=None):
    """
    Check "controllo TMAX=TMIN=0"
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param flag: the value of the flag to set for found records
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s)" % flag)

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[3] > 0 and r[5] > 0]

    num_invalid_records = 0
    for record in records_to_use:
        if record[2] == record[4] == 0:
            if record[3] != 5:
                record[3] = flag
                num_invalid_records += 1
            if record[5] != 5:
                record[5] = flag
                num_invalid_records += 1

    logger.info("Checked %s records" % len(new_records))
    logger.info("Found %s flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check7(records, min_threshold=None, max_threshold=None, flag=-21, val_index=2, logger=None,
           flag_index=None):
    """
    Check "controllo world excedence" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param flag: the value of the flag to set for found records
    :param min_threshold: minimum value in the check
    :param max_threshold: maximum value in the check
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :param flag_index: index of the flag to be reset (default is val_index+1
    :return: new_records
    """
    if flag_index is None:
        flag_index = val_index + 1
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s, %s)"
                % (min_threshold, max_threshold, flag, val_index))

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[flag_index] > 0 and r[val_index] is not None]
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
            if record[flag_index] != 5:
                record[flag_index] = flag
                num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


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


def check8(records, threshold, split=False, flag_sup=-23, flag_inf=-24, val_index=2,
           exclude_zero=False, logger=None):
    """
    Check "controllo gap checks" for the input records.
    If split = False: case of "controllo gap checks  precipitazione" (see documentation)
    If split = True: case of "controllo gap checks temperatura" (see documentation)
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param threshold: value of the threshold in the check
    :param split: if False (default), don't split by median, and consider only flag_sup
    :param flag_sup: value of the flag to be set for found records with split=False, or with
                     split=True for the top part of the split
    :param flag_inf: value of the flag to be set for found records with split=True for the
                     bottom part of the split
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param exclude_zero: if True, consider only values != 0 in the computation
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s, %s, %s)"
                % (threshold, split, flag_sup, flag_inf, val_index))

    num_invalid_records_sup = 0
    num_invalid_records_inf = 0
    new_records = [r[:] for r in records]
    if exclude_zero:
        records_to_use = [
            r for r in new_records if r[val_index + 1] > 0 and r[val_index] not in (None, 0)]
    else:
        records_to_use = [
            r for r in new_records if r[val_index + 1] > 0 and r[val_index] is not None]

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
            if value >= threshold_sup and station_record[val_index+1] != 5:
                station_record[val_index+1] = flag_sup
                num_invalid_records_sup += 1
            elif value <= threshold_inf and station_record[val_index+1] != 5:
                station_record[val_index+1] = flag_inf
                num_invalid_records_inf += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records_sup, flag_sup))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records_inf, flag_inf))
    logger.info("Check completed")
    return new_records


def check9(records, num_dev_std=6, window_days=15, min_num=100, flag=-25, val_index=2,
           logger=None):
    """
    Check "controllo z-score checks temperatura"
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param num_dev_std: times of the standard deviation to be considered in the check
    :param window_days: the time window to consider (in days)
    :param min_num: the minimum size of the values to be found inside the window
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :return: new_records
    """
    if not (window_days % 2):
        raise ValueError('window_days must be odd')
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s, %s, %s)"
                % (num_dev_std, window_days, min_num, flag, val_index))

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_index+1] > 0 and r[val_index] is not None]
    num_invalid_records = 0
    half_window = (window_days - 1) // 2
    group_by_station = operator.itemgetter(0)

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        check_date = datetime(2000, 1, 1)  # first of a leap year
        station_records = list(station_records)
        # first loop to load dayname_groups
        dayname_groups = dict()
        for station_record in station_records:
            dayname = (station_record[1].day, station_record[1].month)
            dayname_groups[dayname] = dayname_groups.get(dayname, [])
            dayname_groups[dayname].append(station_record)

        for i in range(366):
            reference_days = [check_date + timedelta(n)
                              for n in range(-half_window, half_window+1)]
            day_month_tuples = ((day.day, day.month) for day in reference_days)
            sample_records = []
            for dayname in day_month_tuples:
                sample_records.extend(dayname_groups.get(dayname, []))
            if len(sample_records) >= min_num:
                sample_values = [r[val_index] for r in sample_records]
                average = np.mean(sample_values)
                dev_std_limit = np.std(sample_values, ddof=1) * num_dev_std
                check_records = dayname_groups.get((check_date.day, check_date.month), [])
                for check_record in check_records:
                    if abs(check_record[val_index] - average) > dev_std_limit \
                            and check_record[val_index+1] != 5:
                        check_record[val_index+1] = flag
                        num_invalid_records += 1
            check_date += timedelta(1)

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def get_days_with_negative_average_temp(temp_records):
    """to be used to filter temperature records in check10"""
    group_by_station = operator.itemgetter(0)
    ret_value_neg = dict()
    for station, station_records in itertools.groupby(temp_records, group_by_station):
        ret_value_neg[station] = []
        for record in station_records:
            day = record[1]
            tmax, tmax_flag, tmin, tmin_flag = record[2:6]
            if [tmax, tmax_flag, tmin, tmin_flag].count(None) == 0:
                if tmax_flag > 0 and tmin_flag > 0:
                    taverage = (tmax + tmin) / 2
                    if taverage < 0:
                        ret_value_neg[station].append(day)
    return ret_value_neg


def check10(records, temp_records, ice=False, times_perc=9, percentile=95, window_days=29,
            min_num=20, flag=-25, val_index=2, logger=None):
    """
    Check "controllo z-score checks precipitazione [ghiaccio]".
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param temp_records: list of records of temperatures
    :param ice: if True, consider only records where average temperature defined and < 0
    :param times_perc: number of times of the percentile to create the limit
    :param percentile: percentile value of the distribution to use
    :param window_days: the time window to consider (in days)
    :param min_num: the minimum size of the values to be found inside the window
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :return: new_records
    """
    if not (window_days % 2):
        raise ValueError('window_days must be odd')
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s, %s, %s, %s)"
                % (times_perc, percentile, window_days, min_num, flag, val_index))
    neg_temp_days = get_days_with_negative_average_temp(temp_records)

    group_by_station = operator.itemgetter(0)

    new_records = [r[:] for r in records]
    if ice:
        records_to_use = [r for r in new_records if r[val_index+1] > 0
                          and r[val_index] is not None and r[1] in neg_temp_days.get(r[0], [])]
    else:
        records_to_use = [r for r in new_records if r[val_index + 1] > 0
                          and r[val_index] is not None and r[1] not in neg_temp_days.get(r[0], [])]
    num_invalid_records = 0
    half_window = (window_days - 1) // 2

    for station, station_records in itertools.groupby(records_to_use, group_by_station):
        check_date = datetime(2000, 1, 1)  # first of a leap year
        station_records = list(station_records)
        # first loop to load dayname_groups
        dayname_groups = dict()
        for station_record in station_records:
            dayname = (station_record[1].day, station_record[1].month)
            dayname_groups[dayname] = dayname_groups.get(dayname, [])
            dayname_groups[dayname].append(station_record)

        for i in range(366):
            reference_days = [check_date + timedelta(n)
                              for n in range(-half_window, half_window+1)]
            day_month_tuples = ((day.day, day.month) for day in reference_days)
            sample_records = []
            for dayname in day_month_tuples:
                sample_records.extend(dayname_groups.get(dayname, []))
            sample_values = np.array(
                [float(r[val_index]) for r in sample_records if float(r[val_index]) != 0])
            if len(sample_values) >= min_num:
                percentile_limit = np.percentile(sample_values, percentile) * times_perc
                check_records = dayname_groups.get((check_date.day, check_date.month), [])
                for check_record in check_records:
                    if check_record[val_index] > percentile_limit \
                            and check_record[val_index+1] != 5:
                        check_record[val_index+1] = flag
                        num_invalid_records += 1
            check_date += timedelta(1)

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check11(records, max_diff=18, flag=-27, val_index=2, logger=None):
    """
    Check "controllo jump checks" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param max_diff: module of the threshold increase between consecutive days
    :param flag: the value of the flag to set for found records
    :param val_index: record[val_index] is the value to check, and record[val_index+1] is the flag
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (max_diff, flag, val_index))

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
                        abs(prev1_rec_value - value) > max_diff and prev1_record[val_index+1] != 5:
                    prev1_record[val_index+1] = flag
                    num_invalid_records += 1
            prev2_record = prev1_record
            prev1_record = station_record

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s records with flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check12(records, min_diff=-5, flag=-29, val_indexes=(2, 4), logger=None):
    """
    Check "controllo TMAX < TMIN" for the input records.
    Assumes all records are sorted by station, date.
    The sort order is maintained in the returned values, that are
    the list of records (with flag updated).

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param min_diff: threshold difference between vars[0] and vars[1]
    :param flag: the value of the flag to set for found records
    :param val_indexes: record[val_indexes[0]] and record[val_indexes[1]] are the values to compare
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (min_diff, flag, val_indexes))

    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_indexes[0]+1] > 0
                      and r[val_indexes[0]] is not None
                      and r[val_indexes[1]+1] > 0
                      and r[val_indexes[1]] is not None]
    num_invalid_records = 0

    for result in records_to_use:
        if result[val_indexes[0]] - result[val_indexes[1]] < min_diff:
            if result[val_indexes[0]+1] != 5:
                result[val_indexes[0]+1] = flag
                num_invalid_records += 1
            if result[val_indexes[1]+1] != 5:
                result[val_indexes[1]+1] = flag
                num_invalid_records += 1

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s flags reset to %s" % (num_invalid_records, flag))
    logger.info("Check completed")
    return new_records


def check13(records, operators, jump=35, flag=-31, val_indexes=(2, 4), logger=None):
    """
    Check "controllo dtr (diurnal temperature range)".
    Operators is applied in the formula:
    ::

        operator2(variables[0], operator1(variables[1]) + jump)

    example1:
    ::

        operator1, operator2 = max, operator.ge
        variables = [Tmax, Tmin]
        jump = +35
        -->
        Tmax >= min(Tmin) + 35

    example2:
    ::

        operator1, operator2 = min, operator.le
        variables = [Tmin, Tmax]
        jump = -35
        -->
        Tmin <= min(Tmax) - 35

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param operators: couple of operators to apply in
    :param jump: the jump to apply in the formula
    :param flag: the value of the flag to set for found records
    :param val_indexes: record[val_indexes[0]] and record[val_indexes[1]] are the values to compare
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s, %s)"
                % (repr(operators), jump, flag, val_indexes))

    group_by_station = operator.itemgetter(0)
    new_records = [r[:] for r in records]
    records_to_use = [r for r in new_records if r[val_indexes[0]+1] > 0
                      and r[val_indexes[0]] is not None
                      and r[val_indexes[1]+1] > 0
                      and r[val_indexes[1]] is not None]
    num_invalid_flags = 0
    operator1, operator2 = operators

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
            day, val1, val2 = \
                station_record[1], station_record[val_indexes[0]], station_record[val_indexes[1]]
            prev1_rec_day, prev1_rec_val1, prev1_rec_val2 = \
                prev1_record[1], prev1_record[val_indexes[0]], prev1_record[val_indexes[1]]
            prev2_rec_day, prev2_rec_val1, prev2_rec_val2 = \
                prev2_record[1], prev2_record[val_indexes[0]], prev2_record[val_indexes[1]]
            if prev1_rec_day == day - timedelta(1) and prev2_rec_day == day - timedelta(2):
                if operator2(prev1_rec_val1,
                             operator1(prev2_rec_val2, prev1_rec_val2, val2) + jump):
                    if prev1_record[val_indexes[0]+1] != 5:
                        prev1_record[val_indexes[0]+1] = flag
                        num_invalid_flags += 1
                    if prev1_record[val_indexes[1]+1] != 5:
                        prev1_record[val_indexes[1]+1] = flag
                        num_invalid_flags += 1
                    if prev2_record[val_indexes[1]+1] != 5:
                        prev2_record[val_indexes[1]+1] = flag
                        num_invalid_flags += 1
                    if station_record[val_indexes[1]+1] != 5:
                        station_record[val_indexes[1]+1] = flag
                        num_invalid_flags += 1
            prev2_record = prev1_record
            prev1_record = station_record

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s flags reset to %s" % (num_invalid_flags, flag))
    logger.info("Check completed")
    return new_records


def check_consistency(records, val_indexes, flag_index, flag=-10, logger=None):
    """
    Check consistency between values in indexes `val_indexes`.
    val_indexes = i1, i2, i3: check that record[i1] <= record[i2] <= record[i3]
    Flags index is the index of the flag to be reset.

    :param records: iterable of input records, of kind [cod_staz, data_i, ...]
    :param val_indexes: indexes of the values to be checked for consistency
    :param flag_index: the index of the flag to be set
    :param flag: the value of the flag to set for found errors
    :param val_indexes: indexes of the values to compare
    :param logger: logging object where to report actions
    :return: new_records
    """
    if logger is None:
        logger = logging.getLogger(LOG_NAME)
    logger.info("starting check (parameters: %s, %s, %s)" % (val_indexes, flag_index, flag))

    new_records = [r[:] for r in records]
    records_to_use = [
        r for r in records if r[val_indexes[0]] is not None and
        r[val_indexes[1]] is not None and r[val_indexes[2]] is not None and r[flag_index] > 0
    ]
    num_invalid_flags = 0
    for r in records_to_use:
        if not (r[val_indexes[0]] <= r[val_indexes[1]] <= r[val_indexes[2]]) \
                and r[flag_index] != 5:
            num_invalid_flags += 1
            r[flag_index] = flag

    logger.info("Checked %s records" % len(records_to_use))
    logger.info("Found %s flags reset to %s" % (num_invalid_flags, flag))
    logger.info("Check completed")
    return new_records
