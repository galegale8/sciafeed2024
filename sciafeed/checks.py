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
import itertools

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
    msg = "Checked %i records" % i
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
                conn, 'tmxgg', sql_fields, stations_ids, exclude_values=exclude_values)
            field_to_check = 'val_md'
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, 'tmngg', sql_fields, stations_ids, exclude_values=exclude_values)
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
    msg = "Checked %i records" % i
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
                conn, 'tmxgg', sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, 'tmngg', sql_fields, stations_ids)
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
                conn, 'tmxgg', sql_fields, stations_ids)
        elif var == 'Tmin':
            sql_fields = "cod_staz, data_i, (tmngg).val_md"
            results = querying.select_temp_records(
                conn, 'tmngg', sql_fields, stations_ids)
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
