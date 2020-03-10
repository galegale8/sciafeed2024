"""
This module contains the functions and utilities to check climatologic data.
`data` is a python structure, of kind:
::

    [(stat_props, datetime object, par_code, par_value, flag), ...]

- stat_props: a python dictionary containing information to identify a station
- datetime object: a datetime.datetime instance that is the time of measurement
- par_code: the parameter code
- par_value: the parameter value
- flag: a boolean flag to consider valid or not the value
"""
import itertools


def data_internal_consistence_check(input_data, limiting_params=None):
    """
    Get the internal consistent check for an input data object.
    It assumes that `input_data` has an agreed structure, i.e.:
    ::
    [(stat_props, date obj, par_code, par_value, par_flag), ....]

    Return the list of error messages, and the data with flags modified.
    `limiting_params` is a dict {code: (code_min, code_max), ...}.

    :param input_data: an object containing measurements
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (err_msgs, data_modified)
    """
    if limiting_params is None:
        limiting_params = dict()
    err_msgs = []
    data_modified = []
    for (stat_props, row_date), measures in itertools.groupby(input_data, key=lambda x: x[:2]):
        # here measures have all the same station and date
        props = {m[2]: (m[3], m[4]) for m in measures}
        for par_code, (par_value, par_flag) in props.items():
            if par_code not in limiting_params or not par_flag or par_value is None:
                # no check if the parameter is flagged invalid or no in the limiting_params
                measure = [stat_props, row_date, par_code, par_value, par_flag]
                data_modified.append(measure)
                continue
            par_code_min, par_code_max = limiting_params[par_code]
            par_code_min_value, par_code_min_flag = props[par_code_min]
            par_code_max_value, par_code_max_flag = props[par_code_max]
            # check minimum
            if par_code_min_flag and par_code_min_value is not None:
                par_code_min_value = float(par_code_min_value)
                if par_value < par_code_min_value:
                    par_flag = False
                    # TODO: an ID on the string when it comes from the db
                    err_msg = "The values of %r and %r are not consistent" \
                              % (par_code, par_code_min)
                    err_msgs.append(err_msg)
            # check maximum
            if par_code_max_flag and par_code_max_value is not None:
                par_code_max_value = float(par_code_max_value)
                if par_value > par_code_max_value:
                    par_flag = False
                    # TODO: an ID on the string when it comes from the db
                    err_msg = "The values of %r and %r are not consistent" \
                              % (par_code, par_code_max)
                    err_msgs.append(err_msg)
            measure = [stat_props, row_date, par_code, par_value, par_flag]
            data_modified.append(measure)
    return err_msgs, data_modified


def data_weak_climatologic_check(input_data, parameters_thresholds=None):
    """
    Get the weak climatologic check for an input data object, i.e. it flags
    as invalid a value if it is out of a defined range.
    It assumes that `input_data` has an agreed structure i.e.:
    ::
    [(stat_props, date obj, par_code, par_value, par_flag), ....]

    Return the list of error messages, and the resulting data with flags updated.
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
        stat_props, row_date, par_code, par_value, par_flag = measure
        if par_code not in parameters_thresholds or not par_flag or par_value is None:
            # no check if limiting parameters are flagged invalid or value is None
            data_modified.append(measure)
            continue
        min_threshold, max_threshold = map(float, parameters_thresholds[par_code])
        if not (min_threshold <= par_value <= max_threshold):
            par_flag = False
            err_msg = "The value of %r is out of range [%s, %s]" \
                      % (par_code, min_threshold, max_threshold)
            err_msgs.append(err_msg)
        new_measure = [stat_props, row_date, par_code, par_value, par_flag]
        data_modified.append(new_measure)
    return err_msgs, data_modified
