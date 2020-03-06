
import itertools

from sciafeed import formats


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


# entry point candidate
def do_file_weak_climatologic_check(filepath, parameters_filepath, format_label=None):
    """
    Get the weak climatologic check for an input file, i.e. it flags
    as invalid a value if it is out of a defined range.
    Only rightly formatted rows are considered (see function `formats.validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.
    `parameters_thresholds` is a dict {code: (min, max), ...}.

    :param filepath: path to the input file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param format_label: the label string (according to `sciafeed.formats.FORMATS`)
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    if not format_label:
        format_label, format_module = formats.guess_format(filepath)
    else:
        format_module = dict(formats.FORMATS).get(format_label)
    fmt_errors = formats.validate_format(filepath, parameters_filepath, format_label)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    stat_props, extra_metadata = formats.extract_metadata(filepath, format_label)
    load_parameter_f = getattr(format_module, 'load_parameter_file')
    load_parameter_thresholds_f = getattr(format_module, 'load_parameter_thresholds')
    parse_row_f = getattr(format_module, 'parse_row')
    parameters_map = load_parameter_f(parameters_filepath)
    parameters_thresholds = load_parameter_thresholds_f(parameters_filepath)
    err_msgs = []
    data = []
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_errors_dict:
                continue
            row_data = parse_row_f(row, parameters_map=parameters_map, stat_props=stat_props)
            err_msgs_row, row_data = data_weak_climatologic_check(
                row_data, parameters_thresholds)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            data.extend(row_data)
    ret_value = err_msgs, data
    return ret_value


# entry point candidate
def do_file_internal_consistence_check(filepath, parameters_filepath,
                                       limiting_params=None, format_label=None):
    """
    Get the internal consistence check for a file.
    Only rightly formatted rows are considered (see function `validate_format`).
    Return the list of tuples (row index, error message), and the resulting data with flags
    updated.

    :param filepath: path to the input file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :param format_label: the label string (according to `sciafeed.formats.FORMATS`)
    :return: ([..., (row index, err_msg), ...], data_parsed)
    """
    if not format_label:
        format_label, format_module = formats.guess_format(filepath)
    else:
        format_module = dict(formats.FORMATS).get(format_label)
    fmt_errors = formats.validate_format(filepath, parameters_filepath, format_label)
    fmt_errors_dict = dict(fmt_errors)
    if 0 in fmt_errors_dict:
        # global formatting error: no parsing
        return fmt_errors, None
    if limiting_params is None:
        limiting_params = dict()
    stat_props, extra_metadata = formats.extract_metadata(filepath, format_label)
    load_parameter_f = getattr(format_module, 'load_parameter_file')
    parse_row_f = getattr(format_module, 'parse_row')
    parameters_map = load_parameter_f(parameters_filepath)
    err_msgs = []
    data = []
    with open(filepath) as fp:
        for i, row in enumerate(fp, 1):
            if not row.strip() or i in fmt_errors_dict:
                continue
            row_data = parse_row_f(row, parameters_map, stat_props=stat_props)
            err_msgs_row, row_data = data_internal_consistence_check(row_data, limiting_params)
            for err_msg_row in err_msgs_row:
                err_msgs.append((i, err_msg_row))
            data.extend(row_data)
    ret_value = err_msgs, data
    return ret_value
