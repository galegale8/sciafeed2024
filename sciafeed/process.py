"""
This module contains functions and utilities that involve more components of sciafeed.
"""
from sciafeed import checks
from sciafeed import export
from sciafeed import formats


def parse_and_check(filepath, parameters_filepath, limiting_params=None, format_label=None):
    """
    Read a file located at `filepath`, and parse data inside it, doing
    - format validation
    - weak climatologic check
    - internal consistence check
    Return the tuple (err_msgs, parsed data) where `err_msgs` is the list of tuples
    (row index, error message) of the errors found.

    :param filepath: path to the input file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :param format_label: the label string (according to `sciafeed.formats.FORMATS`)
    :return: (err_msgs, data_parsed)
    """
    if not format_label:
        format_label, format_module = formats.guess_format(filepath)
    else:
        format_module = dict(formats.FORMATS).get(format_label)
    if not format_module:
        return [(0, "the file has unknown format")], None
    load_parameter_f = getattr(format_module, 'load_parameter_file')
    load_parameter_thresholds_f = getattr(format_module, 'load_parameter_thresholds')
    parse_row_f = getattr(format_module, 'parse_row')
    rows_generator_f = getattr(format_module, 'rows_generator')

    stat_props, extra_metadata = formats.extract_metadata(
        filepath, parameters_filepath, format_label)
    par_map = load_parameter_f(parameters_filepath)
    par_thresholds = load_parameter_thresholds_f(parameters_filepath)

    err_msgs = []
    data = []
    fmt_err_msgs = formats.validate_format(filepath, parameters_filepath, format_label)
    err_msgs.extend(fmt_err_msgs)
    fmt_err_indexes_dict = dict(fmt_err_msgs)
    if 0 in fmt_err_indexes_dict:
        # global error, no parsing
        return err_msgs, None

    for i, row in rows_generator_f(filepath, par_map, stat_props, extra_metadata):
        if not row or i in fmt_err_indexes_dict:
            continue
        row_data = parse_row_f(row, par_map, stat_props=stat_props)
        err_msgs1_row, row_data = checks.data_weak_climatologic_check(
            row_data, par_thresholds)
        err_msgs2_row, row_data = checks.data_internal_consistence_check(
            row_data, limiting_params)
        data.extend(row_data)
        err_msgs.extend([(i, err_msg1_row) for err_msg1_row in err_msgs1_row])
        err_msgs.extend([(i, err_msg2_row) for err_msg2_row in err_msgs2_row])
    ret_value = err_msgs, data
    return ret_value


def make_report(in_filepath, out_filepath=None, outdata_filepath=None,
                parameters_filepath=None, limiting_params=None):
    """
    Read a file located at `in_filepath` and generate a report on the parsing.
    If `out_filepath` is defined, the report string is written on a file.
    If the path `outdata_filepath` is defined, a file with the data parsed is created at the path.
    Return the list of report strings and the data parsed.

    :param in_filepath: input file
    :param out_filepath: path of the output report
    :param outdata_filepath: path of the output file containing data
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param limiting_params: dictionary of limiting parameters for each parameter code
    :return: (report_strings, data_parsed)
    """
    format_label, format_module = formats.guess_format(in_filepath)
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

    err_msgs, data_parsed = parse_and_check(
        in_filepath, parameters_filepath=parameters_filepath, limiting_params=limiting_params)
    if not err_msgs:
        msg = "No errors found"
        msgs.append(msg)
    else:
        for row_index, err_msg in err_msgs:
            msgs.append("Row %s: %s" % (row_index, err_msg))

    if outdata_filepath:
        msgs.append('')
        export.export2csv(data_parsed, outdata_filepath)
        msg = "Data saved on file %r" % outdata_filepath
        msgs.append(msg)

    msgs.append('')
    msg = "END OF ANALYSIS OF %s FILE" % format_label
    msgs.append(msg)
    msgs.append('=' * len(msg))
    msgs.append('')

    if out_filepath:
        with open(out_filepath, 'a') as fp:
            for msg in msgs:
                fp.write(msg + '\n')

    return msgs, data_parsed
