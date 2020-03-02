"""
This module contains the functions and utilities common to all SCIA data formats
"""
from sciafeed import arpa19, arpa21, arpaer, arpafvg, noaa, rmn, trentino


FORMAT_MODULES = (
    ('ARPA-19', arpa19),
    ('ARPA-21', arpa21),
    ('ARPA-FVG', arpafvg),
    ('ARPA-ER', arpaer),
    ('RMN', rmn),
    ('TRENTINO', trentino),
    ('NOAA', noaa)
)


def guess_format(filepath):
    """
    Try to guess the format of a file located at `filepath`. It uses (if exists) the
    function 'is_format_compliant' of the modules.
    Return the tuple (label of the format, python module of the format).

    :param filepath: file path of the file to guess the format of
    :return: (label of the format, python module of the format)
    """
    for format_label, format_module in FORMAT_MODULES:
        is_format_compliant = getattr(format_module, 'is_format_compliant', lambda f: False)
        if is_format_compliant(filepath):
            break
    else:  # never gone on break
        return 'Unknown', None
    return format_label, format_module


# entry point candidate
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
    format_label, format_module = guess_format(in_filepath)
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
    parse_and_check = getattr(format_module, 'parse_and_check')
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
        write_data = getattr(format_module, 'write_data')
        write_data(data_parsed, outdata_filepath)
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
