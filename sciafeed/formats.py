"""
This module contains the functions and utilities common to all SCIA data formats
"""
import csv
import operator

from sciafeed import arpa19, arpa21, arpaer, arpafvg, bolzano, noaa, rmn, trentino


FORMATS = (
    ('ARPA-19', arpa19),
    ('ARPA-21', arpa21),
    ('ARPA-FVG', arpafvg),
    ('ARPA-ER', arpaer),
    ('BOLZANO', bolzano),
    ('NOAA', noaa),
    ('RMN', rmn),
    ('TRENTINO', trentino),
)


def guess_format(filepath):
    """
    Try to guess the format of a file located at `filepath`. It uses (if exists) the
    function 'is_format_compliant' of the modules.
    Return the tuple (label of the format, python module of the format).

    :param filepath: file path of the file to guess the format of
    :return: (label of the format, python module of the format)
    """
    for format_label, format_module in FORMATS:
        is_format_compliant = getattr(format_module, 'is_format_compliant', lambda f: False)
        if is_format_compliant(filepath):
            break
    else:  # never gone on break
        return 'Unknown', None
    return format_label, format_module


def export(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` of an ARPA19 file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the output of the function `parse`.

    :param data: ARPA19 file data
    :param out_filepath: output file where to write the data
    :param omit_parameters: list of the parameters to omit
    :param omit_missing: if False, include also values marked as missing
    """
    fieldnames = ['station', 'latitude', 'date', 'parameter', 'value', 'valid']
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for measure in sorted(data, key=operator.itemgetter(1)):
            stat_props, current_date, par_code, par_value, par_flag = measure
            if par_code in omit_parameters:
                continue
            if omit_missing and par_value is None:
                continue
            row = {
                'station': stat_props.get('code', ''),
                'latitude': stat_props.get('lat', ''),
                'date': current_date.isoformat(),
                'parameter': par_code,
                'value': par_value,
                'valid': par_flag and '1' or '0'
            }
            writer.writerow(row)


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
        export_function = getattr(format_module, 'export', export)
        export_function(data_parsed, outdata_filepath)
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


def validate_format(filepath, parameters_filepath, format_label=None):
    """
    Open a file and validate it against its format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param format_label: the label string (according to `sciafeed.formats.FORMATS`)
    :return: [..., (row index, error message), ...]
    """
    if not format_label:
        _, format_module = guess_format(filepath)
    else:
        format_module = dict(FORMATS).get(format_label)
    if not format_module:
        msgs, data_parsed = [(0, "file %r has unknown format" % filepath)], None
        return msgs, data_parsed
    validator = getattr(format_module, 'validate_format')
    ret_value = validator(filepath, parameters_filepath)
    return ret_value


def extract_metadata(filepath, format_label):
    """
    Extract station information and extra metadata from a file `filepath`
    of format `format_label`.
    Return the list of dictionaries [stat_props, extra_metadata]

    :param filepath: path to the file to validate
    :param format_label: the label string (according to `sciafeed.formats.FORMATS`)
    :return: [stat_props, extra_metadata]
    """
    format_module = dict(FORMATS).get(format_label)
    extractor = getattr(format_module, 'extract_metadata')
    stat_props, extra_metadata = extractor(filepath, format_label)
    return stat_props, extra_metadata
