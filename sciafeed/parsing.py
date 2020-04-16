"""
This module contains the functions and utilities to parse all SCIA data formats
"""
from os.path import dirname

from sciafeed import arpa19, arpa21, arpaer, arpafvg, bolzano, hiscentral, noaa, rmn, trentino


FORMATS = [(getattr(mod, 'FORMAT_LABEL'), mod) for mod in (
        arpa19, arpa21, arpaer, arpafvg, bolzano, hiscentral, noaa, rmn, trentino)]


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


def parse(filepath, parameters_filepath=None, format_label=None):
    """
    Try to extract data from a file located at `filepath`. Data returned is of kind:
    ::

    [(metadata, date obj, par_code, par_value, par_flag), ....]

    Return also the list of tuples (err_indx, err_msg) of the formatting errors found.

    :param filepath: the file path where to extract data
    :param parameters_filepath: path to the template of the format to be used
    :param format_label: the name of the format
    :return:data, found_errors
    """
    if not format_label:
        _, format_module = guess_format(filepath)
    else:
        format_module = dict(FORMATS).get(format_label)
    parse_f = getattr(format_module, 'parse')
    if not parameters_filepath:
        data, found_errors = parse_f(filepath)
    else:
        data, found_errors = parse_f(filepath, parameters_filepath)
    return data, found_errors
