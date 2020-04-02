"""
This module contains the functions and utilities to export data structure to file.
`data` is a python structure, of kind:
::

    [(metadata, datetime object, par_code, par_value, flag), ...]

- metadata: a python dictionary containing information to identify a station
- datetime object: a datetime.datetime instance that is the time of measurement
- par_code: the parameter code
- par_value: the parameter value
- flag: a boolean flag to consider valid or not the value
"""
import csv
from datetime import datetime
import operator


def export2csv(data, out_filepath, omit_parameters=(), omit_missing=True):
    """
    Write `data` as CSV file on the path `out_filepath` according to agreed conventions.
    `data` is formatted according to the output of the function `parse`.

    :param data: python structure for climatologic data
    :param out_filepath: output file where to write the data
    :param omit_parameters: list of the parameters to omit
    :param omit_missing: if False, include also values marked as missing
    """
    fieldnames = ['cod_utente', 'cod_rete', 'date', 'time', 'parameter', 'value', 'valid',
                  'source', 'format']
    with open(out_filepath, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for measure in sorted(data, key=operator.itemgetter(1)):
            metadata, current_date, par_code, par_value, par_flag = measure
            if par_code in omit_parameters:
                continue
            if omit_missing and par_value is None:
                continue
            row = {
                'cod_utente': metadata.get('cod_utente', ''),
                'cod_rete': metadata.get('cod_rete', ''),
                'date': current_date.strftime('%Y-%m-%d'),
                'time': isinstance(current_date, datetime) and current_date.strftime('%H:%M:%S')
                        or '',
                'parameter': par_code,
                'value': par_value,
                'valid': par_flag and '1' or '0',
                'source': metadata.get('source', ''),
                'format': metadata.get('format', ''),
            }
            writer.writerow(row)
