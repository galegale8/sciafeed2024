"""
This module contains the functions and utilities to export data structure to file.
`data` is a python structure, of kind:
::

    [(stat_props, datetime object, par_code, par_value, flag), ...]

- stat_props: a python dictionary containing information to identify a station
- datetime object: a datetime.datetime instance that is the time of measurement
- par_code: the parameter code
- par_value: the parameter value
- flag: a boolean flag to consider valid or not the value
"""
import csv
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
