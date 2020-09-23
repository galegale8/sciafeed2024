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

ROUND_PRECISION = 1


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
            if par_value is not None:
                par_value = round(par_value, ROUND_PRECISION)
            cod_utente = metadata.get('cod_utente_prefix', '') + metadata.get('cod_utente', '')
            ttime = isinstance(current_date, datetime) and current_date.strftime('%H:%M:%S') or ''
            row = {
                'cod_utente': cod_utente,
                'cod_rete': metadata.get('cod_rete', ''),
                'date': current_date.strftime('%Y-%m-%d'),
                'time': ttime,
                'parameter': par_code,
                'value': par_value,
                'valid': par_flag and '1' or '0',
                'source': metadata.get('source', ''),
                'format': metadata.get('format', ''),
            }
            writer.writerow(row)


def csv2data(csv_path):
    """
    inverse of function `export2csv`.

    :param csv_path: file to the CSV containing the data
    :return: the data object
    """
    data = []
    with open(csv_path) as csv_in_file:
        reader = csv.DictReader(csv_in_file, delimiter=';')
        for row in reader:
            if row['value'] != '':
                par_value = float(row['value'])
            else:
                par_value = None
            metadata = {
                'cod_utente': row['cod_utente'],
                'cod_rete': row['cod_rete'],
                'source': row['source'],
                'format': row['format'],
            }
            if row['time']:
                current_date = datetime.strptime("%sT%s" % (row['date'], row['time']),
                                                 '%Y-%m-%dT%H:%M:%S')
            else:
                current_date = datetime.strptime("%sT00:00" % row['date'], '%Y-%m-%dT%H:%M').date()
            par_code = row['parameter']
            par_value = par_value
            par_flag = row['valid'] == '1' and True or False
            measure = metadata, current_date, par_code, par_value, par_flag
            data.append(measure)
    return data


def stations2csv(stations, stations_path):
    """
    Export the list of information about stations into a CSV located at `stations_path`.

    :param stations: list of dictionaries of informations about stations
    :param stations_path: path of the output CSV
    """
    # TODO
    pass


def csv2stations2(stations_path):
    """
    Export the list of information about stations into a CSV located at `stations_path`.
    Returns a list of log messages of operations done.

    :param stations: list of dictionaries of informations about stations
    :param stations_path: path of the output CSV
    :return: a list of log messages of operations done
    """
    # TODO
    return []