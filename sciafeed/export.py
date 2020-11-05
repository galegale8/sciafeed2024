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
                  'source', 'format', 'lat', 'lon']
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
                'lat': metadata.get('lat', ''),
                'lon': metadata.get('lon', ''),
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
                'lat': row['lat'],
                'lon': row['lon'],
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


def stations2csv(stations, stations_path, extra_fields=()):
    """
    Export the list of information about stations into a CSV located at `stations_path`.
    The CSV fields are the one of anag__stazioni + extra_fields.
    Each station is a dictionary with labels (not ids) of the station property.

    :param stations: list of dictionaries of informations about stations
    :param stations_path: path of the output CSV
    :param extra_fields: list of extra fields to add to the CSV
    """
    fieldnames = ['id_staz', 'nome', 'lon', 'lat', 'quota', 'cod_utente', 'cod_rete', 'cod_entep',
                  'cod_entef', 'cod_enteg', 'cod_tipostaz', 'cod_classestaz', 'cod_orariostaz',
                  'cod_statostaz', 'cod_istat', 'cod_naz', 'cod_reg', 'cod_prov',
                  'indirizzo', 'codice_wmo', 'codice_icao', 'tipo_synop', 'h_pozz',
                  'sup_isobarica', 'data_inizio', 'data_fine', 'note', 'local_user',
                  'flag_mare', 'altezza_anemometro', 'flag_area_climatica', 'distanzamare']
    fieldnames += extra_fields
    with open(stations_path, 'w') as csv_out_file:
        writer = csv.DictWriter(csv_out_file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        if not stations:
            return
        station_rows = sorted(stations.values(), key=lambda r: r['nome'])
        for station_row in station_rows:
            row = {k: station_row.get(k, '') for k in fieldnames}
            writer.writerow(row)


def csv2items(csv_path, required_fields=(), ignore_fields=(), ignore_empty_fields=False):
    """
    Get a CSV located at `csv_path` and return a list of dictionaries according to the CSV rows.
    If some required fields are missing, raise an error.

    :param csv_path: CSV path
    :param required_fields: list of required fields (if black, raise ValueError)
    :param ignore_fields: list of fields to ignore
    :param ignore_empty_fields: if True, ignore the empty fields
    :return: a list of of dictionaries according to the CSV rows
    """
    items = []
    with open(csv_path) as csv_in_file:
        reader = csv.DictReader(csv_in_file, delimiter=';')
        for i, row in enumerate(reader):
            item = dict()
            for field_name, field_value in row.items():
                if field_name in ignore_fields:
                    continue
                value = field_value.strip()
                if value == '':
                    if ignore_empty_fields:
                        continue
                    item[field_name] = None
                    if field_name in required_fields:
                        raise ValueError("Line %i of %r: required field %r is missing"
                                         % (i, csv_path, field_name))
                else:
                    item[field_name] = value
            items.append(item)
    return items
