"""
This module contains the functions and utilities to download and parse a HISCENTRAL file
"""
import csv
from datetime import datetime
import xml.dom.minidom
from os.path import abspath, basename, dirname, join, splitext
from pathlib import PurePath
import zeep

from sciafeed import TEMPLATES_PATH
from sciafeed import utils


MISSING_VALUE_MARKER = '-9999'
FORMAT_LABEL = 'HISCENTRAL'
ALLOWED_PARAMETERS = ('Precipitation', 'Tmax', 'Tmin')
FIELDNAMES = ['time', 'DataValue', 'UTCOffset', 'Qualifier', 'CensorCode', 'DateTimeUTC',
              'MethodCode', 'SourceCode', 'QualityControlLevelCode']
PARAMETERS_FILEPATH = join(TEMPLATES_PATH, 'hiscentral_params.csv')
LIMITING_PARAMETERS = {
}
WSDL_URLS = {
    '01': "http://hydrolite.ddns.net/italia/hsl-pie/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '02': "http://hydrolite.ddns.net/italia/hsl-vda/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '03': "http://hydrolite.ddns.net/italia/hsl-lom/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '05': "http://hydrolite.ddns.net/italia/hsl-ven/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '06': "http://hydrolite.ddns.net/italia/hsl-fvg/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '07': "http://hydrolite.ddns.net/italia/hsl-lig/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '08': "http://hydrolite.ddns.net/italia/hsl-emr/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '09': "http://hydrolite.ddns.net/italia/hsl-tos/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '10': "http://hydrolite.ddns.net/italia/hsl-umb/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '11': "http://hydrolite.ddns.net/italia/hsl-mar/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '12': "http://hydrolite.ddns.net/italia/hsl-laz/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '13': "http://hydrolite.ddns.net/italia/hsl-abr/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '14': "http://hydrolite.ddns.net/italia/hsl-mol/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '15': "http://hydrolite.ddns.net/italia/hsl-cam/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '16': "http://hydrolite.ddns.net/italia/hsl-pug/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '17': "http://hydrolite.ddns.net/italia/hsl-bas/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '18': "http://hydrolite.ddns.net/italia/hsl-cal/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '19': "http://hydrolite.ddns.net/italia/hsl-sic/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '20': "http://hydrolite.ddns.net/italia/hsl-sar/index.php/default/services/cuahsi_1_1.asmx?WSDL",
    '21': "http://hydrolite.ddns.net/italia/hsl-bol/index.php/default/services/cuahsi_1_1.asmx?WSDL",
}
# # from sciapgm.geo_entihiscentral
REGION_IDS_MAP = {
    '01': "PIEMONTE",
    '02': "VALLE D'AOSTA",
    '03': "LOMBARDIA",
    '04': "TRENTINO-ALTO ADIGE",
    '05': "VENETO",
    '06': "FRIULI-VENEZIA GIULIA",
    '07': "LIGURIA",
    '08': "EMILIA-ROMAGNA",
    '09': "TOSCANA",
    '10': "UMBRIA",
    '11': "MARCHE",
    '12': "LAZIO",
    '13': "ABRUZZO",
    '14': "MOLISE",
    '15': "CAMPANIA",
    '16': "PUGLIA",
    '17': "BASILICATA",
    '18': "CALABRIA",
    '19': "SICILIA",
    '20': "SARDEGNA",
    '21': "BOLZANO",
    # '22': "TRENTO",
}


def get_wsdl_service_response(wsdl_url, method_name, **kwargs):  # pragma: no cover
    """
    Connect to a WSDL service and call `method_name`

    :param wsdl_url: WSDL URL
    :param method_name: the service name to call
    :param kwargs: the keyword arguments to pass to the method
    :return: the xml string of the response
    """
    service = zeep.Client(wsdl=wsdl_url).service
    xml_string = getattr(service, method_name)(**kwargs)
    return xml_string


def get_region_variables(region_id):
    """
    Connect to the service to get the variables managed by a region.
    Return a dictionary of kind:
    ::

        {variable_code: dictionary of variable properties, ...}

    :param region_id: the id of a region
    :return: the dictionary of the variables used for that region
    """
    wsdl_url = WSDL_URLS[region_id]
    vars_xml = get_wsdl_service_response(wsdl_url, 'GetVariables')
    doc = xml.dom.minidom.parseString(vars_xml)
    variables = dict()
    var_elems = doc.getElementsByTagName("variable")
    key_tag_map = [
        ('code', 'variableCode'),
        ('name', 'variableName'),
        ('unit', 'unitAbbreviation'),
    ]
    for var_elem in var_elems:
        var_properties = dict()
        for key, tag_name in key_tag_map:
            var_properties[key] = var_elem.getElementsByTagName(tag_name)[0].firstChild.nodeValue
        var_code = var_properties['code']
        variables[var_code] = var_properties
    return variables


def get_region_locations(region_id):
    """
    Connect to the service to get the stations managed by a region.
    Return a dictionary of kind:
    ::

        {station_code: dictionary of station properties, ...}

    :param region_id: the id of the region
    :return: the dictionary of the stations for that region
    """
    wsdl_url = WSDL_URLS[region_id]
    sites_xml = get_wsdl_service_response(wsdl_url, 'GetSites')
    doc = xml.dom.minidom.parseString(sites_xml)
    locations = dict()
    site_elems = doc.getElementsByTagName("site")
    key_tag_map = [
        ('code', 'siteCode'),
        ('name', 'siteName'),
        ('lat', 'latitude'),
        ('lon', 'longitude')]
    for site_elem in site_elems:
        site_info = site_elem.firstChild
        site_properties = dict()
        for key, tag_name in key_tag_map:
            site_properties[key] = site_info.getElementsByTagName(tag_name)[0].firstChild.nodeValue
        site_code = site_properties['code']
        locations[site_code] = site_properties
    return locations


def download_series(region_id, variable, location, out_csv_path):
    """
    Download the series of a region for a specified variable and station.
    The series is saved into a CSV located at `out_csv_path`.

    :param region_id: the id of the region
    :param variable: the code of the variable
    :param location: the code of the station
    :param out_csv_path: the file path of the CSV to create
    """
    print('- asking series for region_id:%s location:%s variable:%s'
          % (region_id, location, variable))
    wsdl_url = WSDL_URLS[region_id]
    series_xml = get_wsdl_service_response(wsdl_url, 'GetValues',
                                           location=location, variable=variable)
    print('  ...and writing CSV on path %s' % out_csv_path)
    doc = xml.dom.minidom.parseString(series_xml)
    key_tag_map = {
        'time': 'dateTime',
        'DataValue': 'DataValue',
        'UTCOffset': 'timeOffset',
        'Qualifier': 'Qualifier',
        'CensorCode': 'censorCode',
        'DateTimeUTC': 'dateTimeUTC',
        'MethodCode': 'methodCode',
        'SourceCode': 'sourceCode',
        'QualityControlLevelCode': 'qualityControlLevelCode'}
    fieldnames = list(key_tag_map.keys())
    value_elems = doc.getElementsByTagName("value")
    with open(out_csv_path, 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames, delimiter=';')
        csv_writer.writeheader()
        for value_elem in value_elems:
            attrs = dict(value_elem.attributes.items())
            row = dict()
            for fieldname in fieldnames:
                row[fieldname] = attrs.get(key_tag_map[fieldname], 'NA')
            row['time'] = row['time'].split('T')[0]
            row['DateTimeUTC'] = row['DateTimeUTC'].replace('T', ' ')
            row['DataValue'] = value_elem.firstChild.nodeValue
            csv_writer.writerow(row)


# download entry point
def download_hiscentral(region_id, out_csv_folder, variables=None, locations=None):
    if locations is None:
        locations = get_region_locations(region_id)
    if variables is None:
        variables = get_region_variables(region_id)
    file_number = len(locations) * len(variables)
    i = 1
    for location in locations:
        for variable in variables:
            out_csv_name = "serie_%s-reg.%s%s.csv" \
                           % (location, REGION_IDS_MAP[region_id].lower(), variable.capitalize())
            out_csv_path = join(out_csv_folder, out_csv_name)
            print("processing %s/%s ..." % (i, file_number))
            download_series(region_id, variable, location, out_csv_path)
            i += 1


def load_parameter_file(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing details on the HISCENTRAL stored parameters.
    Return a dictionary of type:
    ::

        {HISCENTRAL parameter code: dictionary of properties of parameter
        }

    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param delimiter: CSV delimiter
    :return: dictionary of positions with parameters information
    """
    csv_file = open(parameters_filepath, 'r')
    csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        position = row['hiscentral_par_code']
        ret_value[position] = dict()
        for prop in row.keys():
            ret_value[position][prop] = row[prop].strip()
        ret_value[position]['convertion'] = utils.string2lambda(ret_value[position]['convertion'])
    return ret_value


def load_parameter_thresholds(parameters_filepath=PARAMETERS_FILEPATH, delimiter=';'):
    """
    Load a CSV file containing thresholds of the HISCENTRAL stored parameters.
    Return a dictionary of type:
    ::

        {   param_code: [min_value, max_value]
        }

    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :param delimiter: CSV delimiter
    :return: dictionary of parameters with their ranges
    """
    csv_file = open(parameters_filepath, 'r')
    csv_reader = csv.DictReader(csv_file, delimiter=delimiter)
    ret_value = dict()
    for row in csv_reader:
        par_code = row['par_code']
        try:
            min_threshold, max_threshold = map(float, [row['min'], row['max']])
            ret_value[par_code] = [min_threshold, max_threshold]
        except (KeyError, TypeError, ValueError):
            continue
    return ret_value


def parse_filename(filename: str, allowed_parameters=ALLOWED_PARAMETERS):
    """
    Return (cod_utente, par_name) corresponding to the HISCENTRAL
    input file named `filename`.
    The function assumes the filename is validated (see `validate_filename`).

    :param filename: the name of the HISCENTRAL file
    :param allowed_parameters: list of allowed parameter labels
    :return: the tuple (cod_utente, par_name)
    """
    name, ext = splitext(filename)
    cod_utente = name.split('-')[0].split('_')[1]
    par_name = None
    for allowed_par in allowed_parameters:
        if name.lower().endswith(allowed_par.lower()):
            par_name = allowed_par
    ret_value = (cod_utente, par_name)
    return ret_value


def validate_filename(filename: str, allowed_parameters=ALLOWED_PARAMETERS):
    """
    Check the name of the input HISCENTRAL file named `filename`
    and returns the description string of the error (if found).

    :param filename: the name of the HISCENTRAL file
    :param allowed_parameters: list of allowed parameter labels
    :return: the string describing the error
    """
    err_msg = ''
    name, ext = splitext(filename)
    if ext.lower() != '.csv':
        err_msg = 'Extension expected must be .csv, found %s' % ext
        return err_msg
    if '-' not in filename or '_' not in filename:
        err_msg = 'cod_utente not parsable from the file name'
        return err_msg
    for allowed_par in allowed_parameters:
        if name.lower().endswith(allowed_par.lower()):
            break
    else:  # never gone on break
        err_msg = 'variable name is not parsable from the file name'
        return err_msg
    return err_msg


def parse_row(row, parameters_map, metadata=None, missing_value_marker=MISSING_VALUE_MARKER):
    """
    Parse a row of a HISCENTRAL file, and return the parsed data. Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The function assumes the row as validated (see function `validate_row_format`).
    Flag is True (valid data) or False (not valid).

    :param row: a row dictionary of the HISCENTRAL file as parsed by csv.DictReader
    :param parameters_map: dictionary of information about stored parameters at each position
    :param metadata: default metadata if not provided in the row
    :param missing_value_marker: the string used as a marker for missing value
    :return: [(metadata, datetime object, par_code, par_value, flag), ...]
    """
    if metadata is None:
        metadata = dict()
    else:
        metadata = metadata.copy()
    date_obj = datetime.strptime(row['time'], "%Y-%m-%d").date()
    data = []
    param_code = metadata.get('par_code')
    props = parameters_map.get(param_code)
    if not props:
        return []
    param_value = row['DataValue'].strip()
    if param_value not in ('-', '', MISSING_VALUE_MARKER):
        param_value = props['convertion'](float(param_value.replace(',', '.')))
    else:
        param_value = None
    measure = (metadata, date_obj, param_code, param_value, True)
    data.append(measure)
    return data


def validate_row_format(row):
    """
    It checks a row of a HISCENTRAL file for validation against the format,
    and returns the description of the error (if found).
    This validation is needed to be able to parse the row by the function `parse_row`.

    :param row: the HISCENTRAL file row to validate
    :return: the string describing the error
    """
    err_msg = None
    try:
        datetime.strptime(row['time'], "%Y-%m-%d")
    except ValueError:
        err_msg = 'the reference time for the row is not parsable'
        return err_msg
    param_value = row['DataValue'].strip()
    if param_value not in ('-', ''):
        try:
            float(param_value.replace(',', '.'))
        except ValueError:
            err_msg = 'the value %r is not numeric' % param_value
            return err_msg
    return err_msg


def rows_generator(filepath, parameters_map, metadata):
    """
    A generator of rows of an hiscentral file containing data. Each value returned
    is a tuple (index of the row, row). row is a dictionary.

    :param filepath: the file path of the input file
    :param parameters_map: dictionary of information about stored parameters at each position
    :param metadata: default metadata if not provided in the row
    :return: iterable of (index of the row, row)
    """
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';')
    i = 1
    for i, row in enumerate(csv_reader, 2):
        yield i, row


# entry point candidate
def extract_metadata(filepath, parameters_filepath):
    """
    Extract generic metadata information from a file `filepath` of format HISCENTRAL.
    Return the dictionary of the metadata extracted.
    The function assumes the file name is validated against the format (see function
    `validate_filename`).

    :param filepath: path to the file to validate
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: dictionary of metadata extracted
    """
    source = join(*PurePath(abspath(filepath)).parts[-2:])
    filename = basename(filepath)
    cod_utente, par_name = parse_filename(filename)
    parameters_map = load_parameter_file(parameters_filepath)
    if par_name not in parameters_map:
        par_code = par_name
    else:
        par_code = parameters_map[par_name]['par_code']
    ret_value = {'cod_utente': cod_utente, 'par_code': par_code, 'source': source,
                 'format': FORMAT_LABEL}
    folder_name = dirname(source)
    ret_value.update(utils.folder2props(folder_name))
    return ret_value


# entry point candidate
def validate_format(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Open a HISCENTRAL file and validate it against the format.
    Return the list of tuples (row index, error message) of the errors found.
    row_index=0 is used only for global formatting errors.

    :param filepath: path to the HISCENTRAL file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: [..., (row index, error message), ...]
    """
    parameters_map = load_parameter_file(parameters_filepath)
    err_msgs = validate_filename(filepath)
    if err_msgs:
        return [(0, err_msgs)]
    csv_file = open(filepath, 'r', encoding='unicode_escape')
    csv_reader = csv.DictReader(csv_file, delimiter=';')
    if set(csv_reader.fieldnames) != set(FIELDNAMES):
        return [(0, 'The CSV header is not compliant with the format')]
    metadata = extract_metadata(filepath, parameters_filepath)

    found_errors = []
    last_time = None
    last_row = None

    for i, row in enumerate(csv_reader, 2):
        err_msg = validate_row_format(row)
        if err_msg:
            found_errors.append((i, err_msg))
            continue
        metadata['row'] = i
        row_measures = parse_row(row, parameters_map, metadata=metadata)
        if not row_measures:
            continue
        cur_time = row_measures[0][1]
        if last_time and cur_time == last_time and last_row != row:
            err_msg = 'the row is duplicated with different values'
            found_errors.append((i, err_msg))
            continue
        if last_time and cur_time < last_time:
            err_msg = 'the row is not strictly after the previous'
            found_errors.append((i, err_msg))
            continue
        last_time = cur_time
        last_row = row

    return found_errors


# entry point candidate
def parse(filepath, parameters_filepath=PARAMETERS_FILEPATH):
    """
    Read a HISCENTRAL file located at `filepath` and returns the data stored inside and the list
    of error messages eventually found.  
    Data structure is as a list:
    ::

      [(metadata, datetime object, par_code, par_value, flag), ...]

    The list of error messages is returned as the function `validate_format` does.

    :param filepath: path to the HISCENTRAL file
    :param parameters_filepath: path to the CSV file containing info about stored parameters
    :return: (data, found_errors)
    """""
    data = []
    found_errors = validate_format(filepath, parameters_filepath)
    found_errors_dict = dict(found_errors)
    if 0 in found_errors_dict:
        return data, found_errors
    metadata = extract_metadata(filepath, parameters_filepath)
    parameters_map = load_parameter_file(parameters_filepath)
    for i, row in rows_generator(filepath, parameters_map, metadata):
        if i in found_errors_dict:
            continue
        metadata['row'] = i
        parsed_row = parse_row(row, parameters_map, metadata=metadata)
        data.extend(parsed_row)
    return data, found_errors


# entry point candidate
def is_format_compliant(filepath):
    """
    Return True if the file located at `filepath` is compliant to the format, False otherwise.

    :param filepath: path to file to be checked
    :return: True if the file is compliant, False otherwise
    """
    filename = basename(filepath)
    if validate_filename(filename):
        return False
    # check the header
    with open(filepath) as fp:
        reader = csv.DictReader(fp, delimiter=';')
        if reader.fieldnames != FIELDNAMES:
            return False
    return True
