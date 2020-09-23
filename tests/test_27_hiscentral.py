
from datetime import date
from os.path import exists, join

from sciafeed import hiscentral
from . import TEST_DATA_PATH


from pprint import pprint


def dummy_get_wsdl_service_response(wsdl_url, method_name, **kwargs):
    if method_name == 'GetVariables':
        with open(join(TEST_DATA_PATH, 'hiscentral', 'variables.xml')) as fp:
            return fp.read().strip()
    if method_name == 'GetValues':
        with open(join(TEST_DATA_PATH, 'hiscentral', 'series.xml')) as fp:
            return fp.read().strip()
    if method_name == 'GetSites':
        with open(join(TEST_DATA_PATH, 'hiscentral', 'sites.xml')) as fp:
            return fp.read().strip()
    raise AttributeError(method_name)


def test_get_region_variables(mocker):
    mocker.patch('sciafeed.hiscentral.get_wsdl_service_response',
                 new=dummy_get_wsdl_service_response)
    expected = {
        'Discharge': {'code': 'Discharge', 'name': 'Discharge', 'unit': 'm3s'},
        'Precipitation': {'code': 'Precipitation', 'name': 'Precipitation', 'unit': 'mm'},
        'Tmax': {'code': 'Tmax', 'name': 'Temperature', 'unit': 'degC'},
        'Tmin': {'code': 'Tmin', 'name': 'Temperature', 'unit': 'degC'},
        'Water_Level': {'code': 'Water_Level', 'name': 'Water Level', 'unit': 'm'}
    }
    effective = hiscentral.get_region_variables('02')
    assert effective == expected


def test_get_region_locations(mocker):
    mocker.patch('sciafeed.hiscentral.get_wsdl_service_response',
                 new=dummy_get_wsdl_service_response)
    effective = hiscentral.get_region_locations('02')
    assert len(effective) == 77
    assert '4400' in effective
    assert effective.get('4400') == {
        'code': '4400',
        'lat': '45.82369995',
        'lon': '7.687809944',
        'name': 'Ayas - Mandriou'
    }


def test_download_series(mocker, tmpdir):
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_990-reg.abruzzoTmax.csv')
    with open(filepath) as fp:
        expected_lines = fp.readlines()
    mocker.patch('sciafeed.hiscentral.get_wsdl_service_response',
                 new=dummy_get_wsdl_service_response)
    output_csv = str(tmpdir.join('output.csv'))
    assert not exists(output_csv)
    hiscentral.download_series('13', 'Tmax', '990', output_csv)
    assert exists(output_csv)
    with open(output_csv) as fp:
        effective_lines = fp.readlines()
        for expected_line in expected_lines:
            assert expected_line in effective_lines


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'hiscentral', 'hiscentral_params.csv')
    parameter_map = hiscentral.load_parameter_file(test_filepath)
    for key in parameter_map:
        assert 'hiscentral_par_code' in parameter_map[key]
        assert 'par_code' in parameter_map[key]
        assert 'description' in parameter_map[key]


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'hiscentral', 'hiscentral_params.csv')
    expected_thresholds = {
        'PREC': [0.0, 989.0],
        'Tmax': [-30.0, 50.0],
        'Tmin': [-40.0, 40.0]}
    parameter_thresholds = hiscentral.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_parse_filename():
    filename = "serie_990-reg.abruzzoTmax.csv"
    assert hiscentral.parse_filename(filename) == ('990', 'Tmax')


def test_validate_filename():
    # right
    filename = "serie_990-reg.abruzzoTmax.csv"
    err_msg = hiscentral.validate_filename(filename)
    assert not err_msg
    # wrong extension
    filename = "serie_990-reg.abruzzoTmax.xls"
    err_msg = hiscentral.validate_filename(filename)
    assert err_msg == 'Extension expected must be .csv, found .xls'
    # wrong cod_utente
    filename = "serie_990_reg.abruzzoTmax.csv"
    err_msg = hiscentral.validate_filename(filename)
    assert err_msg == 'cod_utente not parsable from the file name'
    # wrong parameter
    filename = "serie_990-reg.abruzzoTmedia.csv"
    err_msg = hiscentral.validate_filename(filename)
    assert err_msg == 'variable name is not parsable from the file name'


def test_extract_metadata():
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_990-reg.abruzzoTmax.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'hiscentral', 'hiscentral_params.csv')
    metadata = hiscentral.extract_metadata(filepath, parameters_filepath)
    assert metadata == {'cod_utente': '990', 'par_code': 'Tmax',
                        'source': 'hiscentral/serie_990-reg.abruzzoTmax.csv',
                        'format': 'HISCENTRAL', 'par_name': 'Tmax'}


def test_parse_row():
    row = {
        'time': '2000-07-01',
        'DataValue': '28',
        'UTCOffset': '1',
        'Qualifier': 'NA',
        'CensorCode': 'nc',
        'DateTimeUTC': '2000-06-30 23:00:00',
        'MethodCode': '0',
        'SourceCode': '1',
        'QualityControlLevelCode': '1'
    }
    parameters_filepath = join(TEST_DATA_PATH, 'hiscentral', 'hiscentral_params.csv')
    metadata = {'par_code': 'Tmax', 'par_name': 'Tmax'}
    parameters_map = hiscentral.load_parameter_file(parameters_filepath=parameters_filepath)
    expected = [
        (metadata, date(2000, 7, 1), 'Tmax', 28.0, True),
    ]
    effective = hiscentral.parse_row(row, parameters_map, metadata=metadata)
    assert effective == expected


def test_validate_row_format():
    # right format
    row = {
        'time': '2000-07-01',
        'DataValue': '28',
        'UTCOffset': '1',
        'Qualifier': 'NA',
        'CensorCode': 'nc',
        'DateTimeUTC': '2000-06-30 23:00:00',
        'MethodCode': '0',
        'SourceCode': '1',
        'QualityControlLevelCode': '1'
    }
    assert not hiscentral.validate_row_format(row)

    # wrong date
    row = {
        'time': '2000-17-01',
        'DataValue': '28',
        'UTCOffset': '1',
        'Qualifier': 'NA',
        'CensorCode': 'nc',
        'DateTimeUTC': '2000-06-30 23:00:00',
        'MethodCode': '0',
        'SourceCode': '1',
        'QualityControlLevelCode': '1'
    }
    assert hiscentral.validate_row_format(row) == 'the reference time for the row is not parsable'

    # wrong value
    row = {
        'time': '2000-07-01',
        'DataValue': 'about 1.0',
        'UTCOffset': '1',
        'Qualifier': 'NA',
        'CensorCode': 'nc',
        'DateTimeUTC': '2000-06-30 23:00:00',
        'MethodCode': '0',
        'SourceCode': '1',
        'QualityControlLevelCode': '1'
    }
    assert hiscentral.validate_row_format(row) == "the value 'about 1.0' is not numeric"


def test_validate_format(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_990-reg.abruzzoTmax.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'hiscentral', 'hiscentral_params.csv')
    assert not hiscentral.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = str(tmpdir.join('serie_990-reg.abruzzoTmax.xls'))
    err_msgs = hiscentral.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'Extension expected must be .csv, found .xls')]

    # global error
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_990-reg.abruzzoTmedia.csv')
    err_msgs = hiscentral.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'variable name is not parsable from the file name')]
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_wrong1-reg.abruzzoTmax.csv')
    err_msgs = hiscentral.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'The CSV header is not compliant with the format')]

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_wrong2-reg.abruzzoTmax.csv')
    err_msgs = hiscentral.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        (4, 'the reference time for the row is not parsable'),
        (8, 'the row is duplicated with different values'),
        (9, 'the row is not strictly after the previous'),
        (11, "the value '3A8' is not numeric")
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_990-reg.abruzzoTmax.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'hiscentral', 'hiscentral_params.csv')
    metadata = {'cod_utente': '990', 'par_code': 'Tmax', 'format': 'HISCENTRAL',
                'source': 'hiscentral/serie_990-reg.abruzzoTmax.csv', 'par_name': 'Tmax'}
    expected_data = [
        (metadata, date(2000, 7, 1), 'Tmax', 28.0, True),
        (metadata, date(2000, 7, 2), 'Tmax', 31.0, True),
        (metadata, date(2000, 7, 3), 'Tmax', 33.0, True),
        (metadata, date(2000, 7, 4), 'Tmax', 37.0, True),
        (metadata, date(2000, 7, 5), 'Tmax', 36.0, True),
        (metadata, date(2000, 7, 6), 'Tmax', 34.0, True),
        (metadata, date(2000, 7, 7), 'Tmax', 33.0, True),
        (metadata, date(2000, 7, 8), 'Tmax', 38.0, True),
        (metadata, date(2000, 7, 9), 'Tmax', 35.0, True),
    ]
    effective, err_msgs = hiscentral.parse(filepath, parameters_filepath=parameters_filepath)
    for i, record in enumerate(effective):
        assert effective[i][1:] == expected_data[i][1:]
        expected_md = expected_data[i][0]
        expected_md['row'] = i + 2
        assert effective[i][0] == expected_md
    assert err_msgs == hiscentral.validate_format(
        filepath, parameters_filepath=parameters_filepath)


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    assert not hiscentral.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_wrong1-reg.abruzzoTmax.csv')
    assert not hiscentral.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'hiscentral', 'serie_990-reg.abruzzoTmax.csv')
    assert hiscentral.is_format_compliant(filepath)
