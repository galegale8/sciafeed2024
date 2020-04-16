
from datetime import date
from os.path import join

from sciafeed import bolzano
from . import TEST_DATA_PATH

import pytest


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    parameter_map = bolzano.load_parameter_file(test_filepath)
    for i in range(3, 6):
        assert str(i) in parameter_map
        assert 'par_code' in parameter_map[str(i)]
        assert 'description' in parameter_map[str(i)]


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    expected_thresholds = {
        'PREC': [0.0, 989.0], 'Tmax': [-30.0, 50.0], 'Tmin': [-40.0, 40.0]
    }
    parameter_thresholds = bolzano.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_get_station_props():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    expected = {
        'cod_utente': '0250',
        'desc': 'Marienberg - Monte Maria',
        'height': '1310',
        'utmx': '616288',
        'utmy': '5173583'}
    effective = bolzano.get_station_props(filepath)
    assert effective == expected

    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.txt')
    with pytest.raises(ValueError):
        bolzano.get_station_props(filepath)


def test_extract_metadata():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    expected = {
        'cod_utente': '0250',
        'desc': 'Marienberg - Monte Maria',
        'height': '1310',
        'utmx': '616288',
        'utmy': '5173583', 'source': 'bolzano/MonteMaria.xls',
        'format': 'BOLZANO',
    }
    effective = bolzano.extract_metadata(filepath, parameters_filepath)
    assert effective == expected

    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.txt')
    with pytest.raises(ValueError):
        bolzano.extract_metadata(filepath, parameters_filepath)


def test_parse_row():
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    parameters_map = bolzano.load_parameter_file(parameters_filepath=parameters_filepath)

    row = ['', '01.01.1981', '0,0', '9,0', '3,0']
    expected = [
        ({}, date(1981, 1, 1), 'Tmin', 3.0, True),
        ({}, date(1981, 1, 1), 'Tmax', 9.0, True),
        ({}, date(1981, 1, 1), 'PREC', 0.0, True),
    ]

    effective = bolzano.parse_row(row, parameters_map)
    assert effective == expected


def test_validate_row_format():
    # right row
    row = ['', '01.01.1981', '0,0', '9,0', '3,0']
    err_msg = bolzano.validate_row_format(row)
    assert not err_msg

    # wrong date format
    row = ['', '31.02.1981', '0,0', '9,0', '3,0']
    err_msg = bolzano.validate_row_format(row)
    assert err_msg == 'the date format is wrong'

    # wrong value for parameter
    row = ['', '01.02.1981', '0,0', '9,0', '3A,0']
    err_msg = bolzano.validate_row_format(row)
    assert err_msg == 'the row contains values not numeric'


def test_validate_format():
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    err_msgs = bolzano.validate_format(filepath, parameters_filepath)
    assert not err_msgs

    # global errors
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.txt')
    err_msgs = bolzano.validate_format(filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .xls, found .txt')]

    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong1.xls')
    err_msgs = bolzano.validate_format(filepath, parameters_filepath)
    assert err_msgs == [(0, 'BOLZANO file not compliant')]

    # several formatting errors
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong2.xls')
    err_msgs = bolzano.validate_format(filepath, parameters_filepath)
    assert err_msgs == [
        (14, 'the date format is wrong'),
        (15, 'the row contains values not numeric'),
        (18, 'the row is not strictly after the previous'),
        (22, 'the row is duplicated with different values')
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    station_info_expected = {'cod_utente': '0250',
                             'desc': 'Marienberg - Monte Maria',
                             'height': '1310', 'utmx': '616288',
                             'utmy': '5173583',
                             'source': 'bolzano/MonteMaria.xls',
                             'format': 'BOLZANO'}
    data_info_expected = [
        (date(1981, 1, 1), 'Tmin', 3.0, True),
        (date(1981, 1, 1), 'Tmax', 9.0, True),
        (date(1981, 1, 1), 'PREC', 0.0, True),
        (date(1981, 1, 2), 'Tmin', -4.0, True),
        (date(1981, 1, 2), 'Tmax', 5.0, True),
        (date(1981, 1, 2), 'PREC', 0.4, True),
        (date(1981, 1, 3), 'Tmin', -4.0, True),
        (date(1981, 1, 3), 'Tmax', 5.0, True),
        (date(1981, 1, 3), 'PREC', 0.0, True),
        (date(1981, 1, 4), 'Tmin', 1.0, True),
        (date(1981, 1, 4), 'Tmax', 9.0, True),
        (date(1981, 1, 4), 'PREC', 14.5, True),
        (date(1981, 1, 5), 'Tmin', -8.0, True),
        (date(1981, 1, 5), 'Tmax', 3.0, True),
        (date(1981, 1, 5), 'PREC', 5.1, True),
        (date(1981, 1, 6), 'Tmin', -8.0, True),
        (date(1981, 1, 6), 'Tmax', -5.0, True),
        (date(1981, 1, 6), 'PREC', 1.0, True),
        (date(1981, 1, 7), 'Tmin', -9.0, True),
        (date(1981, 1, 7), 'Tmax', -5.0, True),
        (date(1981, 1, 7), 'PREC', 6.1, True),
        (date(1981, 1, 8), 'Tmin', -13.0, True),
        (date(1981, 1, 8), 'Tmax', -7.0, True),
        (date(1981, 1, 8), 'PREC', 0.0, True),
    ]
    effective_data, err_msgs = bolzano.parse(filepath, parameters_filepath)
    for i, data_item in enumerate(effective_data):
        expected_md = station_info_expected.copy()
        expected_md['row'] = i // 3 + 14
        assert data_item[0] == expected_md
        assert data_item[1:] == data_info_expected[i]
    assert err_msgs == bolzano.validate_format(filepath, parameters_filepath)


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    assert bolzano.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong1.csv')
    assert not bolzano.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    assert not bolzano.is_format_compliant(filepath)
