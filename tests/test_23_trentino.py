
from datetime import datetime
from os.path import join

from sciafeed import trentino
from . import TEST_DATA_PATH

import pytest


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    parameter_map = trentino.load_parameter_file(test_filepath)
    for key, value in parameter_map.items():
        assert 'CSV_CODE' in value
        assert 'par_code' in value
        assert 'description' in value


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    expected_thresholds = {
        'PREC': [0.0, 989.0], 'Tmax': [-30.0, 50.0], 'Tmin': [-40.0, 40.0]
    }
    parameter_thresholds = trentino.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds

    test2_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_wrong_params.csv')
    expected_thresholds = {'PREC': [0.0, 989.0]}
    parameter_thresholds = trentino.load_parameter_thresholds(test2_filepath)
    assert parameter_thresholds == expected_thresholds


def test_parse_filename():
    filename = 'T0001.csv'
    assert trentino.parse_filename(filename) == 'T0001'


def test_validate_filename():
    filename = 'T0001.csv'
    assert not trentino.validate_filename(filename)
    filename = 'wrong.xls'
    err_msg = trentino.validate_filename(filename)
    assert err_msg == 'Extension expected must be .csv, found .xls'


def test_guess_fieldnames():
    parmap_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    parameters_map = trentino.load_parameter_file(parmap_filepath)
    # right file
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    fieldnames, station, extra_info = trentino.guess_fieldnames(test_filepath, parameters_map)
    assert fieldnames == ['date', 'Tmin', 'quality']
    assert station == 'T0001'
    assert extra_info == {
        'cod_utente': 'T0001',
        'lat': 46.06227631,
        'lon': 11.23670156,
        'height': 475.0,
        'desc': 'Pergine Valsugana (Convento)'
    }

    # wrong file: missing header
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'wrong1.csv')
    with pytest.raises(ValueError) as err:
        trentino.guess_fieldnames(test_filepath, parameters_map)
    assert str(err.value) == 'trentino header not compliant'

    # tolerated file: strange header
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'wrong2.csv')
    fieldnames, station, extra_info = trentino.guess_fieldnames(test_filepath, parameters_map)
    assert fieldnames == ['date', 'Tmin', 'quality']
    assert station == 'T0001'
    assert not extra_info


def test_extract_metadata():
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    stat_props, extra_metadata = trentino.extract_metadata(filepath, parameters_filepath)
    assert stat_props == {'cod_utente': 'T0001', 'lat': 46.06227631, 'lon': 11.23670156,
                          'height': 475.0, 'desc': 'Pergine Valsugana (Convento)'}
    assert extra_metadata == {'fieldnames': ['date', 'Tmin', 'quality']}


def test_parse_row():
    parameters_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    parameters_map = trentino.load_parameter_file(parameters_filepath=parameters_filepath)

    # quality ok
    for quality in ['1', '76']:
        row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '10.0',
            'quality': quality
        }
        expected = [[{}, datetime(1930, 5, 1, 9, 0), 'Tmin', 10.0, True]]
        effective = trentino.parse_row(row, parameters_map)
        assert effective == expected

    # quality 255 or 151: value None
    for quality in ['151', '255']:
        row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '10.0',
            'quality': quality
        }
        expected = [[{}, datetime(1930, 5, 1, 9, 0), 'Tmin', None, True]]
        effective = trentino.parse_row(row, parameters_map)
        assert effective == expected

    # quality bad
    for quality in ['2', '140']:
        row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '10.0',
            'quality': quality
        }
        expected = [[{}, datetime(1930, 5, 1, 9, 0), 'Tmin', 10.0, False]]
        effective = trentino.parse_row(row, parameters_map)
        assert effective == expected


def test_validate_row_format():
    # right row
    row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '10.0',
            'quality': '1'
    }
    err_msg = trentino.validate_row_format(row)
    assert not err_msg

    # wrong date format
    row = {
            'date': '09:00:00 31/02/1930',
            'Tmin': '10.0',
            'quality': '1'
    }
    err_msg = trentino.validate_row_format(row)
    assert err_msg == 'the date format is wrong'

    # wrong value for parameter
    row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '?',
            'quality': '1'
    }
    err_msg = trentino.validate_row_format(row)
    assert err_msg == 'the value for Tmin is not numeric'


def test_parse():
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    station_props = {'cod_utente': 'T0001', 'desc': 'Pergine Valsugana (Convento)',
                     'height': 475.0, 'lat': 46.06227631, 'lon': 11.23670156}
    expected_data = [
        [station_props, datetime(1930, 5, 1, 9, 0), 'Tmin', 10.0, True],
        [station_props, datetime(1930, 5, 2, 9, 0), 'Tmin', 11.0, True],
        [station_props, datetime(1930, 5, 3, 9, 0), 'Tmin', 10.0, True],
        [station_props, datetime(1930, 5, 4, 9, 0), 'Tmin', 8.0, True],
        [station_props, datetime(1930, 5, 5, 9, 0), 'Tmin', 12.0, True],
        [station_props, datetime(1930, 5, 6, 9, 0), 'Tmin', 8.0, True],
        [station_props, datetime(1930, 5, 7, 9, 0), 'Tmin', 10.0, True],
        [station_props, datetime(1930, 5, 8, 9, 0), 'Tmin', 7.0, True],
        [station_props, datetime(1930, 5, 9, 9, 0), 'Tmin', 8.0, True],
        [station_props, datetime(1930, 5, 10, 9, 0), 'Tmin', 7.0, True],
        [station_props, datetime(1930, 5, 11, 9, 0), 'Tmin', 5.0, True],
        [station_props, datetime(1930, 5, 12, 9, 0), 'Tmin', 7.0, True],
        [station_props, datetime(1930, 5, 13, 9, 0), 'Tmin', None, True],
        [station_props, datetime(1930, 5, 14, 9, 0), 'Tmin', 9.0, True]
    ]
    effective = trentino.parse(filepath, parameters_filepath)
    assert effective == expected_data


def test_validate_format():
    parameters_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    err_msgs = trentino.validate_format(filepath, parameters_filepath)
    assert not err_msgs

    # global errors
    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong1.csv')
    err_msgs = trentino.validate_format(filepath, parameters_filepath)
    assert err_msgs == [(0, 'trentino header not compliant')]

    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong1.xls')
    err_msgs = trentino.validate_format(filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .csv, found .xls')]

    # several formatting errors
    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong3.csv')
    err_msgs = trentino.validate_format(filepath, parameters_filepath)
    assert err_msgs == [
        (5, 'the date format is wrong'),
        (6, 'the value for Tmin is not numeric'),
        (8, 'the row is not strictly after the previous'),
        (12, 'the row is duplicated with different values'),
        (13, 'the value for quality is missing')
    ]


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    assert trentino.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong1.csv')
    assert not trentino.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    assert not trentino.is_format_compliant(filepath)
