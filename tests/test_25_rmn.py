
from datetime import datetime
from os.path import join, exists

from sciafeed import rmn
from . import TEST_DATA_PATH

import pytest


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    parameter_map = rmn.load_parameter_file(test_filepath)
    for i in range(1, 6):
        assert i in parameter_map
        assert 'par_code' in parameter_map[i]
        assert 'description' in parameter_map[i]


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    expected_thresholds = {
        'DD': [0.0, 360.0],
        'FF': [0.0, 102.0],
        'P ': [960.0, 1060.0],
        'Tmedia': [-35.0, 45.0],
        'UR media': [20.0, 100.0]
    }
    parameter_thresholds = rmn.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds

    test_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params2.csv')
    expected_thresholds = {
        'DD': [0.0, 360.0],
        'P ': [960.0, 1060.0],
        'Tmedia': [-35.0, 45.0],
        'UR media': [20.0, 100.0]
    }
    parameter_thresholds = rmn.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_guess_fieldnames():
    parmap_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    parameters_map = rmn.load_parameter_file(parmap_filepath)
    # right file
    test_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    fieldnames, station = rmn.guess_fieldnames(test_filepath, parameters_map)
    assert fieldnames == ['DATA', 'ORA', 'DD', 'FF', 'Tmedia', 'P', 'UR media']
    assert station == 'ANCONA'

    # wrong file: missing header
    test_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong1.csv')
    with pytest.raises(ValueError) as err:
        rmn.guess_fieldnames(test_filepath, parameters_map)
    assert str(err.value) == 'RMN header not found'

    # wrong file: strange header
    test_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong2.csv')
    with pytest.raises(ValueError) as err:
        rmn.guess_fieldnames(test_filepath, parameters_map)
    assert str(err.value) == "Unknown column on header: 'ANCONA TEMPERATURA MARE m/s'"


def test_parse_row():
    row = {
        'DATA': '20180101',
        'DD': '180',
        'FF': '1,9',
        'ORA': '00:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    parameters_map = rmn.load_parameter_file(parameters_filepath=parameters_filepath)

    expected = (datetime(2018, 1, 1, 0, 0), {
        'DD': (180.0, True),
        'FF': (1.9, True),
        'P': (1018.1, True),
        'Tmedia': (7.2, True),
        'UR media': (63.0, True)
    })
    effective = rmn.parse_row(row, parameters_map)
    assert effective == expected

    row = {
        'DATA': '20180101',
        'DD': '',
        'FF': '',
        'ORA': '00:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    expected = (datetime(2018, 1, 1, 0, 0), {
        'DD': (None, True),
        'FF': (None, True),
        'Tmedia': (7.2, True),
        'P': (1018.1, True),
        'UR media': (63.0, True)
    })
    effective = rmn.parse_row(row, parameters_map)
    assert effective == expected

    row = {
        'DATA': '20180101',
        'DD': '180',
        'ORA': '00:00',
    }
    expected = (datetime(2018, 1, 1, 0, 0), {
        'DD': (180, True),
    })
    effective = rmn.parse_row(row, parameters_map)
    assert effective == expected


def test_validate_row_format():
    # right format
    row = {
        'DATA': '20180101',
        'DD': '180',
        'FF': '1,9',
        'ORA': '00:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    assert not rmn.validate_row_format(row)

    # wrong date
    row = {
        'DATA': '20180230',
        'DD': '180',
        'FF': '1,9',
        'ORA': '00:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    assert rmn.validate_row_format(row) == 'the reference time for the row is not parsable'

    # wrong value
    row = {
        'DATA': '20180101',
        'DD': '',
        'FF': 'about 1,9',
        'ORA': '10:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    assert rmn.validate_row_format(row) == "the value 'about 1,9' is not numeric"


def test_validate_format(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    assert not rmn.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = str(tmpdir.join('ancona_right.xls'))
    with open(filepath, 'w'):
        pass
    err_msgs = rmn.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'RMN header not found')]

    # global error
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong1.csv')
    err_msgs = rmn.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [(0, 'RMN header not found')]
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong2.csv')
    err_msgs = rmn.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [(0, "Unknown column on header: 'ANCONA TEMPERATURA MARE m/s'")]
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong4.csv')
    err_msgs = rmn.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [(0, 'not found station name')]

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong3.csv')
    err_msgs = rmn.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        (4, 'the reference time for the row is not parsable'),
        (16, 'the row is duplicated with different values'),
        (17, 'the row is not strictly after the previous'),
        (23, "the value '180gradi' is not numeric")
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    station = 'ANCONA'
    expected_data = {
        datetime(2018, 1, 1, 0, 0): {
            'DD': (180.0, True),
            'FF': (1.9, True),
            'P': (1018.1, True),
            'Tmedia': (7.2, True),
            'UR media': (63.0, True)},
        datetime(2018, 1, 1, 0, 10): {
            'DD': (180.0, True),
            'FF': (2.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 20): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 30): {
            'DD': (180.0, True),
            'FF': (0.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 40): {
            'DD': (180.0, True),
            'FF': (0.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 50): {
            'DD': (180.0, True),
            'FF': (0.8, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 0): {
            'DD': (180.0, True),
            'FF': (1.0, True),
            'P': (1017.6, True),
            'Tmedia': (8.0, True),
            'UR media': (60.0, True)},
        datetime(2018, 1, 1, 1, 10): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 20): {
            'DD': (180.0, True),
            'FF': (1.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 30): {
            'DD': (180.0, True),
            'FF': (3.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 40): {
            'DD': (180.0, True),
            'FF': (2.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 50): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 0): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (1016.9, True),
            'Tmedia': (9.0, True),
            'UR media': (58.0, True)},
        datetime(2018, 1, 1, 2, 10): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 20): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 30): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 40): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 50): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 0): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (1016.2, True),
            'Tmedia': (8.7, True),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 3, 10): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 20): {
            'DD': (180.0, True),
            'FF': (4.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 30): {
            'DD': (180.0, True),
            'FF': (4.2, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 40): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 50): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 0): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (1015.2, True),
            'Tmedia': (10.1, True),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 4, 10): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 20): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 30): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 40): {
            'DD': (180.0, True),
            'FF': (5.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 50): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 0): {
            'DD': (180.0, True),
            'FF': (5.8, True),
            'P': (1014.3, True),
            'Tmedia': (9.7, True),
            'UR media': (62.0, True)},
        datetime(2018, 1, 1, 5, 10): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 20): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 30): {
            'DD': (180.0, True),
            'FF': (5.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 40): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 50): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 6, 0): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (1014.1, True),
            'Tmedia': (9.5, True),
            'UR media': (64.0, True)}
    }
    effective = rmn.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == (station, expected_data)


def test_write_data(tmpdir):
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    data = rmn.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        'station;latitude;date;parameter;value;valid\n',
        'ANCONA;;2018-01-01T00:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:00:00;FF;1.9;1\n',
        'ANCONA;;2018-01-01T00:00:00;Tmedia;7.2;1\n',
        'ANCONA;;2018-01-01T00:00:00;P;1018.1;1\n',
        'ANCONA;;2018-01-01T00:00:00;UR media;63.0;1\n',
        'ANCONA;;2018-01-01T00:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:10:00;FF;2.6;1\n',
        'ANCONA;;2018-01-01T00:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:20:00;FF;1.6;1\n',
        'ANCONA;;2018-01-01T00:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:30:00;FF;0.6;1\n',
        'ANCONA;;2018-01-01T00:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:40:00;FF;0.5;1\n',
        'ANCONA;;2018-01-01T00:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:50:00;FF;0.8;1\n',
        'ANCONA;;2018-01-01T01:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:00:00;FF;1.0;1\n',
        'ANCONA;;2018-01-01T01:00:00;Tmedia;8.0;1\n',
        'ANCONA;;2018-01-01T01:00:00;P;1017.6;1\n',
        'ANCONA;;2018-01-01T01:00:00;UR media;60.0;1\n',
        'ANCONA;;2018-01-01T01:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:10:00;FF;1.6;1\n',
        'ANCONA;;2018-01-01T01:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:20:00;FF;1.4;1\n',
        'ANCONA;;2018-01-01T01:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:30:00;FF;3.6;1\n',
        'ANCONA;;2018-01-01T01:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:40:00;FF;2.3;1\n',
        'ANCONA;;2018-01-01T01:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:50:00;FF;3.7;1\n',
        'ANCONA;;2018-01-01T02:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:00:00;FF;4.0;1\n',
        'ANCONA;;2018-01-01T02:00:00;Tmedia;9.0;1\n',
        'ANCONA;;2018-01-01T02:00:00;P;1016.9;1\n',
        'ANCONA;;2018-01-01T02:00:00;UR media;58.0;1\n',
        'ANCONA;;2018-01-01T02:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:10:00;FF;3.7;1\n',
        'ANCONA;;2018-01-01T02:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:20:00;FF;3.7;1\n',
        'ANCONA;;2018-01-01T02:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:30:00;FF;3.9;1\n',
        'ANCONA;;2018-01-01T02:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:40:00;FF;4.0;1\n',
        'ANCONA;;2018-01-01T02:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:50:00;FF;4.1;1\n',
        'ANCONA;;2018-01-01T03:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:00:00;FF;3.9;1\n',
        'ANCONA;;2018-01-01T03:00:00;Tmedia;8.7;1\n',
        'ANCONA;;2018-01-01T03:00:00;P;1016.2;1\n',
        'ANCONA;;2018-01-01T03:00:00;UR media;59.0;1\n',
        'ANCONA;;2018-01-01T03:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:10:00;FF;4.0;1\n',
        'ANCONA;;2018-01-01T03:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:20:00;FF;4.4;1\n',
        'ANCONA;;2018-01-01T03:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:30:00;FF;4.2;1\n',
        'ANCONA;;2018-01-01T03:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:40:00;FF;4.1;1\n',
        'ANCONA;;2018-01-01T03:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:50:00;FF;4.0;1\n',
        'ANCONA;;2018-01-01T04:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:00:00;FF;4.5;1\n',
        'ANCONA;;2018-01-01T04:00:00;Tmedia;10.1;1\n',
        'ANCONA;;2018-01-01T04:00:00;P;1015.2;1\n',
        'ANCONA;;2018-01-01T04:00:00;UR media;59.0;1\n',
        'ANCONA;;2018-01-01T04:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:10:00;FF;4.6;1\n',
        'ANCONA;;2018-01-01T04:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:20:00;FF;4.6;1\n',
        'ANCONA;;2018-01-01T04:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:30:00;FF;5.4;1\n',
        'ANCONA;;2018-01-01T04:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:40:00;FF;5.5;1\n',
        'ANCONA;;2018-01-01T04:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:50:00;FF;5.4;1\n',
        'ANCONA;;2018-01-01T05:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:00:00;FF;5.8;1\n',
        'ANCONA;;2018-01-01T05:00:00;Tmedia;9.7;1\n',
        'ANCONA;;2018-01-01T05:00:00;P;1014.3;1\n',
        'ANCONA;;2018-01-01T05:00:00;UR media;62.0;1\n',
        'ANCONA;;2018-01-01T05:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:10:00;FF;5.3;1\n',
        'ANCONA;;2018-01-01T05:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:20:00;FF;5.3;1\n',
        'ANCONA;;2018-01-01T05:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:30:00;FF;5.0;1\n',
        'ANCONA;;2018-01-01T05:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:40:00;FF;5.3;1\n',
        'ANCONA;;2018-01-01T05:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:50:00;FF;4.5;1\n',
        'ANCONA;;2018-01-01T06:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T06:00:00;FF;4.6;1\n',
        'ANCONA;;2018-01-01T06:00:00;Tmedia;9.5;1\n',
        'ANCONA;;2018-01-01T06:00:00;P;1014.1;1\n',
        'ANCONA;;2018-01-01T06:00:00;UR media;64.0;1\n'
    ]
    assert not exists(out_filepath)

    # complete write
    rmn.write_data(data, out_filepath)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows

    # omit some parameters and include missing ones
    rmn.write_data(data, out_filepath, omit_parameters=('FF', ), omit_missing=False)
    expected_rows = [
        'station;latitude;date;parameter;value;valid\n',
        'ANCONA;;2018-01-01T00:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:00:00;Tmedia;7.2;1\n',
        'ANCONA;;2018-01-01T00:00:00;P;1018.1;1\n',
        'ANCONA;;2018-01-01T00:00:00;UR media;63.0;1\n',
        'ANCONA;;2018-01-01T00:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:10:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T00:10:00;P;;1\n',
        'ANCONA;;2018-01-01T00:10:00;UR media;;1\n',
        'ANCONA;;2018-01-01T00:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:20:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T00:20:00;P;;1\n',
        'ANCONA;;2018-01-01T00:20:00;UR media;;1\n',
        'ANCONA;;2018-01-01T00:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:30:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T00:30:00;P;;1\n',
        'ANCONA;;2018-01-01T00:30:00;UR media;;1\n',
        'ANCONA;;2018-01-01T00:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:40:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T00:40:00;P;;1\n',
        'ANCONA;;2018-01-01T00:40:00;UR media;;1\n',
        'ANCONA;;2018-01-01T00:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T00:50:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T00:50:00;P;;1\n',
        'ANCONA;;2018-01-01T00:50:00;UR media;;1\n',
        'ANCONA;;2018-01-01T01:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:00:00;Tmedia;8.0;1\n',
        'ANCONA;;2018-01-01T01:00:00;P;1017.6;1\n',
        'ANCONA;;2018-01-01T01:00:00;UR media;60.0;1\n',
        'ANCONA;;2018-01-01T01:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:10:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T01:10:00;P;;1\n',
        'ANCONA;;2018-01-01T01:10:00;UR media;;1\n',
        'ANCONA;;2018-01-01T01:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:20:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T01:20:00;P;;1\n',
        'ANCONA;;2018-01-01T01:20:00;UR media;;1\n',
        'ANCONA;;2018-01-01T01:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:30:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T01:30:00;P;;1\n',
        'ANCONA;;2018-01-01T01:30:00;UR media;;1\n',
        'ANCONA;;2018-01-01T01:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:40:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T01:40:00;P;;1\n',
        'ANCONA;;2018-01-01T01:40:00;UR media;;1\n',
        'ANCONA;;2018-01-01T01:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T01:50:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T01:50:00;P;;1\n',
        'ANCONA;;2018-01-01T01:50:00;UR media;;1\n',
        'ANCONA;;2018-01-01T02:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:00:00;Tmedia;9.0;1\n',
        'ANCONA;;2018-01-01T02:00:00;P;1016.9;1\n',
        'ANCONA;;2018-01-01T02:00:00;UR media;58.0;1\n',
        'ANCONA;;2018-01-01T02:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:10:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T02:10:00;P;;1\n',
        'ANCONA;;2018-01-01T02:10:00;UR media;;1\n',
        'ANCONA;;2018-01-01T02:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:20:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T02:20:00;P;;1\n',
        'ANCONA;;2018-01-01T02:20:00;UR media;;1\n',
        'ANCONA;;2018-01-01T02:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:30:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T02:30:00;P;;1\n',
        'ANCONA;;2018-01-01T02:30:00;UR media;;1\n',
        'ANCONA;;2018-01-01T02:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:40:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T02:40:00;P;;1\n',
        'ANCONA;;2018-01-01T02:40:00;UR media;;1\n',
        'ANCONA;;2018-01-01T02:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T02:50:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T02:50:00;P;;1\n',
        'ANCONA;;2018-01-01T02:50:00;UR media;;1\n',
        'ANCONA;;2018-01-01T03:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:00:00;Tmedia;8.7;1\n',
        'ANCONA;;2018-01-01T03:00:00;P;1016.2;1\n',
        'ANCONA;;2018-01-01T03:00:00;UR media;59.0;1\n',
        'ANCONA;;2018-01-01T03:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:10:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T03:10:00;P;;1\n',
        'ANCONA;;2018-01-01T03:10:00;UR media;;1\n',
        'ANCONA;;2018-01-01T03:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:20:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T03:20:00;P;;1\n',
        'ANCONA;;2018-01-01T03:20:00;UR media;;1\n',
        'ANCONA;;2018-01-01T03:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:30:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T03:30:00;P;;1\n',
        'ANCONA;;2018-01-01T03:30:00;UR media;;1\n',
        'ANCONA;;2018-01-01T03:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:40:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T03:40:00;P;;1\n',
        'ANCONA;;2018-01-01T03:40:00;UR media;;1\n',
        'ANCONA;;2018-01-01T03:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T03:50:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T03:50:00;P;;1\n',
        'ANCONA;;2018-01-01T03:50:00;UR media;;1\n',
        'ANCONA;;2018-01-01T04:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:00:00;Tmedia;10.1;1\n',
        'ANCONA;;2018-01-01T04:00:00;P;1015.2;1\n',
        'ANCONA;;2018-01-01T04:00:00;UR media;59.0;1\n',
        'ANCONA;;2018-01-01T04:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:10:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T04:10:00;P;;1\n',
        'ANCONA;;2018-01-01T04:10:00;UR media;;1\n',
        'ANCONA;;2018-01-01T04:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:20:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T04:20:00;P;;1\n',
        'ANCONA;;2018-01-01T04:20:00;UR media;;1\n',
        'ANCONA;;2018-01-01T04:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:30:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T04:30:00;P;;1\n',
        'ANCONA;;2018-01-01T04:30:00;UR media;;1\n',
        'ANCONA;;2018-01-01T04:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:40:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T04:40:00;P;;1\n',
        'ANCONA;;2018-01-01T04:40:00;UR media;;1\n',
        'ANCONA;;2018-01-01T04:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T04:50:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T04:50:00;P;;1\n',
        'ANCONA;;2018-01-01T04:50:00;UR media;;1\n',
        'ANCONA;;2018-01-01T05:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:00:00;Tmedia;9.7;1\n',
        'ANCONA;;2018-01-01T05:00:00;P;1014.3;1\n',
        'ANCONA;;2018-01-01T05:00:00;UR media;62.0;1\n',
        'ANCONA;;2018-01-01T05:10:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:10:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T05:10:00;P;;1\n',
        'ANCONA;;2018-01-01T05:10:00;UR media;;1\n',
        'ANCONA;;2018-01-01T05:20:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:20:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T05:20:00;P;;1\n',
        'ANCONA;;2018-01-01T05:20:00;UR media;;1\n',
        'ANCONA;;2018-01-01T05:30:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:30:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T05:30:00;P;;1\n',
        'ANCONA;;2018-01-01T05:30:00;UR media;;1\n',
        'ANCONA;;2018-01-01T05:40:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:40:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T05:40:00;P;;1\n',
        'ANCONA;;2018-01-01T05:40:00;UR media;;1\n',
        'ANCONA;;2018-01-01T05:50:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T05:50:00;Tmedia;;1\n',
        'ANCONA;;2018-01-01T05:50:00;P;;1\n',
        'ANCONA;;2018-01-01T05:50:00;UR media;;1\n',
        'ANCONA;;2018-01-01T06:00:00;DD;180.0;1\n',
        'ANCONA;;2018-01-01T06:00:00;Tmedia;9.5;1\n',
        'ANCONA;;2018-01-01T06:00:00;P;1014.1;1\n',
        'ANCONA;;2018-01-01T06:00:00;UR media;64.0;1\n'
    ]
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows


def test_row_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    parameters_map = rmn.load_parameter_file(parameters_filepath)
    parameters_thresholds = rmn.load_parameter_thresholds(parameters_filepath)

    # right row
    row = {
        'DATA': '20180101',
        'DD': '180',
        'FF': '1,9',
        'ORA': '00:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    row_parsed = rmn.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # two errors
    parameters_thresholds['DD'] = [0, 179]
    parameters_thresholds['FF'] = [-20, 0]
    err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert err_msgs == ["The value of 'DD' is out of range [0.0, 179.0]",
                        "The value of 'FF' is out of range [-20.0, 0.0]"]
    assert parsed_row_updated[:1] == row_parsed[:1]
    assert parsed_row_updated[1]['DD'] == (180.0, False)
    assert parsed_row_updated[1]['FF'] == (1.9, False)
    parsed_row_updated[1]['DD'] = (180.0, True)
    parsed_row_updated[1]['FF'] = (1.9, True)
    assert parsed_row_updated == row_parsed

    # no check if the value is already invalid
    row_parsed[1]['DD'] = (180.0, False)
    row_parsed[1]['FF'] = (1.9, False)
    err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if no parameters_thresholds
    err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_row_internal_consistence_check():
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    parameters_map = rmn.load_parameter_file(parameters_filepath)
    limiting_params = {'Tmedia': ('FF', 'UR media')}

    # right row
    row = {
        'DATA': '20180101',
        'DD': '180',
        'FF': '1,9',
        'ORA': '00:00',
        'P': '1018,1',
        'Tmedia': '7,2',
        'UR media': '63'
    }
    row_parsed = rmn.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # wrong value
    limiting_params = {'Tmedia': ('UR media', 'DD')}
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert err_msgs == ["The values of 'Tmedia' and 'UR media' are not consistent"]
    assert parsed_row_updated[:1] == row_parsed[:1]
    assert parsed_row_updated[1]['Tmedia'] == (7.2, False)
    parsed_row_updated[1]['Tmedia'] = (7.2, True)
    assert parsed_row_updated == row_parsed

    # no check if the value is invalid or None
    row_parsed[1]['Tmedia'] = (7.2, False)
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[1]['Tmedia'] = (None, True)
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[1]['Tmedia'] = (7.2, True)

    # no check if one of the limiting parameters is invalid or None
    row_parsed[1]['UR media'] = (63, False)
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[1]['UR media'] = (None, True)
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[1]['UR media'] = (63, True)
    row_parsed[1]['DD'] = (180, False)
    row_parsed[1]['Tmedia'] = (7.2, False)
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[1]['DD'] = (180, True)
    row_parsed[1]['Tmedia'] = (7.2, True)

    # no check if no limiting parameters
    err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_do_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    parsed = rmn.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = rmn.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # global error
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong1.csv')
    err_msgs, parsed_after_check = rmn.do_weak_climatologic_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'RMN header not found')]
    assert not parsed_after_check

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong3.csv')
    err_msgs, _ = rmn.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs

    # with specific errors
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong5.csv')
    parsed = rmn.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = rmn.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [
        (4, "The value of 'DD' is out of range [0.0, 360.0]"),
        (5, "The value of 'FF' is out of range [0.0, 102.0]"),
        (6, "The value of 'Tmedia' is out of range [-35.0, 45.0]"),
    ]
    assert parsed_after_check[:1] == parsed[:1]
    assert parsed_after_check[1][datetime(2018, 1, 1, 0, 0)]['DD'] == (361.0, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 0, 10)]['FF'] == (-1.6, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 0, 20)]['Tmedia'] == (47.0, False)


def test_do_internal_consistence_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    parsed = rmn.parse(filepath, parameters_filepath=parameters_filepath)

    # right file
    limiting_params = {'Tmedia': ('FF', 'UR media')}
    err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with errors
    limiting_params = {'Tmedia': ('UR media', 'DD')}
    err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (4, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (10, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (16, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (22, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (28, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (34, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (40, "The values of 'Tmedia' and 'UR media' are not consistent")
    ]
    assert parsed_after_check[:1] == parsed[:1]
    assert parsed_after_check[1][datetime(2018, 1, 1, 0, 0)]['Tmedia'] == (7.2, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 1, 0)]['Tmedia'] == (8.0, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 2, 0)]['Tmedia'] == (9.0, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 3, 0)]['Tmedia'] == (8.7, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 4, 0)]['Tmedia'] == (10.1, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 5, 0)]['Tmedia'] == (9.7, False)
    assert parsed_after_check[1][datetime(2018, 1, 1, 6, 0)]['Tmedia'] == (9.5, False)

    # no limiting parameters: no check
    err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong3.csv')
    err_msgs, _ = rmn.do_internal_consistence_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    with open(filepath, 'w'):
        pass
    err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'RMN header not found')]
    assert not parsed_after_check


def test_parse_and_check(tmpdir):
    # right data
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    limiting_params = {'Tmedia': ('FF', 'UR media')}
    err_msgs, data_parsed = rmn.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert not err_msgs
    assert data_parsed == ('ANCONA', {
        datetime(2018, 1, 1, 0, 0): {
            'DD': (180.0, True),
            'FF': (1.9, True),
            'P': (1018.1, True),
            'Tmedia': (7.2, True),
            'UR media': (63.0, True)},
        datetime(2018, 1, 1, 0, 10): {
            'DD': (180.0, True),
            'FF': (2.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 20): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 30): {
            'DD': (180.0, True),
            'FF': (0.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 40): {
            'DD': (180.0, True),
            'FF': (0.5, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 50): {
            'DD': (180.0, True),
            'FF': (0.8, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 0): {
            'DD': (180.0, True),
            'FF': (1.0, True),
            'P': (1017.6, True),
            'Tmedia': (8.0, True),
            'UR media': (60.0, True)},
        datetime(2018, 1, 1, 1, 10): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 20): {
            'DD': (180.0, True),
            'FF': (1.4, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 30): {
            'DD': (180.0, True),
            'FF': (3.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 40): {
            'DD': (180.0, True),
            'FF': (2.3, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 50): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 0): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (1016.9, True),
            'Tmedia': (9.0, True),
            'UR media': (58.0, True)},
        datetime(2018, 1, 1, 2, 10): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 20): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 30): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 40): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 50): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 0): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (1016.2, True),
            'Tmedia': (8.7, True),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 3, 10): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 20): {
            'DD': (180.0, True),
            'FF': (4.4, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 30): {
            'DD': (180.0, True),
            'FF': (4.2, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 40): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 50): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 0): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (1015.2, True),
            'Tmedia': (10.1, True),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 4, 10): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 20): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 30): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 40): {
            'DD': (180.0, True),
            'FF': (5.5, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 50): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 0): {
            'DD': (180.0, True),
            'FF': (5.8, True),
            'P': (1014.3, True),
            'Tmedia': (9.7, True),
            'UR media': (62.0, True)},
        datetime(2018, 1, 1, 5, 10): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 20): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 30): {
            'DD': (180.0, True),
            'FF': (5.0, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 40): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 50): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P':  (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 6, 0): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (1014.1, True),
            'Tmedia': (9.5, True),
            'UR media': (64.0, True)}
    })

    # global error
    filepath = str(tmpdir.join('report.txt'))
    with open(filepath, 'w'):
        pass
    err_msgs, _ = rmn.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [(0, 'RMN header not found')]

    # various errors
    limiting_params = {'Tmedia': ('UR media', 'DD')}
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong5.csv')
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    err_msgs, data_parsed = rmn.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (10, 'the row is not strictly after the previous'),
        (4, "The value of 'DD' is out of range [0.0, 360.0]"),
        (4, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (5, "The value of 'FF' is out of range [0.0, 102.0]"),
        (6, "The value of 'Tmedia' is out of range [-35.0, 45.0]"),
        (16, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (22, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (28, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (34, "The values of 'Tmedia' and 'UR media' are not consistent"),
        (40, "The values of 'Tmedia' and 'UR media' are not consistent")
    ]
    assert data_parsed == ('ANCONA',  {
        datetime(2018, 1, 1, 0, 0): {
            'DD': (361.0, False),
            'FF': (1.9, True),
            'P': (1018.1, True),
            'Tmedia': (7.2, False),
            'UR media': (63.0, True)},
        datetime(2018, 1, 1, 0, 10): {
            'DD': (180.0, True),
            'FF': (-1.6, False),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 20): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P': (None, True),
            'Tmedia': (47.0, False),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 30): {
            'DD': (180.0, True),
            'FF': (0.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 40): {
            'DD': (180.0, True),
            'FF': (0.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 50): {
            'DD': (180.0, True),
            'FF': (0.8, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 10): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 20): {
            'DD': (180.0, True),
            'FF': (1.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 30): {
            'DD': (180.0, True),
            'FF': (3.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 40): {
            'DD': (180.0, True),
            'FF': (2.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 50): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 0): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (1016.9, True),
            'Tmedia': (9.0, False),
            'UR media': (58.0, True)},
        datetime(2018, 1, 1, 2, 10): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 20): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 30): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 40): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 50): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 0): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (1016.2, True),
            'Tmedia': (8.7, False),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 3, 10): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 20): {
            'DD': (180.0, True),
            'FF': (4.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 30): {
            'DD': (180.0, True),
            'FF': (4.2, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 40): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 50): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 0): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (1015.2, True),
            'Tmedia': (10.1, False),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 4, 10): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 20): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 30): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 40): {
            'DD': (180.0, True),
            'FF': (5.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 50): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 0): {
            'DD': (180.0, True),
            'FF': (5.8, True),
            'P': (1014.3, True),
            'Tmedia': (9.7, False),
            'UR media': (62.0, True)},
        datetime(2018, 1, 1, 5, 10): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 20): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 30): {
            'DD': (180.0, True),
            'FF': (5.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 40): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 50): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 6, 0): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (1014.1, True),
            'Tmedia': (9.5, False),
            'UR media': (64.0, True)}
    })
