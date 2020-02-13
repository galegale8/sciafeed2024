
from datetime import datetime
from os.path import join, exists

from sciafeed import rmn
from . import TEST_DATA_PATH
from pprint import pprint
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


def test_validate_row_format():
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
    parameters_map = rmn.load_parameter_file(parameters_filepath=parameters_filepath)

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
                                        'UR media': (64.0, True)}}
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


# def test_row_weak_climatologic_check():
#     parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
#     parameters_map = rmn.load_parameter_file(parameters_filepath)
#     parameters_thresholds = rmn.load_parameter_thresholds(parameters_filepath)
#
#     # right row
#     row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
#           "      1      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
#         row_parsed, parameters_thresholds)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#     # two errors
#     assert parameters_thresholds['1'] == [0, 1020]
#     assert parameters_thresholds['9'] == [20, 100]
#     row = "201301010700 43.876999   1021    358     65  32767  32767  32767  32767  32767" \
#           "    101  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
#           "      1      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
#         row_parsed, parameters_thresholds)
#     assert err_msgs == ["The value of '1' is out of range [0.0, 1020.0]",
#                         "The value of '9' is out of range [20.0, 100.0]"]
#     assert parsed_row_updated[:2] == row_parsed[:2]
#     assert parsed_row_updated[2]['1'] == (1021.0, False)
#     parsed_row_updated[2]['1'] = (1021.0, True)
#     parsed_row_updated[2]['9'] = (101.0, True)
#     assert parsed_row_updated == row_parsed
#
#     # no check if no parameters_thresholds
#     err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(row_parsed)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#     # no check if the value is already invalid
#     row = "201301010700 43.876999   1021    358     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
#           "      2      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
#         row_parsed, parameters_thresholds)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#     # no check if thresholds are not defined
#     assert '12' not in parameters_thresholds
#     row = "201301010700 43.876999   1021    358     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  99999  32767  32767  32767  32767  32767  32767  32767" \
#           "      2      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_weak_climatologic_check(
#         row_parsed, parameters_thresholds)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#
# def test_row_internal_consistence_check():
#     parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
#     parameters_map = rmn.load_parameter_file(parameters_filepath)
#     limiting_params = {'1': ('2', '3')}
#
#     # right row
#     row = "201301010600 43.876999     31      6     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  10181  32767  32767  32767  32767  32767  32767  32767" \
#           "      1      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
#         row_parsed, limiting_params)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#     # wrong value
#     row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
#           "      1      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
#         row_parsed, limiting_params)
#     assert err_msgs == ["The values of '1', '2' and '3' are not consistent"]
#     assert parsed_row_updated[:2] == row_parsed[:2]
#     assert parsed_row_updated[2]['1'] == (20.0, False)
#     parsed_row_updated[2]['1'] = (20.0, True)
#     assert parsed_row_updated == row_parsed
#
#     # no check if no limiting parameters
#     err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(row_parsed)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#     # no check if the value is invalid
#     row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
#           "      2      1      1      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
#         row_parsed, limiting_params)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#     # no check if one of the thresholds is invalid
#     row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
#           "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
#           "      1      1      2      2      2      2      2      2      1      2      2" \
#           "      1      2      2      2      2      2      2      2"
#     row_parsed = rmn.parse_row(row, parameters_map)
#     err_msgs, parsed_row_updated = rmn.row_internal_consistence_check(
#         row_parsed, limiting_params)
#     assert not err_msgs
#     assert parsed_row_updated == row_parsed
#
#
# def test_do_weak_climatologic_check(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
#
#     # right file
#     filepath = join(TEST_DATA_PATH, 'rmn', 'loc01_70001_201301010000_201401010100.dat')
#     parsed = rmn.parse(filepath, parameters_filepath=parameters_filepath)
#     err_msgs, parsed_after_check = rmn.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with specific errors
#     filepath = join(TEST_DATA_PATH, 'rmn', 'wrong_70002_201301010000_201401010100.dat')
#     parsed = rmn.parse(filepath, parameters_filepath=parameters_filepath)
#     err_msgs, parsed_after_check = rmn.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert err_msgs == [
#         (1, "The value of '1' is out of range [0.0, 1020.0]"),
#         (2, "The value of '2' is out of range [0.0, 360.0]"),
#         (3, "The value of '3' is out of range [-350.0, 450.0]"),
#     ]
#     assert parsed_after_check[:2] == parsed[:2]
#     assert parsed_after_check[2][datetime(2013, 1, 1, 0, 0)]['1'] == (2000.0, False)
#     assert parsed_after_check[2][datetime(2013, 1, 1, 1, 0)]['2'] == (361.0, False)
#     assert parsed_after_check[2][datetime(2013, 1, 1, 2, 0)]['3'] == (-351.0, False)
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'rmn', 'wrong_70001_201301010000_201401010100.dat')
#     err_msgs, _ = rmn.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = rmn.do_weak_climatologic_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check
#
#
# def test_do_internal_consistence_check(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
#     filepath = join(TEST_DATA_PATH, 'rmn', 'loc01_70001_201301010000_201401010100.dat')
#     parsed = rmn.parse(filepath, parameters_filepath=parameters_filepath)
#
#     # right file
#     limiting_params = {'3': ('4', '5')}
#     err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with errors
#     limiting_params = {'3': ('1', '2')}
#     err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [
#         (5, "The values of '3', '1' and '2' are not consistent"),
#         (6, "The values of '3', '1' and '2' are not consistent"),
#         (7, "The values of '3', '1' and '2' are not consistent"),
#         (10, "The values of '3', '1' and '2' are not consistent"),
#         (20, "The values of '3', '1' and '2' are not consistent"),
#     ]
#     assert parsed_after_check[:2] == parsed[:2]
#     assert parsed_after_check[2][datetime(2013, 1, 1, 4, 0)]['3'] == (64.0, False)
#     assert parsed_after_check[2][datetime(2013, 1, 1, 5, 0)]['3'] == (67.0, False)
#     assert parsed_after_check[2][datetime(2013, 1, 1, 6, 0)]['3'] == (65.0, False)
#
#     # no limiting parameters: no check
#     err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'rmn', 'wrong_70001_201301010000_201401010100.dat')
#     err_msgs, _ = rmn.do_internal_consistence_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = rmn.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check
#
#
# def test_parse_and_check(tmpdir):
#     filepath = join(TEST_DATA_PATH, 'rmn', 'wrong_70002_201301010000_201401010100.dat')
#     parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
#     limiting_params = {'3': ('1', '2')}
#     err_msgs, data_parsed = rmn.parse_and_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [
#         (1, "The value of '1' is out of range [0.0, 1020.0]"),
#         (2, "The value of '2' is out of range [0.0, 360.0]"),
#         (3, "The value of '3' is out of range [-350.0, 450.0]"),
#         (5, "The values of '3', '1' and '2' are not consistent"),
#         (6, "The values of '3', '1' and '2' are not consistent"),
#         (7, "The values of '3', '1' and '2' are not consistent"),
#         (10, "The values of '3', '1' and '2' are not consistent"),
#         (20, "The values of '3', '1' and '2' are not consistent"),
#     ]
#     assert data_parsed == ('70002', 43.876999, {
#         datetime(2013, 1, 1, 0, 0): {
#             '1': (2000.0, False),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10205.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (355.0, True),
#             '3': (68.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (83.0, True)},
#         datetime(2013, 1, 1, 1, 0): {
#             '1': (6.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10198.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (361.0, False),
#             '3': (65.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (86.0, True)},
#         datetime(2013, 1, 1, 2, 0): {
#             '1': (3.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10196.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (288.0, True),
#             '3': (-351.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (86.0, True)},
#         datetime(2013, 1, 1, 3, 0): {
#             '1': (11.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10189.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (357.0, True),
#             '3': (63.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (87.0, True)},
#         datetime(2013, 1, 1, 4, 0): {
#             '1': (9.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10184.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (1.0, True),
#             '3': (64.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (88.0, True)},
#         datetime(2013, 1, 1, 5, 0): {
#             '1': (30.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10181.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (6.0, True),
#             '3': (67.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (89.0, True)},
#         datetime(2013, 1, 1, 6, 0): {
#             '1': (31.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10181.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (6.0, True),
#             '3': (65.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (93.0, True)},
#         datetime(2013, 1, 1, 7, 0): {
#             '1': (20.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10182.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (358.0, True),
#             '3': (65.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (93.0, True)},
#         datetime(2013, 1, 1, 8, 0): {
#             '1': (5.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10182.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (342.0, True),
#             '3': (66.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (95.0, True)},
#         datetime(2013, 1, 1, 9, 0): {
#             '1': (35.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10179.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (12.0, True),
#             '3': (106.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (88.0, True)},
#         datetime(2013, 1, 1, 10, 0): {
#             '1': (13.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10182.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (154.0, True),
#             '3': (121.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (72.0, True)},
#         datetime(2013, 1, 1, 11, 0): {
#             '1': (54.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10177.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (218.0, True),
#             '3': (123.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (69.0, True)},
#         datetime(2013, 1, 1, 12, 0): {
#             '1': (61.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10167.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (225.0, True),
#             '3': (125.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (73.0, True)},
#         datetime(2013, 1, 1, 13, 0): {
#             '1': (65.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10162.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (226.0, True),
#             '3': (122.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (74.0, True)},
#         datetime(2013, 1, 1, 14, 0): {
#             '1': (46.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10161.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (221.0, True),
#             '3': (117.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (78.0, True)},
#         datetime(2013, 1, 1, 15, 0): {
#             '1': (19.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10161.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (233.0, True),
#             '3': (110.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (82.0, True)},
#         datetime(2013, 1, 1, 16, 0): {
#             '1': (28.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10158.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (355.0, True),
#             '3': (100.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (96.0, True)},
#         datetime(2013, 1, 1, 17, 0): {
#             '1': (24.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10156.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (345.0, True),
#             '3': (99.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (96.0, True)},
#         datetime(2013, 1, 1, 18, 0): {
#             '1': (26.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10155.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (357.0, True),
#             '3': (101.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (97.0, True)},
#         datetime(2013, 1, 1, 19, 0): {
#             '1': (26.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10154.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (2.0, True),
#             '3': (99.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (100.0, True)}
#         }
#     )
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, _ = rmn.parse_and_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#
#
# def test_make_report(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')
#
#     # no errors
#     in_filepath = join(TEST_DATA_PATH, 'rmn', 'loc01_70001_201301010000_201401010100.dat')
#     limiting_params = {'3': ('4', '5')}
#     out_filepath = str(tmpdir.join('report.txt'))
#     outdata_filepath = str(tmpdir.join('data.csv'))
#     assert not exists(out_filepath)
#     assert not exists(outdata_filepath)
#     msgs, data_parsed = rmn.make_report(
#         in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
#         limiting_params=limiting_params)
#     assert exists(out_filepath)
#     assert exists(outdata_filepath)
#     assert "No errors found" in msgs
#
#     # some formatting errors
#     in_filepath = join(TEST_DATA_PATH, 'rmn', 'wrong_70001_201301010000_201401010100.dat')
#     limiting_params = {'3': ('4', '5')}
#     out_filepath = str(tmpdir.join('report2.txt'))
#     outdata_filepath = str(tmpdir.join('data2.csv'))
#     assert not exists(out_filepath)
#     assert not exists(outdata_filepath)
#     msgs, data_parsed = rmn.make_report(
#         in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
#         limiting_params=limiting_params)
#     assert exists(out_filepath)
#     assert exists(outdata_filepath)
#     err_msgs = [
#         "Row 2: The spacing in the row is wrong",
#         'Row 3: the latitude changes',
#         'Row 5: it is not strictly after the previous',
#         'Row 21: duplication of rows with different data',
#         'Row 22: the time is not coherent with the filename',
#     ]
#     for err_msg in err_msgs:
#         assert err_msg in msgs
#
#     # some errors
#     in_filepath = join(TEST_DATA_PATH, 'rmn', 'wrong_70002_201301010000_201401010100.dat')
#     limiting_params = {'3': ('1', '2')}
#     out_filepath = str(tmpdir.join('report3.txt'))
#     outdata_filepath = str(tmpdir.join('data3.csv'))
#     assert not exists(out_filepath)
#     assert not exists(outdata_filepath)
#     msgs, data_parsed = rmn.make_report(
#         in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
#         limiting_params=limiting_params)
#     assert exists(out_filepath)
#     assert exists(outdata_filepath)
#     err_msgs = [
#         "Row 1: The value of '1' is out of range [0.0, 1020.0]",
#         "Row 2: The value of '2' is out of range [0.0, 360.0]",
#         "Row 3: The value of '3' is out of range [-350.0, 450.0]",
#         "Row 5: The values of '3', '1' and '2' are not consistent",
#         "Row 6: The values of '3', '1' and '2' are not consistent",
#         "Row 7: The values of '3', '1' and '2' are not consistent",
#         "Row 10: The values of '3', '1' and '2' are not consistent",
#         "Row 20: The values of '3', '1' and '2' are not consistent"
#     ]
#     for err_msg in err_msgs:
#         assert err_msg in msgs
#     assert data_parsed == ('70002', 43.876999, {
#         datetime(2013, 1, 1, 0, 0): {
#             '1': (2000.0, False),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10205.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (355.0, True),
#             '3': (68.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (83.0, True)},
#         datetime(2013, 1, 1, 1, 0): {
#             '1': (6.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10198.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (361.0, False),
#             '3': (65.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (86.0, True)},
#         datetime(2013, 1, 1, 2, 0): {
#             '1': (3.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10196.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (288.0, True),
#             '3': (-351.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (86.0, True)},
#         datetime(2013, 1, 1, 3, 0): {
#             '1': (11.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10189.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (357.0, True),
#             '3': (63.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (87.0, True)},
#         datetime(2013, 1, 1, 4, 0): {
#             '1': (9.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10184.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (1.0, True),
#             '3': (64.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (88.0, True)},
#         datetime(2013, 1, 1, 5, 0): {
#             '1': (30.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10181.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (6.0, True),
#             '3': (67.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (89.0, True)},
#         datetime(2013, 1, 1, 6, 0): {
#             '1': (31.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10181.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (6.0, True),
#             '3': (65.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (93.0, True)},
#         datetime(2013, 1, 1, 7, 0): {
#             '1': (20.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10182.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (358.0, True),
#             '3': (65.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (93.0, True)},
#         datetime(2013, 1, 1, 8, 0): {
#             '1': (5.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10182.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (342.0, True),
#             '3': (66.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (95.0, True)},
#         datetime(2013, 1, 1, 9, 0): {
#             '1': (35.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10179.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (12.0, True),
#             '3': (106.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (88.0, True)},
#         datetime(2013, 1, 1, 10, 0): {
#             '1': (13.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10182.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (154.0, True),
#             '3': (121.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (72.0, True)},
#         datetime(2013, 1, 1, 11, 0): {
#             '1': (54.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10177.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (218.0, True),
#             '3': (123.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (69.0, True)},
#         datetime(2013, 1, 1, 12, 0): {
#             '1': (61.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10167.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (225.0, True),
#             '3': (125.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (73.0, True)},
#         datetime(2013, 1, 1, 13, 0): {
#             '1': (65.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10162.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (226.0, True),
#             '3': (122.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (74.0, True)},
#         datetime(2013, 1, 1, 14, 0): {
#             '1': (46.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10161.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (221.0, True),
#             '3': (117.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (78.0, True)},
#         datetime(2013, 1, 1, 15, 0): {
#             '1': (19.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10161.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (233.0, True),
#             '3': (110.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (82.0, True)},
#         datetime(2013, 1, 1, 16, 0): {
#             '1': (28.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10158.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (355.0, True),
#             '3': (100.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (96.0, True)},
#         datetime(2013, 1, 1, 17, 0): {
#             '1': (24.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10156.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (345.0, True),
#             '3': (99.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (96.0, True)},
#         datetime(2013, 1, 1, 18, 0): {
#             '1': (26.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10155.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (357.0, True),
#             '3': (101.0, True),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (97.0, True)},
#         datetime(2013, 1, 1, 19, 0): {
#             '1': (26.0, True),
#             '10': (None, False),
#             '11': (None, False),
#             '12': (10154.0, True),
#             '13': (None, False),
#             '14': (None, False),
#             '15': (None, False),
#             '16': (None, False),
#             '17': (None, False),
#             '18': (None, False),
#             '19': (None, False),
#             '2': (2.0, True),
#             '3': (99.0, False),
#             '4': (None, False),
#             '5': (None, False),
#             '6': (None, False),
#             '7': (None, False),
#             '8': (None, False),
#             '9': (100.0, True)}
#         }
#     )
