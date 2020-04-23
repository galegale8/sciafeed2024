
from datetime import datetime
from os.path import join

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

    expected = [
        ({}, datetime(2018, 1, 1, 0, 0), 'DD', 180.0, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'FF', 1.9, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'Tmedia', 7.2, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'P', 1018.1, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'UR media', 63.0, True)
    ]
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
    expected = [
        ({}, datetime(2018, 1, 1, 0, 0), 'DD', None, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'FF', None, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'Tmedia', 7.2, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'P', 1018.1, True),
        ({}, datetime(2018, 1, 1, 0, 0), 'UR media', 63.0, True)
    ]
    effective = rmn.parse_row(row, parameters_map)
    assert effective == expected

    row = {
        'DATA': '20180101',
        'DD': '180',
        'ORA': '00:00',
    }
    expected = [
        ({}, datetime(2018, 1, 1, 0, 0), 'DD', 180.0, True),
    ]
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
    metadata = {'cod_utente': 'ANCONA', 'format': 'RMN', 'source': 'rmn/ancona_right.csv',
                'fieldnames': ['DATA', 'ORA', 'DD', 'FF', 'Tmedia', 'P', 'UR media']}
    expected_data = [
        (metadata, datetime(2018, 1, 1, 0, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 0, 0), 'FF', 1.9, True),
        (metadata, datetime(2018, 1, 1, 0, 0), 'Tmedia', 7.2, True),
        (metadata, datetime(2018, 1, 1, 0, 0), 'P', 1018.1, True),
        (metadata, datetime(2018, 1, 1, 0, 0), 'UR media', 63.0, True),
        (metadata, datetime(2018, 1, 1, 1, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 1, 0), 'FF', 1.0, True),
        (metadata, datetime(2018, 1, 1, 1, 0), 'Tmedia', 8.0, True),
        (metadata, datetime(2018, 1, 1, 1, 0), 'P', 1017.6, True),
        (metadata, datetime(2018, 1, 1, 1, 0), 'UR media', 60.0, True),
        (metadata, datetime(2018, 1, 1, 2, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 2, 0), 'FF', 4.0, True),
        (metadata, datetime(2018, 1, 1, 2, 0), 'Tmedia', 9.0, True),
        (metadata, datetime(2018, 1, 1, 2, 0), 'P', 1016.9, True),
        (metadata, datetime(2018, 1, 1, 2, 0), 'UR media', 58.0, True),
        (metadata, datetime(2018, 1, 1, 3, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 3, 0), 'FF', 3.9, True),
        (metadata, datetime(2018, 1, 1, 3, 0), 'Tmedia', 8.7, True),
        (metadata, datetime(2018, 1, 1, 3, 0), 'P', 1016.2, True),
        (metadata, datetime(2018, 1, 1, 3, 0), 'UR media', 59.0, True),
        (metadata, datetime(2018, 1, 1, 4, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 4, 0), 'FF', 4.5, True),
        (metadata, datetime(2018, 1, 1, 4, 0), 'Tmedia', 10.1, True),
        (metadata, datetime(2018, 1, 1, 4, 0), 'P', 1015.2, True),
        (metadata, datetime(2018, 1, 1, 4, 0), 'UR media', 59.0, True),
        (metadata, datetime(2018, 1, 1, 5, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 5, 0), 'FF', 5.8, True),
        (metadata, datetime(2018, 1, 1, 5, 0), 'Tmedia', 9.7, True),
        (metadata, datetime(2018, 1, 1, 5, 0), 'P', 1014.3, True),
        (metadata, datetime(2018, 1, 1, 5, 0), 'UR media', 62.0, True),
        (metadata, datetime(2018, 1, 1, 6, 0), 'DD', 180.0, True),
        (metadata, datetime(2018, 1, 1, 6, 0), 'FF', 4.6, True),
        (metadata, datetime(2018, 1, 1, 6, 0), 'Tmedia', 9.5, True),
        (metadata, datetime(2018, 1, 1, 6, 0), 'P', 1014.1, True),
        (metadata, datetime(2018, 1, 1, 6, 0), 'UR media', 64.0, True),
    ]
    effective, err_msgs = rmn.parse(filepath, parameters_filepath=parameters_filepath)
    for i, record in enumerate(effective):
        assert effective[i][1:] == expected_data[i][1:]
        expected_md = expected_data[i][0]
        expected_md['row'] = (i // 5) * 6 + 4
        assert effective[i][0] == expected_md
    assert err_msgs == rmn.validate_format(filepath, parameters_filepath=parameters_filepath)
