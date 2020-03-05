
from datetime import datetime
from os.path import join, exists

from sciafeed import bolzano
from . import TEST_DATA_PATH

import pytest


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    expected_map = {
        '3': {
            'column': '3',
            'description': 'Precipitazione in mm',
            'max': '989',
            'min': '0',
            'par_code': 'PREC',
            'unit': 'mm'},
        '4': {
            'column': '4',
            'description': 'Temperatura massima dell’aria',
            'max': '50',
            'min': '-30',
            'par_code': 'Tmax',
            'unit': '°C'},
        '5': {
            'column': '5',
            'description': 'Temperatura minima dell’aria',
            'max': '40',
            'min': '-40',
            'par_code': 'Tmin',
            'unit': '°C'}
    }
    parameter_map = bolzano.load_parameter_file(test_filepath)
    assert parameter_map == expected_map
    

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
        'code': '02500MS',
        'desc': 'Marienberg - Monte Maria',
        'height': '1310',
        'utmx': '616288',
        'utmy': '5173583'}
    effective = bolzano.get_station_props(filepath)
    assert effective == expected

    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.txt')
    with pytest.raises(ValueError):
        bolzano.get_station_props(filepath)


def test_parse_row():
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    parameters_map = bolzano.load_parameter_file(parameters_filepath=parameters_filepath)

    row = ['', '01.01.1981', '0,0', '9,0', '3,0']

    expected = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (0.0, True)}
    )
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


def test_parse():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    expected_data = ('02500MS', {
        datetime(1981, 1, 1, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (9.0, True),
            'Tmin': (3.0, True)},
        datetime(1981, 1, 2, 0, 0): {
            'PREC': (0.4, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 3, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 4, 0, 0): {
            'PREC': (14.5, True),
            'Tmax': (9.0, True),
            'Tmin': (1.0, True)},
        datetime(1981, 1, 5, 0, 0): {
            'PREC': (5.1, True),
            'Tmax': (3.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 6, 0, 0): {
            'PREC': (1.0, True),
            'Tmax': (-5.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 7, 0, 0): {
            'PREC': (6.1, True),
            'Tmax': (-5.0, True),
            'Tmin': (-9.0, True)},
        datetime(1981, 1, 8, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (-7.0, True),
            'Tmin': (-13.0, True)}})
    effective = bolzano.parse(filepath, parameters_filepath)
    assert effective == expected_data


def test_export(tmpdir):
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    data = bolzano.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows1 = [
        'station;latitude;date;parameter;value;valid\n',
        '02500MS;;1981-01-01T00:00:00;Tmin;3.0;1\n',
        '02500MS;;1981-01-01T00:00:00;Tmax;9.0;1\n',
        '02500MS;;1981-01-01T00:00:00;PREC;0.0;1\n',
        '02500MS;;1981-01-02T00:00:00;Tmin;-4.0;1\n',
        '02500MS;;1981-01-02T00:00:00;Tmax;5.0;1\n',
        '02500MS;;1981-01-02T00:00:00;PREC;0.4;1\n',
        '02500MS;;1981-01-03T00:00:00;Tmin;-4.0;1\n',
        '02500MS;;1981-01-03T00:00:00;Tmax;5.0;1\n',
        '02500MS;;1981-01-03T00:00:00;PREC;0.0;1\n',
        '02500MS;;1981-01-04T00:00:00;Tmin;1.0;1\n',
        '02500MS;;1981-01-04T00:00:00;Tmax;9.0;1\n',
        '02500MS;;1981-01-04T00:00:00;PREC;14.5;1\n',
        '02500MS;;1981-01-05T00:00:00;Tmin;-8.0;1\n',
        '02500MS;;1981-01-05T00:00:00;Tmax;3.0;1\n',
        '02500MS;;1981-01-05T00:00:00;PREC;5.1;1\n',
        '02500MS;;1981-01-06T00:00:00;Tmin;-8.0;1\n',
        '02500MS;;1981-01-06T00:00:00;Tmax;-5.0;1\n',
        '02500MS;;1981-01-06T00:00:00;PREC;1.0;1\n',
        '02500MS;;1981-01-07T00:00:00;Tmin;-9.0;1\n',
        '02500MS;;1981-01-07T00:00:00;Tmax;-5.0;1\n',
        '02500MS;;1981-01-07T00:00:00;PREC;6.1;1\n',
        '02500MS;;1981-01-08T00:00:00;Tmin;-13.0;1\n',
        '02500MS;;1981-01-08T00:00:00;Tmax;-7.0;1\n',
        '02500MS;;1981-01-08T00:00:00;PREC;0.0;1\n'
    ]
    assert not exists(out_filepath)
    bolzano.export(data, out_filepath)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows1

    # omit Tmin
    out_filepath = str(tmpdir.join('datafile2.csv'))
    expected_rows2 = [
        'station;latitude;date;parameter;value;valid\n',
        '02500MS;;1981-01-01T00:00:00;PREC;0.0;1\n',
        '02500MS;;1981-01-02T00:00:00;PREC;0.4;1\n',
        '02500MS;;1981-01-03T00:00:00;PREC;0.0;1\n',
        '02500MS;;1981-01-04T00:00:00;PREC;14.5;1\n',
        '02500MS;;1981-01-05T00:00:00;PREC;5.1;1\n',
        '02500MS;;1981-01-06T00:00:00;PREC;1.0;1\n',
        '02500MS;;1981-01-07T00:00:00;PREC;6.1;1\n',
        '02500MS;;1981-01-08T00:00:00;PREC;0.0;1\n'
    ]
    assert not exists(out_filepath)
    bolzano.export(data, out_filepath, omit_parameters=('Tmin', 'Tmax'))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows2


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


def test_row_weak_climatologic_check():
    parameters_thresholds = {'PREC': [0.0, 989.0], 'Tmax': [-30.0, 50.0], 'Tmin': [-40.0, 40.0]}

    # right row
    parsed_row = (
        datetime(1981, 1, 1, 0, 0), {'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (0.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == parsed_row

    # wrong rows: low
    parsed_row = (datetime(1981, 1, 1, 0, 0), {
        'Tmin': (-40.5, True), 'Tmax': (9.0, True), 'PREC': (0.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert err_msgs == ["The value of 'Tmin' is out of range [-40.0, 40.0]"]
    assert parsed_row_updated == (datetime(1981, 1, 1, 0, 0), {
        'Tmin': (-40.5, False), 'Tmax': (9.0, True), 'PREC': (0.0, True)}
    )

    # wrong rows: high
    parsed_row = (datetime(1981, 1, 1, 0, 0), {
        'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (1000.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert err_msgs == ["The value of 'PREC' is out of range [0.0, 989.0]"]
    assert parsed_row_updated == (datetime(1981, 1, 1, 0, 0), {
        'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (1000.0, False)}
    )

    # no check if not valid or None
    parsed_row = (datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (1000.0, False)})
    err_msgs, parsed_row_updated = bolzano.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == parsed_row

    # no check if no thresholds
    parsed_row = (datetime(1981, 1, 1, 0, 0), {
        'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (1000.0, True)})
    err_msgs, parsed_row_updated = bolzano.row_weak_climatologic_check(parsed_row)
    assert not err_msgs
    assert parsed_row_updated == parsed_row


def test_do_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')

    # right file
    expected_data = ('02500MS', {
        datetime(1981, 1, 1, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (9.0, True),
            'Tmin': (3.0, True)},
        datetime(1981, 1, 2, 0, 0): {
            'PREC': (0.4, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 3, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 4, 0, 0): {
            'PREC': (14.5, True),
            'Tmax': (9.0, True),
            'Tmin': (1.0, True)},
        datetime(1981, 1, 5, 0, 0): {
            'PREC': (5.1, True),
            'Tmax': (3.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 6, 0, 0): {
            'PREC': (1.0, True),
            'Tmax': (-5.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 7, 0, 0): {
            'PREC': (6.1, True),
            'Tmax': (-5.0, True),
            'Tmin': (-9.0, True)},
        datetime(1981, 1, 8, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (-7.0, True),
            'Tmin': (-13.0, True)}}
    )
    err_msgs, parsed_data = bolzano.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_data == expected_data

    # with global formatting errors
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong1.xls')
    err_msgs, parsed_data = bolzano.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [(0, 'BOLZANO file not compliant')]
    assert not parsed_data

    # with some errors
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong3.xls')
    err_msgs, parsed_data = bolzano.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [
        (17, "The value of 'Tmax' is out of range [-30.0, 50.0]"),
        (24, "The value of 'PREC' is out of range [0.0, 989.0]")]
    assert parsed_data == ('02500MS', {
        datetime(1981, 1, 3, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 4, 0, 0): {
            'PREC': (14.5, True),
            'Tmax': (9999.0, False),
            'Tmin': (1.0, True)},
        datetime(1981, 1, 5, 0, 0): {
            'PREC': (5.1, True),
            'Tmax': (3.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 6, 0, 0): {
            'PREC': (1.0, True),
            'Tmax': (-5.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 7, 0, 0): {
            'PREC': (6.1, True),
            'Tmax': (-5.0, True),
            'Tmin': (-9.0, True)},
        datetime(1981, 1, 8, 0, 0): {
            'PREC': (-3.0, False),
            'Tmax': (-7.0, True),
            'Tmin': (-13.0, True)}})


def test_row_internal_consistence_check():
    limiting_params = {'Tmin': ('PREC', 'Tmax')}

    # right row
    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (0.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # wrong value
    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (2.0, True), 'PREC': (10.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert err_msgs == ["The values of 'Tmin' and 'PREC' are not consistent",
                        "The values of 'Tmin' and 'Tmax' are not consistent"]
    assert parsed_row_updated == (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, False), 'Tmax': (2.0, True), 'PREC': (10.0, True)}
    )

    # no check if the value is invalid or None
    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, False), 'Tmax': (9.0, True), 'PREC': (10.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (None, True), 'Tmax': (9.0, True), 'PREC': (10.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if one of the limiting parameters is invalid or None
    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (10.0, False)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (None, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if no limiting parameters
    row_parsed = (
        datetime(1981, 1, 1, 0, 0), {
            'Tmin': (3.0, True), 'Tmax': (9.0, True), 'PREC': (10.0, True)}
    )
    err_msgs, parsed_row_updated = bolzano.row_internal_consistence_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_do_internal_consistence_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    parsed = bolzano.parse(filepath, parameters_filepath=parameters_filepath)

    # file with errors
    limiting_params = {'Tmin': ('PREC', 'Tmax')}
    err_msgs, parsed_after_check = bolzano.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (15, "The values of 'Tmin' and 'PREC' are not consistent"),
        (16, "The values of 'Tmin' and 'PREC' are not consistent"),
        (17, "The values of 'Tmin' and 'PREC' are not consistent"),
        (18, "The values of 'Tmin' and 'PREC' are not consistent"),
        (19, "The values of 'Tmin' and 'PREC' are not consistent"),
        (20, "The values of 'Tmin' and 'PREC' are not consistent"),
        (21, "The values of 'Tmin' and 'PREC' are not consistent")
    ]
    assert parsed_after_check == ('02500MS', {
        datetime(1981, 1, 1, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (9.0, True),
            'Tmin': (3.0, True)},
        datetime(1981, 1, 2, 0, 0): {
            'PREC': (0.4, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, False)},
        datetime(1981, 1, 3, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, False)},
        datetime(1981, 1, 4, 0, 0): {
            'PREC': (14.5, True),
            'Tmax': (9.0, True),
            'Tmin': (1.0, False)},
        datetime(1981, 1, 5, 0, 0): {
            'PREC': (5.1, True),
            'Tmax': (3.0, True),
            'Tmin': (-8.0, False)},
        datetime(1981, 1, 6, 0, 0): {
            'PREC': (1.0, True),
            'Tmax': (-5.0, True),
            'Tmin': (-8.0, False)},
        datetime(1981, 1, 7, 0, 0): {
            'PREC': (6.1, True),
            'Tmax': (-5.0, True),
            'Tmin': (-9.0, False)},
        datetime(1981, 1, 8, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (-7.0, True),
            'Tmin': (-13.0, False)}})

    # no limiting parameters: no check
    err_msgs, parsed_after_check = bolzano.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong2.xls')
    err_msgs, _ = bolzano.do_internal_consistence_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    with open(filepath, 'w'):
        pass
    err_msgs, parsed_after_check = bolzano.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .xls, found .txt')]
    assert not parsed_after_check


def test_parse_and_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    expected_data = ('02500MS', {
        datetime(1981, 1, 1, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (9.0, True),
            'Tmin': (3.0, True)},
        datetime(1981, 1, 2, 0, 0): {
            'PREC': (0.4, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 3, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, True)},
        datetime(1981, 1, 4, 0, 0): {
            'PREC': (14.5, True),
            'Tmax': (9.0, True),
            'Tmin': (1.0, True)},
        datetime(1981, 1, 5, 0, 0): {
            'PREC': (5.1, True),
            'Tmax': (3.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 6, 0, 0): {
            'PREC': (1.0, True),
            'Tmax': (-5.0, True),
            'Tmin': (-8.0, True)},
        datetime(1981, 1, 7, 0, 0): {
            'PREC': (6.1, True),
            'Tmax': (-5.0, True),
            'Tmin': (-9.0, True)},
        datetime(1981, 1, 8, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (-7.0, True),
            'Tmin': (-13.0, True)}})
    err_msgs, parsed_data = bolzano.parse_and_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_data == expected_data

    # with some errors
    limiting_params = {'Tmin': ('PREC', 'Tmax')}
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong3.xls')
    err_msgs, parsed_data = bolzano.parse_and_check(filepath, parameters_filepath,
                                                    limiting_params=limiting_params)
    assert err_msgs == [
        (14, 'the date format is wrong'),
        (15, 'the row contains values not numeric'),
        (18, 'the row is not strictly after the previous'),
        (22, 'the row is duplicated with different values'),
        (16, "The values of 'Tmin' and 'PREC' are not consistent"),
        (17, "The value of 'Tmax' is out of range [-30.0, 50.0]"),
        (17, "The values of 'Tmin' and 'PREC' are not consistent"),
        (19, "The values of 'Tmin' and 'PREC' are not consistent"),
        (20, "The values of 'Tmin' and 'PREC' are not consistent"),
        (21, "The values of 'Tmin' and 'PREC' are not consistent"),
        (23, "The values of 'Tmin' and 'PREC' are not consistent"),
        (24, "The value of 'PREC' is out of range [0.0, 989.0]")
    ]
    assert parsed_data == ('02500MS', {
        datetime(1981, 1, 3, 0, 0): {
            'PREC': (0.0, True),
            'Tmax': (5.0, True),
            'Tmin': (-4.0, False)},
        datetime(1981, 1, 4, 0, 0): {
            'PREC': (14.5, True),
            'Tmax': (9999.0, False),
            'Tmin': (1.0, False)},
        datetime(1981, 1, 5, 0, 0): {
            'PREC': (5.1, True),
            'Tmax': (3.0, True),
            'Tmin': (-8.0, False)},
        datetime(1981, 1, 6, 0, 0): {
            'PREC': (1.0, True),
            'Tmax': (-5.0, True),
            'Tmin': (-8.0, False)},
        datetime(1981, 1, 7, 0, 0): {
            'PREC': (6.1, True),
            'Tmax': (-5.0, True),
            'Tmin': (-9.0, False)},
        datetime(1981, 1, 8, 0, 0): {
            'PREC': (-3.0, False),
            'Tmax': (-7.0, True),
            'Tmin': (-13.0, True)}}
    )

    # global error
    filepath = str(tmpdir.join('report.txt'))
    with open(filepath, 'w'):
        pass
    err_msgs, parsed_after_check = bolzano.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [(0, 'Extension expected must be .xls, found .txt')]
    assert not parsed_after_check


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
    assert bolzano.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong1.csv')
    assert not bolzano.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    assert not bolzano.is_format_compliant(filepath)
