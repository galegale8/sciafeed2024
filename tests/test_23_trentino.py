
from datetime import datetime
from os.path import join, exists

from sciafeed import trentino
from . import TEST_DATA_PATH
from pprint import pprint
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
        'code': 'T0001',
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
        expected = (
            datetime(1930, 5, 1, 9, 0), {'Tmin': (10.0, True)}
        )
        effective = trentino.parse_row(row, parameters_map)
        assert effective == expected

    # quality 255 or 151: value None
    for quality in ['151', '255']:
        row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '10.0',
            'quality': quality
        }
        expected = (
            datetime(1930, 5, 1, 9, 0), {'Tmin': (None, True)}
        )
        effective = trentino.parse_row(row, parameters_map)
        assert effective == expected

    # quality bad
    for quality in ['2', '140']:
        row = {
            'date': '09:00:00 01/05/1930',
            'Tmin': '10.0',
            'quality': quality
        }
        expected = (
            datetime(1930, 5, 1, 9, 0), {'Tmin': (10.0, False)}
        )
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
    expected_data = ('T0001', 46.06227631, {
        datetime(1930, 5, 1, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 2, 9, 0): {'Tmin': (11.0, True)},
        datetime(1930, 5, 3, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 4, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 5, 9, 0): {'Tmin': (12.0, True)},
        datetime(1930, 5, 6, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 7, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 8, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 9, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 10, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 11, 9, 0): {'Tmin': (5.0, True)},
        datetime(1930, 5, 12, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 13, 9, 0): {'Tmin': (None, True)},
        datetime(1930, 5, 14, 9, 0): {'Tmin': (9.0, True)}
    })
    effective = trentino.parse(filepath, parameters_filepath)
    assert effective == expected_data


def test_write_data(tmpdir):
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    data = trentino.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows1 = [
        'station;latitude;date;parameter;value;valid\n',
        'T0001;46.06227631;1930-05-01T09:00:00;Tmin;10.0;1\n',
        'T0001;46.06227631;1930-05-02T09:00:00;Tmin;11.0;1\n',
        'T0001;46.06227631;1930-05-03T09:00:00;Tmin;10.0;1\n',
        'T0001;46.06227631;1930-05-04T09:00:00;Tmin;8.0;1\n',
        'T0001;46.06227631;1930-05-05T09:00:00;Tmin;12.0;1\n',
        'T0001;46.06227631;1930-05-06T09:00:00;Tmin;8.0;1\n',
        'T0001;46.06227631;1930-05-07T09:00:00;Tmin;10.0;1\n',
        'T0001;46.06227631;1930-05-08T09:00:00;Tmin;7.0;1\n',
        'T0001;46.06227631;1930-05-09T09:00:00;Tmin;8.0;1\n',
        'T0001;46.06227631;1930-05-10T09:00:00;Tmin;7.0;1\n',
        'T0001;46.06227631;1930-05-11T09:00:00;Tmin;5.0;1\n',
        'T0001;46.06227631;1930-05-12T09:00:00;Tmin;7.0;1\n',
        'T0001;46.06227631;1930-05-14T09:00:00;Tmin;9.0;1\n'
    ]
    assert not exists(out_filepath)
    trentino.write_data(data, out_filepath)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows1

    # omit Tmin
    out_filepath = str(tmpdir.join('datafile2.csv'))
    expected_rows2 = ['station;latitude;date;parameter;value;valid\n']
    assert not exists(out_filepath)
    trentino.write_data(data, out_filepath, omit_parameters=('Tmin',))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows2

    # include missing
    out_filepath = str(tmpdir.join('datafile3.csv'))
    assert not exists(out_filepath)
    trentino.write_data(data, out_filepath, omit_missing=False)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        for row in expected_rows1:
            assert row in rows
        assert 'T0001;46.06227631;1930-05-13T09:00:00;Tmin;;1\n' in rows


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


def test_row_weak_climatologic_check():
    parameters_thresholds = {'PREC': [0.0, 989.0], 'Tmax': [-30.0, 50.0], 'Tmin': [-40.0, 40.0]}

    # right row
    parsed_row = (datetime(1930, 5, 1, 9, 0), {'Tmin': (10.0, True)})
    err_msgs, parsed_row_updated = trentino.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == parsed_row

    # wrong rows: low
    parsed_row = (datetime(1930, 5, 1, 9, 0), {'Tmin': (-100.0, True)})
    err_msgs, parsed_row_updated = trentino.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert err_msgs == ["The value of 'Tmin' is out of range [-40.0, 40.0]"]
    assert parsed_row_updated == (datetime(1930, 5, 1, 9, 0), {'Tmin': (-100.0, False)})

    # wrong rows: high
    parsed_row = (datetime(1930, 5, 1, 9, 0), {'Tmin': (100.0, True)})
    err_msgs, parsed_row_updated = trentino.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert err_msgs == ["The value of 'Tmin' is out of range [-40.0, 40.0]"]
    assert parsed_row_updated == (datetime(1930, 5, 1, 9, 0), {'Tmin': (100.0, False)})

    # no check if not valid or None
    for parsed_row in [
        (datetime(1930, 5, 1, 9, 0), {'Tmin': (100.0, False)}),
        (datetime(1930, 5, 1, 9, 0), {'Tmin': (None, True)})
    ]:
        err_msgs, parsed_row_updated = trentino.row_weak_climatologic_check(
            parsed_row, parameters_thresholds)
        assert not err_msgs
        assert parsed_row_updated == parsed_row

    # no check if no thresholds
    parsed_row = (datetime(1930, 5, 1, 9, 0), {'Tmin': (100.0, True)})
    err_msgs, parsed_row_updated = trentino.row_weak_climatologic_check(parsed_row)
    assert not err_msgs
    assert parsed_row_updated == parsed_row


def test_do_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')

    # right file
    expected_data = ('T0001', {
        datetime(1930, 5, 1, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 2, 9, 0): {'Tmin': (11.0, True)},
        datetime(1930, 5, 3, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 4, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 5, 9, 0): {'Tmin': (12.0, True)},
        datetime(1930, 5, 6, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 7, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 8, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 9, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 10, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 11, 9, 0): {'Tmin': (5.0, True)},
        datetime(1930, 5, 12, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 13, 9, 0): {'Tmin': (None, True)},
        datetime(1930, 5, 14, 9, 0): {'Tmin': (9.0, True)}
    })
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    err_msgs, parsed_data = trentino.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_data == expected_data

    # with global formatting errors
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.xls')
    err_msgs, parsed_data = trentino.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .csv, found .xls')]
    assert not parsed_data

    # with some errors
    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong3.csv')
    err_msgs, parsed_data = trentino.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [(17, "The value of 'Tmin' is out of range [-40.0, 40.0]")]
    assert parsed_data == ('T0001', {
        datetime(1930, 5, 3, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 5, 9, 0): {'Tmin': (12.0, True)},
        datetime(1930, 5, 6, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 8, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 9, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 10, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 11, 9, 0): {'Tmin': (500.0, False)},
        datetime(1930, 5, 12, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 13, 9, 0): {'Tmin': (None, True)},
        datetime(1930, 5, 14, 9, 0): {'Tmin': (9.0, True)}
    })


def test_parse_and_check():
    parameters_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    expected_data = ('T0001', 46.06227631, {
        datetime(1930, 5, 1, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 2, 9, 0): {'Tmin': (11.0, True)},
        datetime(1930, 5, 3, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 4, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 5, 9, 0): {'Tmin': (12.0, True)},
        datetime(1930, 5, 6, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 7, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 8, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 9, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 10, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 11, 9, 0): {'Tmin': (5.0, True)},
        datetime(1930, 5, 12, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 13, 9, 0): {'Tmin': (None, True)},
        datetime(1930, 5, 14, 9, 0): {'Tmin': (9.0, True)}
    })
    err_msgs, parsed_data = trentino.parse_and_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_data == expected_data

    # with some errors
    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong3.csv')
    err_msgs, parsed_data = trentino.parse_and_check(filepath, parameters_filepath)
    assert err_msgs == [
        (5, 'the date format is wrong'),
        (6, 'the value for Tmin is not numeric'),
        (8, 'the row is not strictly after the previous'),
        (12, 'the row is duplicated with different values'),
        (13, 'the value for quality is missing'),
        (17, "The value of 'Tmin' is out of range [-40.0, 40.0]")]
    assert parsed_data == ('T0001', 46.06227631, {
        datetime(1930, 5, 3, 9, 0): {'Tmin': (10.0, True)},
        datetime(1930, 5, 5, 9, 0): {'Tmin': (12.0, True)},
        datetime(1930, 5, 6, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 8, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 9, 9, 0): {'Tmin': (8.0, True)},
        datetime(1930, 5, 10, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 11, 9, 0): {'Tmin': (500.0, False)},
        datetime(1930, 5, 12, 9, 0): {'Tmin': (7.0, True)},
        datetime(1930, 5, 13, 9, 0): {'Tmin': (None, True)},
        datetime(1930, 5, 14, 9, 0): {'Tmin': (9.0, True)}
    })


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    assert trentino.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'trentino', 'wrong1.csv')
    assert not trentino.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    assert not trentino.is_format_compliant(filepath)