
from datetime import datetime
from os.path import join, exists

import pytest

from sciafeed import arpafvg
from . import TEST_DATA_PATH


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    parameter_map = arpafvg.load_parameter_file(test_filepath)
    for i in range(1, 10):
        assert i in parameter_map
        assert 'par_code' in parameter_map[i]
        assert 'description' in parameter_map[i]

from pprint import pprint
def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    expected_thresholds = {
        'Bagnatura_f': [0.0, 60.0],
        'DD': [0.0, 360.0],
        'FF': [0.0, 102.0],
        'INSOL': [0.0, 60.0],
        'PREC': [0.0, 989.0],
        'RADSOL': [0.0, 4186.8],
        'Tmedia': [-35.0, 45.0],
        'UR media': [20.0, 100.0]
    }
    parameter_thresholds = arpafvg.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_parse_filename():
    # simple case loc01_00001_2018010101_2019010101.dat
    filename = 'loc01_00001_2018010101_2019010101.dat'
    expected = ('00001', datetime(2018, 1, 1, 1, 0, 0), datetime(2019, 1, 1, 1, 0, 0))
    effective = arpafvg.parse_filename(filename)
    assert effective == expected

    # more complex
    filename = 'loc01_0022A_1975051511_2019051401.dat'
    expected = ('0022A', datetime(1975, 5, 15, 11, 0, 0), datetime(2019, 5, 14, 1, 0, 0))
    effective = arpafvg.parse_filename(filename)
    assert effective == expected

    # with generic errors
    filenames = [
        'loc01_splitthis_22A_1975051511_2019051401.dat',  # too many '_'
        'loc01_00002_2018010101_2019010101.xls',  # wrong extension
        'loc01_00003_2000051511_1999051401.dat',  # wrong date interval
    ]
    for filename in filenames:
        with pytest.raises(AssertionError):
            arpafvg.parse_filename(filename)

    # formatting errors on dates
    filenames = [
        'loc01_00004_2001130101_2004010101.dat',  # wrong start date
        'loc01_00005_2001010101_2004023001.dat',  # wrong end date
    ]
    for filename in filenames:
        with pytest.raises(ValueError):
            arpafvg.parse_filename(filename)


def test_validate_filename():
    # no error for valid filenames
    right_filenames = [
        'loc01_00001_2018010101_2019010101.dat',
        'loc01_0022A_1975051511_2019051401.dat'
    ]
    for right_filename in right_filenames:
        err_msg = arpafvg.validate_filename(right_filename)
        assert not err_msg

    wrong_filenames = [
        ('loc01_splitthis_22A_1975051511_2019051401.dat',
         "File name 'loc01_splitthis_22A_1975051511_2019051401.dat' is not standard"),
        ('loc01_00002_2018010101_2019010101.xls',
         "Extension expected must be .dat, found .xls"),
        ('loc01_00003_2000051511_1999051401.dat',
         "The time interval in file name 'loc01_00003_2000051511_1999051401.dat' is not valid"),
        ('loc01_00004_2001130101_2004010101.dat',
         "Start date in file name 'loc01_00004_2001130101_2004010101.dat' is not standard"),
        ('loc01_00005_2001010101_2004023001.dat',
         "End date in file name 'loc01_00005_2001010101_2004023001.dat' is not standard"),
        ('loc01_000008_2018010101_2019010101.dat',
         "Station code '000008' is too long"),
    ]
    for wrong_filename, exp_error in wrong_filenames:
        err_msg = arpafvg.validate_filename(wrong_filename)
        assert err_msg
        assert exp_error == err_msg


def test_parse_row():
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    parameters_map = arpafvg.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = (datetime(2018, 1, 1, 1, 0), '46.077222', {
        'Bagnatura_f': (58.0, True),
        'DD': (357.0, True),
        'FF': (0.5, True),
        'INSOL': (0.0, True),
        'PREC': (0.0, True),
        'Pstaz': (1001.0, True),
        'RADSOL': (0.0, True),
        'Tmedia': (2.8, True),
        'UR media': (86.0, True)
    })

    effective = arpafvg.parse_row(row, parameters_map)
    assert effective == expected


def test_validate_row_format():
    # right row
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    assert not arpafvg.validate_row_format(row)

    # empty row no raises errors
    row = '\n'
    assert not arpafvg.validate_row_format(row)

    # too values
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     0   0 46.077222  123"
    assert arpafvg.validate_row_format(row) == "The number of components in the row is wrong"

    # wrong date
    row = " 18 13 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    assert arpafvg.validate_row_format(row) == "The date format in the row is wrong"

    # wrong values
    row = " 18 01 01 01.00 01   a1   2.8  86  58 357   0.5 1001     0   0 46.077222"
    assert arpafvg.validate_row_format(row) == 'The row contains not numeric values'


def test_validate_format(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    assert not arpafvg.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = str(tmpdir.join('loc01_00001_2018010101_2019010101.xls'))
    err_msgs = arpafvg.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'Extension expected must be .dat, found .xls')]

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    err_msgs = arpafvg.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        (1, 'The number of components in the row is wrong'),
        (3, 'duplication of rows with different data'),
        (5, 'the latitude changes'),
        (6, 'the time is not coherent with the filename')
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    station = '00001'
    latitude = 46.077222
    expected_data = {
        datetime(2018, 1, 1, 1, 0): {
            'Bagnatura_f': (58.0, True),
            'DD': (357.0, True),
            'FF': (0.5, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (1001.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (2.8, True),
            'UR media': (86.0, True)},
        datetime(2018, 1, 1, 2, 0): {
            'Bagnatura_f': (59.0, True),
            'DD': (317.0, True),
            'FF': (1.6, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (1001.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.1, True),
            'UR media': (85.0, True)},
        datetime(2018, 1, 1, 3, 0): {
            'Bagnatura_f': (39.0, True),
            'DD': (345.0, True),
            'FF': (1.2, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (1000.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (82.0, True)},
        datetime(2018, 1, 1, 4, 0): {
            'Bagnatura_f': (0.0, True),
            'DD': (313.0, True),
            'FF': (1.8, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (999.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (82.0, True)},
        datetime(2018, 1, 1, 5, 0): {
            'Bagnatura_f': (0.0, True),
            'DD': (348.0, True),
            'FF': (0.9, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (998.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (83.0, True)}}
    effective = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == (station, latitude, expected_data)


def test_write_data(tmpdir):
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    data = arpafvg.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        'station;latitude;date;parameter;value;valid\n',
        '00001;46.077222;2018-01-01T01:00:00;PREC;0.0;1\n',
        '00001;46.077222;2018-01-01T01:00:00;Tmedia;2.8;1\n',
        '00001;46.077222;2018-01-01T01:00:00;UR media;86.0;1\n',
        '00001;46.077222;2018-01-01T01:00:00;Bagnatura_f;58.0;1\n',
        '00001;46.077222;2018-01-01T01:00:00;DD;357.0;1\n',
        '00001;46.077222;2018-01-01T01:00:00;FF;0.5;1\n',
        '00001;46.077222;2018-01-01T01:00:00;RADSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T01:00:00;INSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T02:00:00;PREC;0.0;1\n',
        '00001;46.077222;2018-01-01T02:00:00;Tmedia;3.1;1\n',
        '00001;46.077222;2018-01-01T02:00:00;UR media;85.0;1\n',
        '00001;46.077222;2018-01-01T02:00:00;Bagnatura_f;59.0;1\n',
        '00001;46.077222;2018-01-01T02:00:00;DD;317.0;1\n',
        '00001;46.077222;2018-01-01T02:00:00;FF;1.6;1\n',
        '00001;46.077222;2018-01-01T02:00:00;RADSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T02:00:00;INSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T03:00:00;PREC;0.0;1\n',
        '00001;46.077222;2018-01-01T03:00:00;Tmedia;3.4;1\n',
        '00001;46.077222;2018-01-01T03:00:00;UR media;82.0;1\n',
        '00001;46.077222;2018-01-01T03:00:00;Bagnatura_f;39.0;1\n',
        '00001;46.077222;2018-01-01T03:00:00;DD;345.0;1\n',
        '00001;46.077222;2018-01-01T03:00:00;FF;1.2;1\n',
        '00001;46.077222;2018-01-01T03:00:00;RADSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T03:00:00;INSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T04:00:00;PREC;0.0;1\n',
        '00001;46.077222;2018-01-01T04:00:00;Tmedia;3.4;1\n',
        '00001;46.077222;2018-01-01T04:00:00;UR media;82.0;1\n',
        '00001;46.077222;2018-01-01T04:00:00;Bagnatura_f;0.0;1\n',
        '00001;46.077222;2018-01-01T04:00:00;DD;313.0;1\n',
        '00001;46.077222;2018-01-01T04:00:00;FF;1.8;1\n',
        '00001;46.077222;2018-01-01T04:00:00;RADSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T04:00:00;INSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T05:00:00;PREC;0.0;1\n',
        '00001;46.077222;2018-01-01T05:00:00;Tmedia;3.4;1\n',
        '00001;46.077222;2018-01-01T05:00:00;UR media;83.0;1\n',
        '00001;46.077222;2018-01-01T05:00:00;Bagnatura_f;0.0;1\n',
        '00001;46.077222;2018-01-01T05:00:00;DD;348.0;1\n',
        '00001;46.077222;2018-01-01T05:00:00;FF;0.9;1\n',
        '00001;46.077222;2018-01-01T05:00:00;RADSOL;0.0;1\n',
        '00001;46.077222;2018-01-01T05:00:00;INSOL;0.0;1\n'
    ]
    assert not exists(out_filepath)
    arpafvg.write_data(data, out_filepath, omit_parameters=('Pstaz',))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows


def test_row_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    parameters_map = arpafvg.load_parameter_file(parameters_filepath)
    parameters_thresholds = arpafvg.load_parameter_thresholds(parameters_filepath)

    # right row
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpafvg.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # two errors
    parameters_thresholds['UR media'] = [0, 10]
    parameters_thresholds['Bagnatura_f'] = [-20, 40]
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpafvg.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert err_msgs == ["The value of 'UR media' is out of range [0.0, 10.0]",
                        "The value of 'Bagnatura_f' is out of range [-20.0, 40.0]"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['UR media'] == (86.0, False)
    assert parsed_row_updated[2]['Bagnatura_f'] == (58.0, False)
    parsed_row_updated[2]['UR media'] = (86.0, True)
    parsed_row_updated[2]['Bagnatura_f'] = (58.0, True)
    assert parsed_row_updated == row_parsed

    # no check if no parameters_thresholds
    err_msgs, parsed_row_updated = arpafvg.row_weak_climatologic_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is already invalid
    row_parsed = arpafvg.parse_row(row, parameters_map)
    row_parsed[2]['UR media'] = (86.0, False)
    row_parsed[2]['Bagnatura_f'] = (58.0, False)
    err_msgs, parsed_row_updated = arpafvg.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if thresholds are not defined
    parameters_thresholds = arpafvg.load_parameter_thresholds(parameters_filepath)
    assert 'Pstaz' not in parameters_thresholds
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 9999     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpafvg.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_row_internal_consistence_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    parameters_map = arpafvg.load_parameter_file(parameters_filepath)
    limiting_params = {'PREC': ('FF', 'DD')}  # 1: 6:5

    # right row
    row = " 18 01 01 01.00 01   1.0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpafvg.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # wrong value
    row = " 18 01 01 01.00 01   0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpafvg.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert err_msgs == ["The values of 'PREC', 'FF' and 'DD' are not consistent"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['PREC'] == (0.0, False)
    parsed_row_updated[2]['PREC'] = (0.0, True)
    assert parsed_row_updated == row_parsed

    # no check if no limiting parameters
    err_msgs, parsed_row_updated = arpafvg.row_internal_consistence_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is invalid
    row = " 18 01 01 01.00 01   0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    row_parsed[2]['PREC'] = (0, False)
    err_msgs, parsed_row_updated = arpafvg.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if one of the thresholds is invalid
    row = " 18 01 01 01.00 01   0   2.8  86  58 357   0.5 1001     0   0 46.077222"
    row_parsed = arpafvg.parse_row(row, parameters_map)
    row_parsed[2]['FF'] = (0.5, False)
    err_msgs, parsed_row_updated = arpafvg.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_do_weak_climatologic_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    parsed = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = arpafvg.do_weak_climatologic_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with specific errors
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00002_2018010101_2019010101.dat')
    parsed = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = arpafvg.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [
        (1, "The value of 'FF' is out of range [0.0, 102.0]"),
        (2, "The value of 'DD' is out of range [0.0, 360.0]"),
        (3, "The value of 'PREC' is out of range [0.0, 989.0]")
    ]
    assert parsed_after_check[:2] == parsed[:2]
    assert parsed_after_check[2][datetime(2018, 1, 1, 1, 0)]['FF'] == (103.0, False)
    assert parsed_after_check[2][datetime(2018, 1, 1, 2, 0)]['DD'] == (361.0, False)
    assert parsed_after_check[2][datetime(2018, 1, 1, 3, 0)]['PREC'] == (1000.0, False)
    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    err_msgs, _ = arpafvg.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, parsed_after_check = arpafvg.do_weak_climatologic_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
    assert not parsed_after_check


def test_do_internal_consistence_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    parsed = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)

    # right file
    limiting_params = {'Tmedia': ('FF', 'DD')}  # 2: 6:5
    err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with errors
    limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
    err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (1, "The values of 'PREC', 'Bagnatura_f' and 'DD' are not consistent"),
        (2, "The values of 'PREC', 'Bagnatura_f' and 'DD' are not consistent"),
        (3, "The values of 'PREC', 'Bagnatura_f' and 'DD' are not consistent"),

    ]
    assert parsed_after_check[:2] == parsed[:2]
    assert parsed_after_check[2][datetime(2018, 1, 1, 1, 0)]['PREC'] == (0.0, False)
    assert parsed_after_check[2][datetime(2018, 1, 1, 2, 0)]['PREC'] == (0.0, False)
    assert parsed_after_check[2][datetime(2018, 1, 1, 3, 0)]['PREC'] == (0.0, False)

    # no limiting parameters: no check
    err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    err_msgs, _ = arpafvg.do_internal_consistence_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
    assert not parsed_after_check


def test_parse_and_check(tmpdir):
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
    err_msgs, data_parsed = arpafvg.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (1, 'The number of components in the row is wrong'),
        (3, 'duplication of rows with different data'),
        (5, 'the latitude changes'),
        (6, 'the time is not coherent with the filename'),
        (2, "The values of 'PREC', 'Bagnatura_f' and 'DD' are not consistent")
    ]
    assert data_parsed == ('00001', 46.077222, {
        datetime(2018, 1, 1, 2, 0): {
            'Bagnatura_f': (59.0, True),
            'DD': (317.0, True),
            'FF': (1.6, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, False),
            'Pstaz': (1001.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.1, True),
            'UR media': (85.0, True)},
        datetime(2018, 1, 1, 4, 0): {
            'Bagnatura_f': (0.0, True),
            'DD': (313.0, True),
            'FF': (1.8, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (999.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (82.0, True)}}
    )

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, _ = arpafvg.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]


# FIXME
def test_make_report(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')

    # no errors
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
    out_filepath = str(tmpdir.join('report.txt'))
    outdata_filepath = str(tmpdir.join('data.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = arpafvg.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    assert "No errors found" in msgs

    # some formatting errors
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
    out_filepath = str(tmpdir.join('report2.txt'))
    outdata_filepath = str(tmpdir.join('data2.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = arpafvg.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        'Row 1: The number of components in the row is wrong',
        'Row 3: duplication of rows with different data',
        'Row 5: the latitude changes',
        'Row 6: the time is not coherent with the filename'
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs

    # some errors
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00002_2018010101_2019010101.dat')
    limiting_params = {'3': ('1', '2')}
    out_filepath = str(tmpdir.join('report3.txt'))
    outdata_filepath = str(tmpdir.join('data3.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = arpafvg.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        "Row 1: The value of '1' is out of range [0.0, 1020.0]",
        "Row 2: The value of '2' is out of range [0.0, 360.0]",
        "Row 3: The value of '3' is out of range [-350.0, 450.0]",
        "Row 5: The values of '3', '1' and '2' are not consistent",
        "Row 6: The values of '3', '1' and '2' are not consistent",
        "Row 7: The values of '3', '1' and '2' are not consistent",
        "Row 10: The values of '3', '1' and '2' are not consistent",
        "Row 20: The values of '3', '1' and '2' are not consistent"
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs
    assert data_parsed == ('70002', 43.876999, {
        datetime(2013, 1, 1, 0, 0): {
            '1': (2000.0, False),
            '10': (None, False),
            '11': (None, False),
            '12': (10205.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (355.0, True),
            '3': (68.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (83.0, True)},
        datetime(2013, 1, 1, 1, 0): {
            '1': (6.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10198.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (361.0, False),
            '3': (65.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)},
        datetime(2013, 1, 1, 2, 0): {
            '1': (3.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10196.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (288.0, True),
            '3': (-351.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)},
        datetime(2013, 1, 1, 3, 0): {
            '1': (11.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10189.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (357.0, True),
            '3': (63.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (87.0, True)},
        datetime(2013, 1, 1, 4, 0): {
            '1': (9.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10184.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (1.0, True),
            '3': (64.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (88.0, True)},
        datetime(2013, 1, 1, 5, 0): {
            '1': (30.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10181.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (6.0, True),
            '3': (67.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (89.0, True)},
        datetime(2013, 1, 1, 6, 0): {
            '1': (31.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10181.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (6.0, True),
            '3': (65.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (93.0, True)},
        datetime(2013, 1, 1, 7, 0): {
            '1': (20.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10182.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (358.0, True),
            '3': (65.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (93.0, True)},
        datetime(2013, 1, 1, 8, 0): {
            '1': (5.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10182.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (342.0, True),
            '3': (66.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (95.0, True)},
        datetime(2013, 1, 1, 9, 0): {
            '1': (35.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10179.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (12.0, True),
            '3': (106.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (88.0, True)},
        datetime(2013, 1, 1, 10, 0): {
            '1': (13.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10182.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (154.0, True),
            '3': (121.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (72.0, True)},
        datetime(2013, 1, 1, 11, 0): {
            '1': (54.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10177.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (218.0, True),
            '3': (123.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (69.0, True)},
        datetime(2013, 1, 1, 12, 0): {
            '1': (61.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10167.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (225.0, True),
            '3': (125.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (73.0, True)},
        datetime(2013, 1, 1, 13, 0): {
            '1': (65.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10162.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (226.0, True),
            '3': (122.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (74.0, True)},
        datetime(2013, 1, 1, 14, 0): {
            '1': (46.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10161.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (221.0, True),
            '3': (117.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (78.0, True)},
        datetime(2013, 1, 1, 15, 0): {
            '1': (19.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10161.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (233.0, True),
            '3': (110.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, True)},
        datetime(2013, 1, 1, 16, 0): {
            '1': (28.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10158.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (355.0, True),
            '3': (100.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (96.0, True)},
        datetime(2013, 1, 1, 17, 0): {
            '1': (24.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10156.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (345.0, True),
            '3': (99.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (96.0, True)},
        datetime(2013, 1, 1, 18, 0): {
            '1': (26.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10155.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (357.0, True),
            '3': (101.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (97.0, True)},
        datetime(2013, 1, 1, 19, 0): {
            '1': (26.0, True),
            '10': (None, False),
            '11': (None, False),
            '12': (10154.0, True),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (2.0, True),
            '3': (99.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, True)}
        }
    )
