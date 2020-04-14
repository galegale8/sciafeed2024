
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


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    expected_thresholds = {
        'Bagnatura_f': [0.0, 60.0],
        'DD': [0.0, 360.0],
        'FF': [0.0, 102.0],
        'INSOL': [0.0, 60.0],
        'PREC': [0.0, 989.0],
        'RADSOL': [0.0, 100.0],
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
    row = " 18 01 01 01.00 01   0.0   2.8  86  58 357   0.5 1001     1   0 46.077222"
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    parameters_map = arpafvg.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = [
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'PREC', 0.0, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'Tmedia', 2.8, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'UR media', 86.0, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'Bagnatura_f', 58.0, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'DD', 357.0, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'FF', 0.5, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'Pstaz', 1001.0, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'RADSOL', 0.0239, True],
        [{'lat': 46.077222}, datetime(2018, 1, 1, 0, 0), 'INSOL', 0.0, True]
    ]
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
        (4, 'the latitude changes'),
        (5, 'duplication of rows with different data'),
        (6, 'it is not strictly after the previous'),
        (7, 'the time is not coherent with the filename')
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    metadata = {'cod_utente': '00001', 'start_date': datetime(2018, 1, 1, 1, 0),
                'end_date': datetime(2019, 1, 1, 1, 0), 'lat': 46.077222, 'format': 'ARPA-FVG',
                'source': 'arpafvg/loc01_00001_2018010101_2019010101.dat'}
    expected_data = [
        [metadata, datetime(2018, 1, 1, 0, 0), 'PREC', 0.0, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'Tmedia', 2.8, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'UR media', 86.0, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'Bagnatura_f', 58.0, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'DD', 357.0, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'FF', 0.5, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'Pstaz', 1001.0, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'RADSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 0, 0), 'INSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'PREC', 0.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'Tmedia', 3.1, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'UR media', 85.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'Bagnatura_f', 59.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'DD', 317.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'FF', 1.6, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'Pstaz', 1001.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'RADSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 1, 0), 'INSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'PREC', 0.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'Tmedia', 3.4, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'UR media', 82.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'Bagnatura_f', 39.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'DD', 345.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'FF', 1.2, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'Pstaz', 1000.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'RADSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 2, 0), 'INSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'PREC', 0.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'Tmedia', 3.4, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'UR media', 82.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'Bagnatura_f', 0.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'DD', 313.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'FF', 1.8, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'Pstaz', 999.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'RADSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 3, 0), 'INSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'PREC', 0.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'Tmedia', 3.4, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'UR media', 83.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'Bagnatura_f', 0.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'DD', 348.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'FF', 0.9, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'Pstaz', 998.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'RADSOL', 0.0, True],
        [metadata, datetime(2018, 1, 1, 4, 0), 'INSOL', 0.0, True]
    ]
    effective = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
    for i, record in enumerate(effective):
        assert effective[i][1:] == expected_data[i][1:]
        expected_md = expected_data[i][0]
        expected_md['row'] = i // 9 + 1
        assert effective[i][0] == expected_md
