
from datetime import datetime
from os.path import join

import pytest

from sciafeed import arpa21
from . import TEST_DATA_PATH


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    parameter_map = arpa21.load_parameter_file(test_filepath)
    for i in range(1, 20):
        assert i in parameter_map
        assert 'par_code' in parameter_map[i]
        assert 'description' in parameter_map[i]


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    expected_thresholds = {
        'Bagnatura_f': [0.0, 60.0],
        'DD': [0.0, 360.0],
        'FF': [0.0, 102.0],
        'INSOL_00': [0.0, 1080.0],
        'P': [960.0, 1060.0],
        'PREC': [0.0, 989.0],
        'RADSOL': [0.0, 100.0],
        'Tmax': [-30.0, 50.0],
        'Tmedia': [-35.0, 45.0],
        'Tmin': [-40.0, 40.0],
        'UR max': [20.0, 100.0],
        'UR media': [20.0, 100.0],
        'UR min': [20.0, 100.0]
    }
    parameter_thresholds = arpa21.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_parse_filename():
    # simple case
    filename = 'loc01_00201_201201010000_201301010100.dat'
    expected = ('00201', datetime(2012, 1, 1, 0, 0, 0), datetime(2013, 1, 1, 1, 0, 0))
    effective = arpa21.parse_filename(filename)
    assert effective == expected

    # more complex
    filename = 'loc01_22A_197505151123_201905140159.dat'
    expected = ('0022A', datetime(1975, 5, 15, 11, 23, 0), datetime(2019, 5, 14, 1, 59, 0))
    effective = arpa21.parse_filename(filename)
    assert effective == expected

    # with generic errors
    filenames = [
        'loc01_splitthis_22A_197505151123_201905140159.dat',  # too many '_'
        'loc01_00004_200101010100_200401010100.xls',  # wrong extension
        'loc01_00005_200401010100_200101010100.dat',  # wrong date interval
    ]
    for filename in filenames:
        with pytest.raises(AssertionError):
            arpa21.parse_filename(filename)

    # formatting errors on dates
    filenames = [
        'loc01_00006_200101010160_200401010100.dat',  # wrong start date
        'loc01_00007_200101010100_200402300100.dat',  # wrong end date
    ]
    for filename in filenames:
        with pytest.raises(ValueError):
            arpa21.parse_filename(filename)


def test_validate_filename():
    # no error for valid filenames
    right_filenames = [
        'loc01_00201_201201010000_201301010100.dat',
        'loc01_22A_197505151123_201905140159.dat'
    ]
    for right_filename in right_filenames:
        err_msg = arpa21.validate_filename(right_filename)
        assert not err_msg

    wrong_filenames = [
        ('loc01_splitthis_22A_197505151123_201905140159.dat',
         "File name 'loc01_splitthis_22A_197505151123_201905140159.dat' is not standard"),
        ('loc01_00004_200101010100_200401010100.xls',
         "Extension expected must be .dat, found .xls"),
        ('loc01_005_200401010100_200101010100.dat',
         "The time interval in file name 'loc01_005_200401010100_200101010100.dat' is not valid"),
        ('loc01_00006_200101010160_200401010100.dat',
         "Start date in file name 'loc01_00006_200101010160_200401010100.dat' is not standard"),
        ('loc01_00007_200101010100_200402300100.dat',
         "End date in file name 'loc01_00007_200101010100_200402300100.dat' is not standard"),
        ('loc01_000008_200101010100_200402300100.dat',
         "Station code '000008' is too long"),
    ]
    for wrong_filename, exp_error in wrong_filenames:
        err_msg = arpa21.validate_filename(wrong_filename)
        assert err_msg
        assert exp_error == err_msg


def test_parse_row():
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    parameters_map = arpa21.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = [
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'FF', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'DD', 242.0, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Tmedia', 5.7, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Tmin', 5.5, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Tmax', 6.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), '6', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), '7', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), '8', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'UR media', 83.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'UR min', 80.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'UR max', 85.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), '12', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'P', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Pmin', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Pmax', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'RADSOL', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), '17', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), '18', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'INSOL_00', None, False],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'PREC', 0.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Bagnatura_f', None, False]
    ]
    effective = arpa21.parse_row(row, parameters_map)
    assert effective == expected

    # only valid values
    expected = [
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Tmedia', 5.7, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Tmin', 5.5, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'Tmax', 6.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'UR media', 83.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'UR min', 80.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'UR max', 85.0, True],
        [{'lat': 37.33913}, datetime(2012, 1, 1, 0, 0), 'PREC', 0.0, True]
    ]
    effective = arpa21.parse_row(row, parameters_map, only_valid=True)
    assert effective == expected


def test_validate_row_format():
    # right row
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    assert not arpa21.validate_row_format(row)

    # empty row no raises errors
    row = '\n'
    assert not arpa21.validate_row_format(row)

    # too less values
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1"
    assert arpa21.validate_row_format(row) == "The number of components in the row is wrong"

    # wrong date
    row = "201213010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    assert arpa21.validate_row_format(row) == "The date format in the row is wrong"

    # wrong values
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      A      2"
    assert arpa21.validate_row_format(row) == 'The row contains not numeric values'

    # check on spacing
    row = "201201010000 37.339130 32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    assert arpa21.validate_row_format(row) == 'The spacing in the row is wrong'

    row = " 201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    assert arpa21.validate_row_format(row) == 'The date length in the row is wrong'

    row = "201201010000 37.3391  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    assert arpa21.validate_row_format(row) == 'The latitude length in the row is wrong'


def test_validate_format(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    assert not arpa21.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = str(tmpdir.join('loc01_00201_201201010000_201301010100.xls'))
    err_msgs = arpa21.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'Extension expected must be .dat, found .xls')]

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00201_201201010000_201301010100.dat')
    err_msgs = arpa21.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        (2, "The spacing in the row is wrong"),
        (3, "the latitude changes"),
        (5, "it is not strictly after the previous"),
        (21, "duplication of rows with different data"),
        (22, "the time is not coherent with the filename"),
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    metadata = {'cod_utente': '00201', 'start_date': datetime(2012, 1, 1, 0, 0), 
                'end_date': datetime(2013, 1, 1, 1, 0), 'lat': 37.33913,
                'source': 'arpa21/loc01_00201_201201010000_201301010100.dat'}
    expected_data = [
        [metadata, datetime(2012, 1, 1, 0, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'DD', 242.0, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Tmedia', 5.7, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Tmin', 5.5, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Tmax', 6.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'UR media', 83.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'UR min', 80.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'UR max', 85.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 0, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'DD', 354.0, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Tmedia', 5.6, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Tmin', 5.2, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Tmax', 5.9, True],
        [metadata, datetime(2012, 1, 1, 1, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'UR media', 81.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'UR min', 79.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'UR max', 83.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 1, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'DD', 184.0, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Tmedia', 5.6, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Tmin', 5.3, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Tmax', 5.8, True],
        [metadata, datetime(2012, 1, 1, 2, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'UR media', 79.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'UR min', 79.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'UR max', 81.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 2, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'DD', 244.0, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Tmedia', 5.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Tmin', 4.6, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Tmax', 5.7, True],
        [metadata, datetime(2012, 1, 1, 3, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'UR media', 82.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'UR min', 79.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'UR max', 85.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 3, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'DD', 198.0, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Tmedia', 4.4, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Tmin', 3.9, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Tmax', 5.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'UR media', 84.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'UR min', 82.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'UR max', 87.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 4, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'DD', 198.0, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Tmedia', 4.6, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Tmin', 3.9, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Tmax', 4.9, True],
        [metadata, datetime(2012, 1, 1, 5, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'UR media', 84.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'UR min', 83.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'UR max', 88.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 5, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'FF', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'DD', 276.0, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Tmedia', 5.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Tmin', 4.4, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Tmax', 5.9, True],
        [metadata, datetime(2012, 1, 1, 6, 0), '6', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), '7', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), '8', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'UR media', 84.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'UR min', 82.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'UR max', 86.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), '12', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'P', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), '17', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), '18', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'INSOL_00', None, False],
        [metadata, datetime(2012, 1, 1, 6, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Bagnatura_f', None, False]
    ]
    effective = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == expected_data

    effective = arpa21.parse(filepath, parameters_filepath=parameters_filepath, only_valid=True)
    expected_data_valid = [
        [metadata, datetime(2012, 1, 1, 0, 0), 'Tmedia', 5.7, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Tmin', 5.5, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'Tmax', 6.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'UR media', 83.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'UR min', 80.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'UR max', 85.0, True],
        [metadata, datetime(2012, 1, 1, 0, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Tmedia', 5.6, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Tmin', 5.2, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'Tmax', 5.9, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'UR media', 81.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'UR min', 79.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'UR max', 83.0, True],
        [metadata, datetime(2012, 1, 1, 1, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Tmedia', 5.6, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Tmin', 5.3, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'Tmax', 5.8, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'UR media', 79.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'UR min', 79.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'UR max', 81.0, True],
        [metadata, datetime(2012, 1, 1, 2, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Tmedia', 5.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Tmin', 4.6, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'Tmax', 5.7, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'UR media', 82.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'UR min', 79.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'UR max', 85.0, True],
        [metadata, datetime(2012, 1, 1, 3, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Tmedia', 4.4, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Tmin', 3.9, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'Tmax', 5.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'UR media', 84.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'UR min', 82.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'UR max', 87.0, True],
        [metadata, datetime(2012, 1, 1, 4, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Tmedia', 4.6, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Tmin', 3.9, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'Tmax', 4.9, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'UR media', 84.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'UR min', 83.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'UR max', 88.0, True],
        [metadata, datetime(2012, 1, 1, 5, 0), 'PREC', 0.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Tmedia', 5.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Tmin', 4.4, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'Tmax', 5.9, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'UR media', 84.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'UR min', 82.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'UR max', 86.0, True],
        [metadata, datetime(2012, 1, 1, 6, 0), 'PREC', 0.0, True]
    ]

    assert effective == expected_data_valid
