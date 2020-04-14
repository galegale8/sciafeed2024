
from datetime import datetime
from os.path import join

from sciafeed import arpa19
from . import TEST_DATA_PATH


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
    parameter_map = arpa19.load_parameter_file(test_filepath)
    for i in range(1, 20):
        assert i in parameter_map
        assert 'par_code' in parameter_map[i]
        assert 'description' in parameter_map[i]


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
    expected_thresholds = {
        'Bagnatura_f': [0.0, 60.0],
        'DD': [0.0, 360.0],
        'FF': [0.0, 102.0],
        'INSOL': [0.0, 60.0],
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
    parameter_thresholds = arpa19.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_parse_filename():
    # simple case
    filename = 'loc01_00003_200101010100_200401010100.dat'
    expected = ('00003', datetime(2001, 1, 1, 1, 0, 0), datetime(2004, 1, 1, 1, 0, 0))
    effective = arpa19.parse_filename(filename)
    assert effective == expected

    # more complex
    filename = 'loc01_22A_197505151123_201905140159.dat'
    expected = ('0022A', datetime(1975, 5, 15, 11, 23, 0), datetime(2019, 5, 14, 1, 59, 0))
    effective = arpa19.parse_filename(filename)
    assert effective == expected


def test_validate_filename():
    # no error for valid filenames
    right_filenames = [
        'loc01_00003_200101010100_200401010100.dat',
        'loc01_22A_197505151123_201905140159.dat'
    ]
    for right_filename in right_filenames:
        err_msg = arpa19.validate_filename(right_filename)
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
        err_msg = arpa19.validate_filename(wrong_filename)
        assert err_msg
        assert exp_error == err_msg


def test_extract_metadata():
    # right file
    filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
    metadata = arpa19.extract_metadata(filepath, parameters_filepath)
    assert metadata == {
        'start_date': datetime(2013, 1, 1, 0, 0),
        'end_date': datetime(2014, 1, 1, 1, 0),
        'source': 'arpa19/loc01_70001_201301010000_201401010100.dat',
        'cod_utente': '70001',
        'format': 'ARPA-19',
    }


def test_parse_row():
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
    parameters_map = arpa19.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = [
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'FF', 0.9, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'DD', 355.0, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Tmedia', 6.8, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Tmin', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Tmax', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), '6', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), '7', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), '8', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'UR media', 83.0, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'UR min', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'UR max', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), '12', 1020.5, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'P', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Pmin', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Pmax', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'RADSOL', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'INSOL', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'PREC', None, False],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Bagnatura_f', None, False]
    ]
    effective = arpa19.parse_row(row, parameters_map)
    assert effective == expected

    # only valid values
    expected = [
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'FF', 0.9, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'DD', 355.0, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'Tmedia', 6.8, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), 'UR media', 83.0, True],
        [{'lat': 43.876999}, datetime(2012, 12, 31, 23, 0), '12', 1020.5, True]
    ]
    effective = arpa19.parse_row(row, parameters_map, only_valid=True)
    assert effective == expected


def test_validate_row_format():
    # right row
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert not arpa19.validate_row_format(row)

    # empty row no raises errors
    row = '\n'
    assert not arpa19.validate_row_format(row)

    # too values
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2    123"
    assert arpa19.validate_row_format(row) == "The number of components in the row is wrong"

    # wrong date
    row = "2001010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == "The date format in the row is wrong"

    # wrong values
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767     A1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The row contains not numeric values'

    # check on spacing
    row = "201301010000 43.876999   9 355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767 10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1   1      2      2      2      2      2      1" \
          "      2      2      1  2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The spacing in the row is wrong'

    row = " 201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The date length in the row is wrong'

    row = "201301010000  43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The latitude length in the row is wrong'


def test_validate_format(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
    assert not arpa19.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = str(tmpdir.join('loc01_70001_201301010000_201401010100.xls'))
    err_msgs = arpa19.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'Extension expected must be .dat, found .xls')]

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'arpa19', 'wrong_70001_201301010000_201401010100.dat')
    err_msgs = arpa19.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        (2, "The spacing in the row is wrong"),
        (3, "the latitude changes"),
        (5, "it is not strictly after the previous"),
        (21, "duplication of rows with different data"),
        (22, "the time is not coherent with the filename"),
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
    metadata = {'cod_utente': '70001', 'lat': 43.876999, 'format': 'ARPA-19',
                'start_date': datetime(2013, 1, 1, 0, 0), 'end_date': datetime(2014, 1, 1, 1, 0),
                'source': 'arpa19/loc01_70001_201301010000_201401010100.dat'}
    expected_data = [
        [metadata, datetime(2012, 12, 31, 23, 0), 'FF', 0.9, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'DD', 355.0, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Tmedia', 6.8, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Tmin', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Tmax', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), '6', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), '7', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), '8', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'UR media', 83.0, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'UR min', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'UR max', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), '12', 1020.5, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'P', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Pmin', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Pmax', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'RADSOL', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'INSOL', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'PREC', None, False],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'FF', 0.6, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'DD', 310.0, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Tmedia', 6.5, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Tmin', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Tmax', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), '6', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), '7', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), '8', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'UR media', 86.0, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'UR min', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'UR max', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), '12', 1019.8, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'P', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Pmin', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Pmax', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'RADSOL', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'INSOL', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'PREC', None, False],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'FF', 0.3, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'DD', 288.0, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Tmedia', 6.3, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Tmin', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Tmax', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), '6', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), '7', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), '8', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'UR media', 86.0, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'UR min', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'UR max', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), '12', 1019.6, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'P', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Pmin', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Pmax', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'RADSOL', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'INSOL', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'PREC', None, False],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'FF', 1.1, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'DD', 357.0, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Tmedia', 6.3, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Tmin', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Tmax', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), '6', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), '7', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), '8', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'UR media', 87.0, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'UR min', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'UR max', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), '12', 1018.9, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'P', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Pmin', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Pmax', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'RADSOL', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'INSOL', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'PREC', None, False],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'FF', 0.9, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'DD', 1.0, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Tmedia', 6.4, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Tmin', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Tmax', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), '6', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), '7', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), '8', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'UR media', 88.0, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'UR min', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'UR max', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), '12', 1018.4, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'P', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Pmin', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Pmax', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'RADSOL', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'INSOL', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'PREC', None, False],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'FF', 3.0, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'DD', 6.0, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Tmedia', 6.7, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Tmin', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Tmax', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), '6', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), '7', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), '8', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'UR media', 89.0, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'UR min', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'UR max', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), '12', 1018.1, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'P', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Pmin', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Pmax', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'RADSOL', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'INSOL', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'PREC', None, False],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Bagnatura_f', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'FF', 3.1, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'DD', 6.0, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Tmedia', 6.5, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Tmin', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Tmax', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), '6', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), '7', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), '8', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'UR media', 93.0, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'UR min', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'UR max', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), '12', 1018.1, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'P', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Pmin', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Pmax', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'RADSOL', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'INSOL', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'PREC', None, False],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Bagnatura_f', None, False]
    ]
    effective_data = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
    for i, record in enumerate(effective_data):
        assert record[1:] == expected_data[i][1:]
        expected_md = expected_data[i][0]
        expected_md['row'] = i // 19 + 1
        assert record[0] == expected_md

    effective_data = arpa19.parse(filepath, parameters_filepath=parameters_filepath,
                                  only_valid=True)
    expected_data = [
        [metadata, datetime(2012, 12, 31, 23, 0), 'FF', 0.9, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'DD', 355.0, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'Tmedia', 6.8, True],
        [metadata, datetime(2012, 12, 31, 23, 0), 'UR media', 83.0, True],
        [metadata, datetime(2012, 12, 31, 23, 0), '12', 1020.5, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'FF', 0.6, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'DD', 310.0, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'Tmedia', 6.5, True],
        [metadata, datetime(2013, 1, 1, 0, 0), 'UR media', 86.0, True],
        [metadata, datetime(2013, 1, 1, 0, 0), '12', 1019.8, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'FF', 0.3, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'DD', 288.0, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'Tmedia', 6.3, True],
        [metadata, datetime(2013, 1, 1, 1, 0), 'UR media', 86.0, True],
        [metadata, datetime(2013, 1, 1, 1, 0), '12', 1019.6, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'FF', 1.1, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'DD', 357.0, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'Tmedia', 6.3, True],
        [metadata, datetime(2013, 1, 1, 2, 0), 'UR media', 87.0, True],
        [metadata, datetime(2013, 1, 1, 2, 0), '12', 1018.9, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'FF', 0.9, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'DD', 1.0, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'Tmedia', 6.4, True],
        [metadata, datetime(2013, 1, 1, 3, 0), 'UR media', 88.0, True],
        [metadata, datetime(2013, 1, 1, 3, 0), '12', 1018.4, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'FF', 3.0, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'DD', 6.0, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'Tmedia', 6.7, True],
        [metadata, datetime(2013, 1, 1, 4, 0), 'UR media', 89.0, True],
        [metadata, datetime(2013, 1, 1, 4, 0), '12', 1018.1, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'FF', 3.1, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'DD', 6.0, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'Tmedia', 6.5, True],
        [metadata, datetime(2013, 1, 1, 5, 0), 'UR media', 93.0, True],
        [metadata, datetime(2013, 1, 1, 5, 0), '12', 1018.1, True]
    ]
    for i, record in enumerate(effective_data):
        assert effective_data[i][1:] == expected_data[i][1:]
        expected_md = expected_data[i][0]
        expected_md['row'] = i // 5 + 1
        assert effective_data[i][0] == expected_md


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
    assert arpa19.is_format_compliant(filepath)

    filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    assert not arpa19.is_format_compliant(filepath)

    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    assert not arpa19.is_format_compliant(filepath)
