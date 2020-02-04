
from datetime import datetime
from os.path import join, exists

import pytest

from sciafeed import arpa19
from . import TEST_DATA_PATH


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    parameter_map = arpa19.load_parameter_file(test_filepath)
    for i in range(1, 20):
        assert i in parameter_map
        assert 'par_code' in parameter_map[i]
        assert 'description' in parameter_map[i]


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    expected_thresholds = {
        '1': [0.0, 1020.0],
        '10': [20.0, 100.0],
        '11': [20.0, 100.0],
        '13': [9600.0, 10600.0],
        '16': [0.0, 100.0],
        '17': [0.0, 60.0],
        '18': [0.0, 9890.0],
        '19': [0.0, 60.0],
        '2': [0.0, 360.0],
        '3': [-350.0, 450.0],
        '4': [-400.0, 400.0],
        '5': [-300.0, 500.0],
        '9': [20.0, 100.0]
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

    # with generic errors
    filenames = [
        'loc01_splitthis_22A_197505151123_201905140159.dat',  # too many '_'
        'loc01_00004_200101010100_200401010100.xls',  # wrong extension
        'loc01_00005_200401010100_200101010100.dat',  # wrong date interval
    ]
    for filename in filenames:
        with pytest.raises(AssertionError):
            arpa19.parse_filename(filename)

    # formatting errors on dates
    filenames = [
        'loc01_00006_200101010160_200401010100.dat',  # wrong start date
        'loc01_00007_200101010100_200402300100.dat',  # wrong end date
    ]
    for filename in filenames:
        with pytest.raises(ValueError):
            arpa19.parse_filename(filename)


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


def test_parse_row():
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    parameters_map = arpa19.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = (datetime(2013, 1, 1, 0, 0), 43.876999, {
        '1': (9.0, True),
        '2': (355.0, True),
        '3': (68.0, True),
        '4': (None, False),
        '5': (None, False),
        '6': (None, False),
        '7': (None, False),
        '8': (None, False),
        '9': (83.0, True),
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
    })
    effective = arpa19.parse_row(row, parameters_map)
    assert effective == expected

    # only valid values
    expected = (datetime(2013, 1, 1, 0, 0), 43.876999, {
        '1': (9.0, True),
        '2': (355.0, True),
        '3': (68.0, True),
        '9': (83.0, True),
        '12': (10205.0, True),
    })
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
    assert arpa19.validate_row_format(row) == "The number of components in the row %r is wrong" \
        % row

    # wrong date
    row = "2001010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == "The date format in the row %r is wrong" % row

    # wrong values
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767     A1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The row %r contains not numeric values' % row

    # check on spacing
    row = "201301010000 43.876999   9 355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767 10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1   1      2      2      2      2      2      1" \
          "      2      2      1  2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The spacing in the row %r is wrong' % row

    row = " 201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The date length in the row %r is wrong' % row

    row = "201301010000  43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row_format(row) == 'The latitude length in the row %r is wrong' % row


def test_validate_format(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    assert not arpa19.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = str(tmpdir.join('loc01_70001_201301010000_201401010100.xls'))
    err_msgs = arpa19.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == ['Extension expected must be .dat, found .xls']

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'wrong_70001_201301010000_201401010100.dat')
    err_msgs = arpa19.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        "Row 2: The spacing in the row '201301010100 43.876999    6    310     65  "
        '32767  32767  32767  32767  32767     86  32767  32767  10198  32767  32767  '
        '32767  32767  32767  32767  32767      1      1      1      2      2      '
        '2      2      2      1      2      2      1      2      2      2      2      '
        "2      2      2\\n' is wrong",
        'Row 3: the latitude changes',
        'Row 5: it is not strictly after the previous',
        'Row 21: duplication of rows with different data',
        'Row 22: the time is not coherent with the filename',
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    station = '70001'
    latitude = 43.876999
    expected_data = {
        datetime(2013, 1, 1, 0, 0): {
            '1': (9.0, True),
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
            '9': (83.0, True)
        },
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
            '2': (310.0, True),
            '3': (65.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)
        },
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
            '3': (63.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)
        },
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
            '9': (87.0, True)
        },
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
            '3': (64.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (88.0, True)
        },
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
            '3': (67.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (89.0, True)
        },
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
            '3': (65.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (93.0, True)
        },
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
            '9': (93.0, True)
        },
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
            '9': (95.0, True)
        },
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
            '3': (106.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (88.0, True)
        },
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
            '9': (72.0, True)
        },
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
            '9': (69.0, True)
        },
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
            '9': (73.0, True)
        },
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
            '9': (74.0, True)
        },
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
            '9': (78.0, True)
        },
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
            '9': (82.0, True)
        },
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
            '9': (96.0, True)
        },
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
            '9': (96.0, True)
        },
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
            '9': (97.0, True)
        },
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
            '3': (99.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, True)
        }
    }
    effective = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == (station, latitude, expected_data)

    effective = arpa19.parse(filepath, parameters_filepath=parameters_filepath, only_valid=True)
    expected_data_valid = {
        datetime(2013, 1, 1, 0, 0): {
            '1': (9.0, True),
            '12': (10205.0, True),
            '2': (355.0, True),
            '3': (68.0, True),
            '9': (83.0, True)
        },
        datetime(2013, 1, 1, 1, 0): {
            '1': (6.0, True),
            '12': (10198.0, True),
            '2': (310.0, True),
            '3': (65.0, True),
            '9': (86.0, True)
        },
        datetime(2013, 1, 1, 2, 0): {
            '1': (3.0, True),
            '12': (10196.0, True),
            '2': (288.0, True),
            '3': (63.0, True),
            '9': (86.0, True)
        },
        datetime(2013, 1, 1, 3, 0): {
            '1': (11.0, True),
            '12': (10189.0, True),
            '2': (357.0, True),
            '3': (63.0, True),
            '9': (87.0, True)
        },
        datetime(2013, 1, 1, 4, 0): {
            '1': (9.0, True),
            '12': (10184.0, True),
            '2': (1.0, True),
            '3': (64.0, True),
            '9': (88.0, True)
        },
        datetime(2013, 1, 1, 5, 0): {
            '1': (30.0, True),
            '12': (10181.0, True),
            '2': (6.0, True),
            '3': (67.0, True),
            '9': (89.0, True)
        },
        datetime(2013, 1, 1, 6, 0): {
            '1': (31.0, True),
            '12': (10181.0, True),
            '2': (6.0, True),
            '3': (65.0, True),
            '9': (93.0, True)
        },
        datetime(2013, 1, 1, 7, 0): {
            '1': (20.0, True),
            '12': (10182.0, True),
            '2': (358.0, True),
            '3': (65.0, True),
            '9': (93.0, True)
            },
        datetime(2013, 1, 1, 8, 0): {
            '1': (5.0, True),
            '12': (10182.0, True),
            '2': (342.0, True),
            '3': (66.0, True),
            '9': (95.0, True)
        },
        datetime(2013, 1, 1, 9, 0): {
            '1': (35.0, True),
            '12': (10179.0, True),
            '2': (12.0, True),
            '3': (106.0, True),
            '9': (88.0, True)
        },
        datetime(2013, 1, 1, 10, 0): {
            '1': (13.0, True),
            '12': (10182.0, True),
            '2': (154.0, True),
            '3': (121.0, True),
            '9': (72.0, True)
        },
        datetime(2013, 1, 1, 11, 0): {
            '1': (54.0, True),
            '12': (10177.0, True),
            '2': (218.0, True),
            '3': (123.0, True),
            '9': (69.0, True)
        },
        datetime(2013, 1, 1, 12, 0): {
            '1': (61.0, True),
            '12': (10167.0, True),
            '2': (225.0, True),
            '3': (125.0, True),
            '9': (73.0, True)
        },
        datetime(2013, 1, 1, 13, 0): {
            '1': (65.0, True),
            '12': (10162.0, True),
            '2': (226.0, True),
            '3': (122.0, True),
            '9': (74.0, True)
        },
        datetime(2013, 1, 1, 14, 0): {
            '1': (46.0, True),
            '12': (10161.0, True),
            '2': (221.0, True),
            '3': (117.0, True),
            '9': (78.0, True)
        },
        datetime(2013, 1, 1, 15, 0): {
            '1': (19.0, True),
            '12': (10161.0, True),
            '2': (233.0, True),
            '3': (110.0, True),
            '9': (82.0, True)
        },
        datetime(2013, 1, 1, 16, 0): {
            '1': (28.0, True),
            '12': (10158.0, True),
            '2': (355.0, True),
            '3': (100.0, True),
            '9': (96.0, True)
        },
        datetime(2013, 1, 1, 17, 0): {
            '1': (24.0, True),
            '12': (10156.0, True),
            '2': (345.0, True),
            '3': (99.0, True),
            '9': (96.0, True)
        },
        datetime(2013, 1, 1, 18, 0): {
            '1': (26.0, True),
            '12': (10155.0, True),
            '2': (357.0, True),
            '3': (101.0, True),
            '9': (97.0, True)
        },
        datetime(2013, 1, 1, 19, 0): {
            '1': (26.0, True),
            '12': (10154.0, True),
            '2': (2.0, True),
            '3': (99.0, True),
            '9': (100.0, True)
        }
    }
    assert effective == (station, latitude, expected_data_valid)


def test_write_data(tmpdir):
    filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')
    data = arpa19.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        'station;latitude;date;parameter;value;valid\n',
        '70001;43.876999;2013-01-01T00:00:00;FF;9.0;1\n',
        '70001;43.876999;2013-01-01T00:00:00;DD;355.0;1\n',
        '70001;43.876999;2013-01-01T00:00:00;Tmedia;68.0;1\n',
        '70001;43.876999;2013-01-01T00:00:00;UR media;83.0;1\n',
        '70001;43.876999;2013-01-01T01:00:00;FF;6.0;1\n',
        '70001;43.876999;2013-01-01T01:00:00;DD;310.0;1\n',
        '70001;43.876999;2013-01-01T01:00:00;Tmedia;65.0;1\n',
        '70001;43.876999;2013-01-01T01:00:00;UR media;86.0;1\n',
        '70001;43.876999;2013-01-01T02:00:00;FF;3.0;1\n',
        '70001;43.876999;2013-01-01T02:00:00;DD;288.0;1\n',
        '70001;43.876999;2013-01-01T02:00:00;Tmedia;63.0;1\n',
        '70001;43.876999;2013-01-01T02:00:00;UR media;86.0;1\n',
        '70001;43.876999;2013-01-01T03:00:00;FF;11.0;1\n',
        '70001;43.876999;2013-01-01T03:00:00;DD;357.0;1\n',
        '70001;43.876999;2013-01-01T03:00:00;Tmedia;63.0;1\n',
        '70001;43.876999;2013-01-01T03:00:00;UR media;87.0;1\n',
        '70001;43.876999;2013-01-01T04:00:00;FF;9.0;1\n',
        '70001;43.876999;2013-01-01T04:00:00;DD;1.0;1\n',
        '70001;43.876999;2013-01-01T04:00:00;Tmedia;64.0;1\n',
        '70001;43.876999;2013-01-01T04:00:00;UR media;88.0;1\n',
        '70001;43.876999;2013-01-01T05:00:00;FF;30.0;1\n',
        '70001;43.876999;2013-01-01T05:00:00;DD;6.0;1\n',
        '70001;43.876999;2013-01-01T05:00:00;Tmedia;67.0;1\n',
        '70001;43.876999;2013-01-01T05:00:00;UR media;89.0;1\n',
        '70001;43.876999;2013-01-01T06:00:00;FF;31.0;1\n',
        '70001;43.876999;2013-01-01T06:00:00;DD;6.0;1\n',
        '70001;43.876999;2013-01-01T06:00:00;Tmedia;65.0;1\n',
        '70001;43.876999;2013-01-01T06:00:00;UR media;93.0;1\n',
        '70001;43.876999;2013-01-01T07:00:00;FF;20.0;1\n',
        '70001;43.876999;2013-01-01T07:00:00;DD;358.0;1\n',
        '70001;43.876999;2013-01-01T07:00:00;Tmedia;65.0;1\n',
        '70001;43.876999;2013-01-01T07:00:00;UR media;93.0;1\n',
        '70001;43.876999;2013-01-01T08:00:00;FF;5.0;1\n',
        '70001;43.876999;2013-01-01T08:00:00;DD;342.0;1\n',
        '70001;43.876999;2013-01-01T08:00:00;Tmedia;66.0;1\n',
        '70001;43.876999;2013-01-01T08:00:00;UR media;95.0;1\n',
        '70001;43.876999;2013-01-01T09:00:00;FF;35.0;1\n',
        '70001;43.876999;2013-01-01T09:00:00;DD;12.0;1\n',
        '70001;43.876999;2013-01-01T09:00:00;Tmedia;106.0;1\n',
        '70001;43.876999;2013-01-01T09:00:00;UR media;88.0;1\n',
        '70001;43.876999;2013-01-01T10:00:00;FF;13.0;1\n',
        '70001;43.876999;2013-01-01T10:00:00;DD;154.0;1\n',
        '70001;43.876999;2013-01-01T10:00:00;Tmedia;121.0;1\n',
        '70001;43.876999;2013-01-01T10:00:00;UR media;72.0;1\n',
        '70001;43.876999;2013-01-01T11:00:00;FF;54.0;1\n',
        '70001;43.876999;2013-01-01T11:00:00;DD;218.0;1\n',
        '70001;43.876999;2013-01-01T11:00:00;Tmedia;123.0;1\n',
        '70001;43.876999;2013-01-01T11:00:00;UR media;69.0;1\n',
        '70001;43.876999;2013-01-01T12:00:00;FF;61.0;1\n',
        '70001;43.876999;2013-01-01T12:00:00;DD;225.0;1\n',
        '70001;43.876999;2013-01-01T12:00:00;Tmedia;125.0;1\n',
        '70001;43.876999;2013-01-01T12:00:00;UR media;73.0;1\n',
        '70001;43.876999;2013-01-01T13:00:00;FF;65.0;1\n',
        '70001;43.876999;2013-01-01T13:00:00;DD;226.0;1\n',
        '70001;43.876999;2013-01-01T13:00:00;Tmedia;122.0;1\n',
        '70001;43.876999;2013-01-01T13:00:00;UR media;74.0;1\n',
        '70001;43.876999;2013-01-01T14:00:00;FF;46.0;1\n',
        '70001;43.876999;2013-01-01T14:00:00;DD;221.0;1\n',
        '70001;43.876999;2013-01-01T14:00:00;Tmedia;117.0;1\n',
        '70001;43.876999;2013-01-01T14:00:00;UR media;78.0;1\n',
        '70001;43.876999;2013-01-01T15:00:00;FF;19.0;1\n',
        '70001;43.876999;2013-01-01T15:00:00;DD;233.0;1\n',
        '70001;43.876999;2013-01-01T15:00:00;Tmedia;110.0;1\n',
        '70001;43.876999;2013-01-01T15:00:00;UR media;82.0;1\n',
        '70001;43.876999;2013-01-01T16:00:00;FF;28.0;1\n',
        '70001;43.876999;2013-01-01T16:00:00;DD;355.0;1\n',
        '70001;43.876999;2013-01-01T16:00:00;Tmedia;100.0;1\n',
        '70001;43.876999;2013-01-01T16:00:00;UR media;96.0;1\n',
        '70001;43.876999;2013-01-01T17:00:00;FF;24.0;1\n',
        '70001;43.876999;2013-01-01T17:00:00;DD;345.0;1\n',
        '70001;43.876999;2013-01-01T17:00:00;Tmedia;99.0;1\n',
        '70001;43.876999;2013-01-01T17:00:00;UR media;96.0;1\n',
        '70001;43.876999;2013-01-01T18:00:00;FF;26.0;1\n',
        '70001;43.876999;2013-01-01T18:00:00;DD;357.0;1\n',
        '70001;43.876999;2013-01-01T18:00:00;Tmedia;101.0;1\n',
        '70001;43.876999;2013-01-01T18:00:00;UR media;97.0;1\n',
        '70001;43.876999;2013-01-01T19:00:00;FF;26.0;1\n',
        '70001;43.876999;2013-01-01T19:00:00;DD;2.0;1\n',
        '70001;43.876999;2013-01-01T19:00:00;Tmedia;99.0;1\n',
        '70001;43.876999;2013-01-01T19:00:00;UR media;100.0;1\n'
    ]
    assert not exists(out_filepath)
    arpa19.write_data(data, out_filepath, omit_parameters=('6', '7', '8', '12'))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows


def test_row_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    parameters_map = arpa19.load_parameter_file(parameters_filepath)
    parameters_thresholds = arpa19.load_parameter_thresholds(parameters_filepath)

    # right row
    row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
          "      1      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # two errors
    assert parameters_thresholds['1'] == [0, 1020]
    assert parameters_thresholds['9'] == [20, 100]
    row = "201301010700 43.876999   1021    358     65  32767  32767  32767  32767  32767" \
          "    101  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
          "      1      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert err_msgs == ["The value of '1' is out of range [0.0, 1020.0]",
                        "The value of '9' is out of range [20.0, 100.0]"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['1'] == (1021.0, False)
    parsed_row_updated[2]['1'] = (1021.0, True)
    parsed_row_updated[2]['9'] = (101.0, True)
    assert parsed_row_updated == row_parsed

    # no check if no parameters_thresholds
    err_msgs, parsed_row_updated = arpa19.row_weak_climatologic_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is already invalid
    row = "201301010700 43.876999   1021    358     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
          "      2      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if thresholds are not defined
    assert '12' not in parameters_thresholds
    row = "201301010700 43.876999   1021    358     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  99999  32767  32767  32767  32767  32767  32767  32767" \
          "      2      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_row_internal_consistence_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    parameters_map = arpa19.load_parameter_file(parameters_filepath)
    limiting_params = {'1': ('2', '3')}

    # right row
    row = "201301010600 43.876999     31      6     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  10181  32767  32767  32767  32767  32767  32767  32767" \
          "      1      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # wrong value
    row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
          "      1      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert err_msgs == ["The values of '1', '2' and '3' are not consistent"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['1'] == (20.0, False)
    parsed_row_updated[2]['1'] = (20.0, True)
    assert parsed_row_updated == row_parsed

    # no check if no limiting parameters
    err_msgs, parsed_row_updated = arpa19.row_internal_consistence_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is invalid
    row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
          "      2      1      1      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if one of the thresholds is invalid
    row = "201301010700 43.876999     20    358     65  32767  32767  32767  32767  32767" \
          "     93  32767  32767  10182  32767  32767  32767  32767  32767  32767  32767" \
          "      1      1      2      2      2      2      2      2      1      2      2" \
          "      1      2      2      2      2      2      2      2"
    row_parsed = arpa19.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa19.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_do_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')
    parsed = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = arpa19.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with errors
    filepath = join(TEST_DATA_PATH, 'wrong_70002_201301010000_201401010100.dat')
    parsed = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = arpa19.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [
        "Row 1: The value of '1' is out of range [0.0, 1020.0]",
        "Row 2: The value of '2' is out of range [0.0, 360.0]",
        "Row 3: The value of '3' is out of range [-350.0, 450.0]"
    ]
    assert parsed_after_check[:2] == parsed[:2]
    assert parsed_after_check[2][datetime(2013, 1, 1, 0, 0)]['1'] == (2000.0, False)
    assert parsed_after_check[2][datetime(2013, 1, 1, 1, 0)]['2'] == (361.0, False)
    assert parsed_after_check[2][datetime(2013, 1, 1, 2, 0)]['3'] == (-351.0, False)


def test_do_internal_consistence_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')
    parsed = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
    limiting_params = None

    # right file
    limiting_params = {'3': ('4', '5')}
    err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with errors
    limiting_params = {'3': ('1', '2')}
    err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        "Row 5: The values of '3', '1' and '2' are not consistent",
        "Row 6: The values of '3', '1' and '2' are not consistent",
        "Row 7: The values of '3', '1' and '2' are not consistent",
        "Row 10: The values of '3', '1' and '2' are not consistent",
        "Row 20: The values of '3', '1' and '2' are not consistent"
    ]
    assert parsed_after_check[:2] == parsed[:2]
    assert parsed_after_check[2][datetime(2013, 1, 1, 4, 0)]['3'] == (64.0, False)
    assert parsed_after_check[2][datetime(2013, 1, 1, 5, 0)]['3'] == (67.0, False)
    assert parsed_after_check[2][datetime(2013, 1, 1, 6, 0)]['3'] == (65.0, False)

    # no limiting parameters: no check
    err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed
