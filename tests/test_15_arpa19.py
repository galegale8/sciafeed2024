
from datetime import datetime
from os.path import join

import pytest

from sciafeed import arpa19
from . import TEST_DATA_PATH


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


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'arpa19_params.csv')
    parameter_map = arpa19.load_parameter_file(test_filepath)
    for i in range(1, 20):
        assert i in parameter_map
        assert 'par_code' in parameter_map[i]
        assert 'description' in parameter_map[i]


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


def test_validate_row():
    # right row
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert not arpa19.validate_row(row)

    # empty row no raises errors
    row = '\n'
    assert not arpa19.validate_row(row)

    # too values
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2    123"
    assert arpa19.validate_row(row) == "The number of components in the row %r is wrong" % row

    # wrong date
    row = "2001010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row(row) == "The date format in the row %r is wrong" % row

    # wrong values
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767     A1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert arpa19.validate_row(row) == 'The row %r contains not numeric values' % row

    # soft/hard check on spaces
    row = "201301010000 43.876999   9 355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767 10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1   1      2      2      2      2      2      1" \
          "      2      2      1  2      2      2      2      2      2      2"
    assert not arpa19.validate_row(row)
    assert arpa19.validate_row(row, strict=True) == 'The spacing in the row %r is wrong' % row

    row = " 201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert not arpa19.validate_row(row)
    assert arpa19.validate_row(row, strict=True) == 'The date length in the row %r is wrong' % row

    row = "201301010000  43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
    assert not arpa19.validate_row(row)
    assert arpa19.validate_row(row, strict=True) == \
        'The latitude length in the row %r is wrong' % row


def test_validate_arpa19(tmpdir):
    # right file
    filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')
    assert not arpa19.validate_arpa19(filepath)

    # wrong file name
    filepath = str(tmpdir.mkdir("sub").join('loc01_70001_201301010000_201401010100.xls'))
    err_msg = arpa19.validate_arpa19(filepath)
    assert err_msg and err_msg == 'Extension expected must be .dat, found .xls'

    # strict validation on wrong spacing
    filepath = join(TEST_DATA_PATH, 'wrong_70001_201301010000_201401010100.dat')
    assert not arpa19.validate_arpa19(filepath)
    err_msg = arpa19.validate_arpa19(filepath, strict=True)
    assert err_msg and err_msg.startswith("Row 2: The spacing in the row ")

    # file name time
    filepath = join(TEST_DATA_PATH, 'wrong_80001_201301010000_201401010100.dat')
    err_msg = arpa19.validate_arpa19(filepath)
    assert err_msg and err_msg == 'Row 20: the times are not coherent with the filename'

    # latitude changes
    filepath = join(TEST_DATA_PATH, 'wrong_90001_201301010000_201401010100.dat')
    err_msg = arpa19.validate_arpa19(filepath)
    assert err_msg and err_msg == 'Row 3: the latitude changes'

    # time sorting
    filepath = join(TEST_DATA_PATH, 'wrong_10001_201301010000_201401010100.dat')
    err_msg = arpa19.validate_arpa19(filepath)
    assert err_msg and err_msg == 'Row 4: it is not strictly after the previous'


def test_parse_arpa19():
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
    effective = arpa19.parse_arpa19(filepath,parameters_filepath=parameters_filepath)
    assert effective == (station, latitude, expected_data)

    effective = arpa19.parse_arpa19(filepath, parameters_filepath=parameters_filepath,
                                    only_valid=True)
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
