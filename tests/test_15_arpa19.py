
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
    parameters_map = arpa19.load_parameter_file()

    # full parsing
    expected = {datetime(2013, 1, 1, 0, 0): {
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
        'lat': 43.876999}
    }
    effective = arpa19.parse_row(row, parameters_map)
    assert effective == expected

    # only valid values
    expected = {datetime(2013, 1, 1, 0, 0): {
        '1': (9.0, True),
        '2': (355.0, True),
        '3': (68.0, True),
        '9': (83.0, True),
        '12': (10205.0, True),
        'lat': 43.876999}
    }
    effective = arpa19.parse_row(row, parameters_map, only_valid=True)
    assert effective == expected


def test_validate_row():
    # right row
    row = "201301010000 43.876999      9    355     68  32767  32767  32767  32767  32767" \
          "     83  32767  32767  10205  32767  32767  32767  32767  32767  32767" \
          "  32767      1      1      1      2      2      2      2      2      1" \
          "      2      2      1      2      2      2      2      2      2      2"
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

