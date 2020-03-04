
from datetime import datetime
from os.path import join, exists

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
        '1': [0.0, 1020.0],
        '10': [20.0, 100.0],
        '11': [20.0, 100.0],
        '13': [9600.0, 10600.0],
        '16': [0.0, 100.0],
        '19': [0.0, 1080.0],
        '2': [0.0, 360.0],
        '20': [0.0, 9890.0],
        '21': [0.0, 60.0],
        '3': [-350.0, 450.0],
        '4': [-400.0, 400.0],
        '5': [-300.0, 500.0],
        '9': [20.0, 100.0]
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
    expected = (datetime(2012, 1, 1, 0, 0), 37.339130, {
        '1': (None, False),
        '2': (242.0, False),
        '3': (57.0, True),
        '4': (55.0, True),
        '5': (60.0, True),
        '6': (None, False),
        '7': (None, False),
        '8': (None, False),
        '9': (83.0, True),
        '10': (80.0, True),
        '11': (85.0, True),
        '12': (None, False),
        '13': (None, False),
        '14': (None, False),
        '15': (None, False),
        '16': (None, False),
        '17': (None, False),
        '18': (None, False),
        '19': (None, False),
        '20': (0, True),
        '21': (None, False),
    })
    effective = arpa21.parse_row(row, parameters_map)
    assert effective == expected

    # only valid values
    expected = (datetime(2012, 1, 1, 0, 0), 37.339130, {
        '3': (57.0, True),
        '4': (55.0, True),
        '5': (60.0, True),
        '9': (83.0, True),
        '10': (80.0, True),
        '11': (85.0, True),
        '20': (0, True),
    })
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
    station = '00201'
    latitude = 37.339130
    expected_data = {
        datetime(2012, 1, 1, 0, 0): {
            '1': (None, False),
            '10': (80.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (242.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (57.0, True),
            '4': (55.0, True),
            '5': (60.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (83.0, True)},
        datetime(2012, 1, 1, 1, 0): {
            '1': (None, False),
            '10': (79.0, True),
            '11': (83.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (354.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (56.0, True),
            '4': (52.0, True),
            '5': (59.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (81.0, True)},
        datetime(2012, 1, 1, 2, 0): {
            '1': (None, False),
            '10': (79.0, True),
            '11': (81.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (184.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (56.0, True),
            '4': (53.0, True),
            '5': (58.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (79.0, True)},
        datetime(2012, 1, 1, 3, 0): {
            '1': (None, False),
            '10': (79.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (244.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (50.0, True),
            '4': (46.0, True),
            '5': (57.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, True)},
        datetime(2012, 1, 1, 4, 0): {
            '1': (None, False),
            '10': (82.0, True),
            '11': (87.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (198.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (44.0, True),
            '4': (39.0, True),
            '5': (50.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (84.0, True)},
        datetime(2012, 1, 1, 5, 0): {
            '1': (None, False),
            '10': (83.0, True),
            '11': (88.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (198.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (46.0, True),
            '4': (39.0, True),
            '5': (49.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (84.0, True)},
        datetime(2012, 1, 1, 6, 0): {
            '1': (None, False),
            '10': (82.0, True),
            '11': (86.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (276.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (50.0, True),
            '4': (44.0, True),
            '5': (59.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (84.0, True)},
        datetime(2012, 1, 1, 7, 0): {
            '1': (None, False),
            '10': (83.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (133.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (59.0, True),
            '4': (58.0, True),
            '5': (62.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (83.0, True)},
        datetime(2012, 1, 1, 8, 0): {
            '1': (None, False),
            '10': (80.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (200.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (72.0, True),
            '4': (62.0, True),
            '5': (85.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, True)},
        datetime(2012, 1, 1, 9, 0): {
            '1': (None, False),
            '10': (73.0, True),
            '11': (86.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (160.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (93.0, True),
            '4': (83.0, True),
            '5': (111.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (80.0, True)},
        datetime(2012, 1, 1, 10, 0): {
            '1': (None, False),
            '10': (67.0, True),
            '11': (77.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (92.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (122.0, True),
            '4': (111.0, True),
            '5': (140.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (73.0, True)},
        datetime(2012, 1, 1, 11, 0): {
            '1': (None, False),
            '10': (68.0, True),
            '11': (78.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (143.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (135.0, True),
            '4': (134.0, True),
            '5': (140.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (74.0, True)},
        datetime(2012, 1, 1, 12, 0): {
            '1': (None, False),
            '10': (76.0, True),
            '11': (82.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (300.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (139.0, True),
            '4': (136.0, True),
            '5': (144.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (78.0, True)},
        datetime(2012, 1, 1, 13, 0): {
            '1': (None, False),
            '10': (80.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (260.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (140.0, True),
            '4': (137.0, True),
            '5': (145.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, True)},
        datetime(2012, 1, 1, 14, 0): {
            '1': (None, False),
            '10': (84.0, True),
            '11': (87.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (236.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (143.0, True),
            '4': (139.0, True),
            '5': (147.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (85.0, True)},
        datetime(2012, 1, 1, 15, 0): {
            '1': (None, False),
            '10': (85.0, True),
            '11': (90.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (235.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (141.0, True),
            '4': (134.0, True),
            '5': (146.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)},
        datetime(2012, 1, 1, 16, 0): {
            '1': (None, False),
            '10': (91.0, True),
            '11': (98.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (240.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (128.0, True),
            '4': (123.0, True),
            '5': (133.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (95.0, True)},
        datetime(2012, 1, 1, 17, 0): {
            '1': (None, False),
            '10': (97.0, True),
            '11': (100.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (246.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (119.0, True),
            '4': (112.0, True),
            '5': (123.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (98.0, True)},
        datetime(2012, 1, 1, 18, 0): {
            '1': (None, False),
            '10': (100.0, True),
            '11': (100.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (322.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (110.0, True),
            '4': (106.0, True),
            '5': (113.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, True)},
        datetime(2012, 1, 1, 19, 0): {
            '1': (None, False),
            '10': (100.0, True),
            '11': (100.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (65.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (99.0, True),
            '4': (95.0, True),
            '5': (106.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, True)}
    }
    effective = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == (station, latitude, expected_data)

    effective = arpa21.parse(filepath, parameters_filepath=parameters_filepath, only_valid=True)
    expected_data_valid = {
        datetime(2012, 1, 1, 0, 0): {
            '10': (80.0, True),
            '11': (85.0, True),
            '20': (0.0, True),
            '3': (57.0, True),
            '4': (55.0, True),
            '5': (60.0, True),
            '9': (83.0, True)},
        datetime(2012, 1, 1, 1, 0): {
            '10': (79.0, True),
            '11': (83.0, True),
            '20': (0.0, True),
            '3': (56.0, True),
            '4': (52.0, True),
            '5': (59.0, True),
            '9': (81.0, True)},
        datetime(2012, 1, 1, 2, 0): {
            '10': (79.0, True),
            '11': (81.0, True),
            '20': (0.0, True),
            '3': (56.0, True),
            '4': (53.0, True),
            '5': (58.0, True),
            '9': (79.0, True)},
        datetime(2012, 1, 1, 3, 0): {
            '10': (79.0, True),
            '11': (85.0, True),
            '20': (0.0, True),
            '3': (50.0, True),
            '4': (46.0, True),
            '5': (57.0, True),
            '9': (82.0, True)},
        datetime(2012, 1, 1, 4, 0): {
            '10': (82.0, True),
            '11': (87.0, True),
            '20': (0.0, True),
            '3': (44.0, True),
            '4': (39.0, True),
            '5': (50.0, True),
            '9': (84.0, True)},
        datetime(2012, 1, 1, 5, 0): {
            '10': (83.0, True),
            '11': (88.0, True),
            '20': (0.0, True),
            '3': (46.0, True),
            '4': (39.0, True),
            '5': (49.0, True),
            '9': (84.0, True)},
        datetime(2012, 1, 1, 6, 0): {
            '10': (82.0, True),
            '11': (86.0, True),
            '20': (0.0, True),
            '3': (50.0, True),
            '4': (44.0, True),
            '5': (59.0, True),
            '9': (84.0, True)},
        datetime(2012, 1, 1, 7, 0): {
            '10': (83.0, True),
            '11': (85.0, True),
            '20': (0.0, True),
            '3': (59.0, True),
            '4': (58.0, True),
            '5': (62.0, True),
            '9': (83.0, True)},
        datetime(2012, 1, 1, 8, 0): {
            '10': (80.0, True),
            '11': (85.0, True),
            '20': (0.0, True),
            '3': (72.0, True),
            '4': (62.0, True),
            '5': (85.0, True),
            '9': (82.0, True)},
        datetime(2012, 1, 1, 9, 0): {
            '10': (73.0, True),
            '11': (86.0, True),
            '20': (0.0, True),
            '3': (93.0, True),
            '4': (83.0, True),
            '5': (111.0, True),
            '9': (80.0, True)},
        datetime(2012, 1, 1, 10, 0): {
             '10': (67.0, True),
             '11': (77.0, True),
             '20': (0.0, True),
             '3': (122.0, True),
             '4': (111.0, True),
             '5': (140.0, True),
             '9': (73.0, True)},
        datetime(2012, 1, 1, 11, 0): {
             '10': (68.0, True),
             '11': (78.0, True),
             '20': (0.0, True),
             '3': (135.0, True),
             '4': (134.0, True),
             '5': (140.0, True),
             '9': (74.0, True)},
        datetime(2012, 1, 1, 12, 0): {
             '10': (76.0, True),
             '11': (82.0, True),
             '20': (0.0, True),
             '3': (139.0, True),
             '4': (136.0, True),
             '5': (144.0, True),
             '9': (78.0, True)},
        datetime(2012, 1, 1, 13, 0): {
             '10': (80.0, True),
             '11': (85.0, True),
             '20': (0.0, True),
             '3': (140.0, True),
             '4': (137.0, True),
             '5': (145.0, True),
             '9': (82.0, True)},
        datetime(2012, 1, 1, 14, 0): {
             '10': (84.0, True),
             '11': (87.0, True),
             '20': (0.0, True),
             '3': (143.0, True),
             '4': (139.0, True),
             '5': (147.0, True),
             '9': (85.0, True)},
        datetime(2012, 1, 1, 15, 0): {
             '10': (85.0, True),
             '11': (90.0, True),
             '20': (0.0, True),
             '3': (141.0, True),
             '4': (134.0, True),
             '5': (146.0, True),
             '9': (86.0, True)},
        datetime(2012, 1, 1, 16, 0): {
             '10': (91.0, True),
             '11': (98.0, True),
             '20': (0.0, True),
             '3': (128.0, True),
             '4': (123.0, True),
             '5': (133.0, True),
             '9': (95.0, True)},
        datetime(2012, 1, 1, 17, 0): {
             '10': (97.0, True),
             '11': (100.0, True),
             '20': (0.0, True),
             '3': (119.0, True),
             '4': (112.0, True),
             '5': (123.0, True),
             '9': (98.0, True)},
        datetime(2012, 1, 1, 18, 0): {
             '10': (100.0, True),
             '11': (100.0, True),
             '20': (0.0, True),
             '3': (110.0, True),
             '4': (106.0, True),
             '5': (113.0, True),
             '9': (100.0, True)},
        datetime(2012, 1, 1, 19, 0): {
             '10': (100.0, True),
             '11': (100.0, True),
             '20': (0.0, True),
             '3': (99.0, True),
             '4': (95.0, True),
             '5': (106.0, True),
             '9': (100.0, True)}}
    
    assert effective == (station, latitude, expected_data_valid)


def test_export(tmpdir):
    filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    data = arpa21.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        "station;latitude;date;parameter;value;valid\n",
        "00201;37.33913;2012-01-01T00:00:00;DD;242.0;0\n",
        "00201;37.33913;2012-01-01T00:00:00;Tmedia;57.0;1\n",
        "00201;37.33913;2012-01-01T00:00:00;Tmin;55.0;1\n",
        "00201;37.33913;2012-01-01T00:00:00;Tmax;60.0;1\n",
        "00201;37.33913;2012-01-01T00:00:00;UR media;83.0;1\n",
        "00201;37.33913;2012-01-01T00:00:00;UR min;80.0;1\n",
        "00201;37.33913;2012-01-01T00:00:00;UR max;85.0;1\n",
        "00201;37.33913;2012-01-01T00:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;DD;354.0;0\n",
        "00201;37.33913;2012-01-01T01:00:00;Tmedia;56.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;Tmin;52.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;Tmax;59.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;UR media;81.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;UR min;79.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;UR max;83.0;1\n",
        "00201;37.33913;2012-01-01T01:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;DD;184.0;0\n",
        "00201;37.33913;2012-01-01T02:00:00;Tmedia;56.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;Tmin;53.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;Tmax;58.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;UR media;79.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;UR min;79.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;UR max;81.0;1\n",
        "00201;37.33913;2012-01-01T02:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;DD;244.0;0\n",
        "00201;37.33913;2012-01-01T03:00:00;Tmedia;50.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;Tmin;46.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;Tmax;57.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;UR media;82.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;UR min;79.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;UR max;85.0;1\n",
        "00201;37.33913;2012-01-01T03:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;DD;198.0;0\n",
        "00201;37.33913;2012-01-01T04:00:00;Tmedia;44.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;Tmin;39.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;Tmax;50.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;UR media;84.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;UR min;82.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;UR max;87.0;1\n",
        "00201;37.33913;2012-01-01T04:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;DD;198.0;0\n",
        "00201;37.33913;2012-01-01T05:00:00;Tmedia;46.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;Tmin;39.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;Tmax;49.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;UR media;84.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;UR min;83.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;UR max;88.0;1\n",
        "00201;37.33913;2012-01-01T05:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;DD;276.0;0\n",
        "00201;37.33913;2012-01-01T06:00:00;Tmedia;50.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;Tmin;44.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;Tmax;59.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;UR media;84.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;UR min;82.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;UR max;86.0;1\n",
        "00201;37.33913;2012-01-01T06:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;DD;133.0;0\n",
        "00201;37.33913;2012-01-01T07:00:00;Tmedia;59.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;Tmin;58.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;Tmax;62.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;UR media;83.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;UR min;83.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;UR max;85.0;1\n",
        "00201;37.33913;2012-01-01T07:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;DD;200.0;0\n",
        "00201;37.33913;2012-01-01T08:00:00;Tmedia;72.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;Tmin;62.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;Tmax;85.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;UR media;82.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;UR min;80.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;UR max;85.0;1\n",
        "00201;37.33913;2012-01-01T08:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;DD;160.0;0\n",
        "00201;37.33913;2012-01-01T09:00:00;Tmedia;93.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;Tmin;83.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;Tmax;111.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;UR media;80.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;UR min;73.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;UR max;86.0;1\n",
        "00201;37.33913;2012-01-01T09:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;DD;92.0;0\n",
        "00201;37.33913;2012-01-01T10:00:00;Tmedia;122.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;Tmin;111.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;Tmax;140.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;UR media;73.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;UR min;67.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;UR max;77.0;1\n",
        "00201;37.33913;2012-01-01T10:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;DD;143.0;0\n",
        "00201;37.33913;2012-01-01T11:00:00;Tmedia;135.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;Tmin;134.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;Tmax;140.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;UR media;74.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;UR min;68.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;UR max;78.0;1\n",
        "00201;37.33913;2012-01-01T11:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;DD;300.0;0\n",
        "00201;37.33913;2012-01-01T12:00:00;Tmedia;139.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;Tmin;136.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;Tmax;144.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;UR media;78.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;UR min;76.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;UR max;82.0;1\n",
        "00201;37.33913;2012-01-01T12:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;DD;260.0;0\n",
        "00201;37.33913;2012-01-01T13:00:00;Tmedia;140.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;Tmin;137.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;Tmax;145.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;UR media;82.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;UR min;80.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;UR max;85.0;1\n",
        "00201;37.33913;2012-01-01T13:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;DD;236.0;0\n",
        "00201;37.33913;2012-01-01T14:00:00;Tmedia;143.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;Tmin;139.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;Tmax;147.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;UR media;85.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;UR min;84.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;UR max;87.0;1\n",
        "00201;37.33913;2012-01-01T14:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;DD;235.0;0\n",
        "00201;37.33913;2012-01-01T15:00:00;Tmedia;141.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;Tmin;134.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;Tmax;146.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;UR media;86.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;UR min;85.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;UR max;90.0;1\n",
        "00201;37.33913;2012-01-01T15:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;DD;240.0;0\n",
        "00201;37.33913;2012-01-01T16:00:00;Tmedia;128.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;Tmin;123.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;Tmax;133.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;UR media;95.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;UR min;91.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;UR max;98.0;1\n",
        "00201;37.33913;2012-01-01T16:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;DD;246.0;0\n",
        "00201;37.33913;2012-01-01T17:00:00;Tmedia;119.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;Tmin;112.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;Tmax;123.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;UR media;98.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;UR min;97.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;UR max;100.0;1\n",
        "00201;37.33913;2012-01-01T17:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;DD;322.0;0\n",
        "00201;37.33913;2012-01-01T18:00:00;Tmedia;110.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;Tmin;106.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;Tmax;113.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;UR media;100.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;UR min;100.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;UR max;100.0;1\n",
        "00201;37.33913;2012-01-01T18:00:00;PREC;0.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;DD;65.0;0\n",
        "00201;37.33913;2012-01-01T19:00:00;Tmedia;99.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;Tmin;95.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;Tmax;106.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;UR media;100.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;UR min;100.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;UR max;100.0;1\n",
        "00201;37.33913;2012-01-01T19:00:00;PREC;0.0;1\n",
    ]
    assert not exists(out_filepath)
    arpa21.export(data, out_filepath, omit_parameters=('6', '7', '8', '12', '17', '18'))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows


def test_row_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    parameters_map = arpa21.load_parameter_file(parameters_filepath)
    parameters_thresholds = arpa21.load_parameter_thresholds(parameters_filepath)

    # right row
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # two errors
    assert parameters_thresholds['3'] == [-350, 450]
    assert parameters_thresholds['9'] == [20, 100]
    row = "201201010000 37.339130  32767    242   -351     55     60  32767  32767  32767" \
          "     12     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert err_msgs == ["The value of '3' is out of range [-350.0, 450.0]",
                        "The value of '9' is out of range [20.0, 100.0]"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['3'] == (-351.0, False)
    assert parsed_row_updated[2]['9'] == (12.0, False)
    parsed_row_updated[2]['3'] = (-351.0, True)
    parsed_row_updated[2]['9'] = (12.0, True)
    assert parsed_row_updated == row_parsed

    # no check if no parameters_thresholds
    err_msgs, parsed_row_updated = arpa21.row_weak_climatologic_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is already invalid
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      2      1      1      2      2" \
          "      2      2      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if thresholds are not defined
    assert '12' not in parameters_thresholds
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  99999  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      2      1      1      2      2" \
          "      2      2      1      1      1      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_row_internal_consistence_check():
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    parameters_map = arpa21.load_parameter_file(parameters_filepath)
    limiting_params = {'3': ('4', '5')}

    # right row
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # wrong value
    row = "201201010000 37.339130  32767    242     61     62     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert err_msgs == ["The values of '3' and '4' are not consistent",
                        "The values of '3' and '5' are not consistent"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['3'] == (61.0, False)
    parsed_row_updated[2]['3'] = (61.0, True)
    assert parsed_row_updated == row_parsed

    # no check if no limiting parameters
    err_msgs, parsed_row_updated = arpa21.row_internal_consistence_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is invalid
    row = "201201010000 37.339130  32767    242     61     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      2      1      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if one of the thresholds is invalid
    row = "201201010000 37.339130  32767    242     57     55     60  32767  32767  32767" \
          "     83     80     85  32767  32767  32767  32767  32767  32767  32767" \
          "  32767      0  32767      2      2      1      2      1      2      2" \
          "      2      1      1      1      2      2      2      2      2      2" \
          "      2      2      1      2"
    row_parsed = arpa21.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = arpa21.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed


def test_do_weak_climatologic_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    parsed = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = arpa21.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with specific errors
    filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00202_201201010000_201301010100.dat')
    parsed = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = arpa21.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [
        (1, "The value of '3' is out of range [-350.0, 450.0]"),
        (2, "The value of '4' is out of range [-400.0, 400.0]"),
        (3, "The value of '5' is out of range [-300.0, 500.0]")
    ]
    assert parsed_after_check[:2] == parsed[:2]
    assert parsed_after_check[2][datetime(2012, 1, 1, 0, 0)]['3'] == (-570.0, False)
    assert parsed_after_check[2][datetime(2012, 1, 1, 1, 0)]['4'] == (520.0, False)
    assert parsed_after_check[2][datetime(2012, 1, 1, 2, 0)]['5'] == (580.0, False)

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00201_201201010000_201301010100.dat')
    err_msgs, _ = arpa21.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, parsed_after_check = arpa21.do_weak_climatologic_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
    assert not parsed_after_check


def test_do_internal_consistence_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    parsed = arpa21.parse(filepath, parameters_filepath=parameters_filepath)

    # right file
    limiting_params = {'3': ('4', '5')}
    err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with errors
    limiting_params = {'9': ('10', '4')}
    err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (1, "The values of '9' and '4' are not consistent"),
        (2, "The values of '9' and '4' are not consistent"),
        (3, "The values of '9' and '4' are not consistent"),
        (4, "The values of '9' and '4' are not consistent"),
        (5, "The values of '9' and '4' are not consistent"),
        (6, "The values of '9' and '4' are not consistent"),
        (7, "The values of '9' and '4' are not consistent"),
        (8, "The values of '9' and '4' are not consistent"),
        (9, "The values of '9' and '4' are not consistent"),
        (20, "The values of '9' and '4' are not consistent")
    ]
    assert parsed_after_check[:2] == parsed[:2]
    assert parsed_after_check[2][datetime(2012, 1, 1, 0, 0)]['9'] == (83.0, False)
    assert parsed_after_check[2][datetime(2012, 1, 1, 1, 0)]['9'] == (81.0, False)
    assert parsed_after_check[2][datetime(2012, 1, 1, 2, 0)]['9'] == (79.0, False)

    # no limiting parameters: no check
    err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00201_201201010000_201301010100.dat')
    err_msgs, _ = arpa21.do_internal_consistence_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
    assert not parsed_after_check


def test_parse_and_check(tmpdir):
    filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00202_201201010000_201301010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
    limiting_params = {'9': ('10', '4')}
    err_msgs, data_parsed = arpa21.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (1, "The value of '3' is out of range [-350.0, 450.0]"),
        (1, "The values of '9' and '4' are not consistent"),
        (2, "The value of '4' is out of range [-400.0, 400.0]"),
        (3, "The value of '5' is out of range [-300.0, 500.0]"),
        (3, "The values of '9' and '4' are not consistent"),
        (4, "The values of '9' and '4' are not consistent"),
        (5, "The values of '9' and '4' are not consistent"),
        (6, "The values of '9' and '4' are not consistent"),
        (7, "The values of '9' and '4' are not consistent"),
        (8, "The values of '9' and '4' are not consistent"),
        (9, "The values of '9' and '4' are not consistent"),
        (20, "The values of '9' and '4' are not consistent")
    ]
    assert data_parsed == ('00202', 37.33913, {
        datetime(2012, 1, 1, 0, 0): {
            '1': (None, False),
            '10': (80.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (242.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (-570.0, False),
            '4': (55.0, True),
            '5': (60.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (83.0, False)},
        datetime(2012, 1, 1, 1, 0): {
            '1': (None, False),
            '10': (79.0, True),
            '11': (83.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (354.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (56.0, True),
            '4': (520.0, False),
            '5': (59.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (81.0, True)},
        datetime(2012, 1, 1, 2, 0): {
            '1': (None, False),
            '10': (79.0, True),
            '11': (81.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (184.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (56.0, True),
            '4': (53.0, True),
            '5': (580.0, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (79.0, False)},
        datetime(2012, 1, 1, 3, 0): {
            '1': (None, False),
            '10': (79.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (244.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (50.0, True),
            '4': (46.0, True),
            '5': (57.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, False)},
        datetime(2012, 1, 1, 4, 0): {
            '1': (None, False),
            '10': (82.0, True),
            '11': (87.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (198.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (44.0, True),
            '4': (39.0, True),
            '5': (50.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (84.0, False)},
        datetime(2012, 1, 1, 5, 0): {
            '1': (None, False),
            '10': (83.0, True),
            '11': (88.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (198.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (46.0, True),
            '4': (39.0, True),
            '5': (49.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (84.0, False)},
        datetime(2012, 1, 1, 6, 0): {
            '1': (None, False),
            '10': (82.0, True),
            '11': (86.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (276.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (50.0, True),
            '4': (44.0, True),
            '5': (59.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (84.0, False)},
        datetime(2012, 1, 1, 7, 0): {
            '1': (None, False),
            '10': (83.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (133.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (59.0, True),
            '4': (58.0, True),
            '5': (62.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (83.0, False)},
        datetime(2012, 1, 1, 8, 0): {
            '1': (None, False),
            '10': (80.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (200.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (72.0, True),
            '4': (62.0, True),
            '5': (85.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, False)},
        datetime(2012, 1, 1, 9, 0): {
            '1': (None, False),
            '10': (73.0, True),
            '11': (86.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (160.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (93.0, True),
            '4': (83.0, True),
            '5': (111.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (80.0, True)},
        datetime(2012, 1, 1, 10, 0): {
            '1': (None, False),
            '10': (67.0, True),
            '11': (77.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (92.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (122.0, True),
            '4': (111.0, True),
            '5': (140.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (73.0, True)},
        datetime(2012, 1, 1, 11, 0): {
            '1': (None, False),
            '10': (68.0, True),
            '11': (78.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (143.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (135.0, True),
            '4': (134.0, True),
            '5': (140.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (74.0, True)},
        datetime(2012, 1, 1, 12, 0): {
            '1': (None, False),
            '10': (76.0, True),
            '11': (82.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (300.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (139.0, True),
            '4': (136.0, True),
            '5': (144.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (78.0, True)},
        datetime(2012, 1, 1, 13, 0): {
            '1': (None, False),
            '10': (80.0, True),
            '11': (85.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (260.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (140.0, True),
            '4': (137.0, True),
            '5': (145.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (82.0, True)},
        datetime(2012, 1, 1, 14, 0): {
            '1': (None, False),
            '10': (84.0, True),
            '11': (87.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (236.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (143.0, True),
            '4': (139.0, True),
            '5': (147.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (85.0, True)},
        datetime(2012, 1, 1, 15, 0): {
            '1': (None, False),
            '10': (85.0, True),
            '11': (90.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (235.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (141.0, True),
            '4': (134.0, True),
            '5': (146.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)},
        datetime(2012, 1, 1, 16, 0): {
            '1': (None, False),
            '10': (91.0, True),
            '11': (98.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (240.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (128.0, True),
            '4': (123.0, True),
            '5': (133.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (95.0, True)},
        datetime(2012, 1, 1, 17, 0): {
            '1': (None, False),
            '10': (97.0, True),
            '11': (100.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (246.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (119.0, True),
            '4': (112.0, True),
            '5': (123.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (98.0, True)},
        datetime(2012, 1, 1, 18, 0): {
            '1': (None, False),
            '10': (100.0, True),
            '11': (100.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (322.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (110.0, True),
            '4': (106.0, True),
            '5': (113.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, True)},
        datetime(2012, 1, 1, 19, 0): {
            '1': (None, False),
            '10': (100.0, True),
            '11': (100.0, True),
            '12': (None, False),
            '13': (None, False),
            '14': (None, False),
            '15': (None, False),
            '16': (None, False),
            '17': (None, False),
            '18': (None, False),
            '19': (None, False),
            '2': (65.0, False),
            '20': (0.0, True),
            '21': (None, False),
            '3': (99.0, True),
            '4': (95.0, True),
            '5': (106.0, True),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, False)}})

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, _ = arpa21.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
