from datetime import date
from os.path import join

from sciafeed import noaa
from . import TEST_DATA_PATH


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    parameter_map = noaa.load_parameter_file(test_filepath)
    for key, value in parameter_map.items():
        assert 'NOAA_CODE' in value
        assert 'par_code' in value
        assert 'description' in value
        assert 'min' in value
        assert 'max' in value


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    expected_thresholds = {
        'FF': [0.0, 102.0],
        'P': [960.0, 1060.0],
        'PREC': [0.0, 989.0],
        'Tmax': [-30.0, 50.0],
        'Tmedia': [-35.0, 45.0],
        'Tmin': [-40.0, 40.0]
    }
    parameter_thresholds = noaa.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_extract_metadata():
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    metadata = noaa.extract_metadata(filepath, parameters_filepath)
    assert metadata == {'source': 'noaa/160080-99999-2019.op', 'format': 'NOAA'}


def test_parse_row():
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    parameters_map = noaa.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    metadata = {'cod_utente': '160080', 'wban': '99999'}
    expected = [
        [metadata, date(2019, 1, 1), 'Tmedia', 1.0556, True],
        [metadata, date(2019, 1, 1), 'DEWP', -4.6111, True],
        [metadata, date(2019, 1, 1), 'P', None, True],
        [metadata, date(2019, 1, 1), 'STP', 859.6, True],
        [metadata, date(2019, 1, 1), 'VISIB', 17702.74, True],
        [metadata, date(2019, 1, 1), 'FF', 4.0638, True],
        [metadata, date(2019, 1, 1), 'MXSPD', 5.7098, True],
        [metadata, date(2019, 1, 1), 'GUST', None, True],
        [metadata, date(2019, 1, 1), 'Tmax', 5.0, True],
        [metadata, date(2019, 1, 1), 'Tmin', -2.0, True],
        [metadata, date(2019, 1, 1), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 1), 'SNDP', 40.64, True],
        [metadata, date(2019, 1, 1), 'UR media', 69.445, True]
    ]
    effective = noaa.parse_row(row, parameters_map)
    assert effective == expected


def test_validate_row_format():
    # right row
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    assert not noaa.validate_row_format(row)

    # empty row no raises errors
    row = '\n'
    assert not noaa.validate_row_format(row)

    # too less values
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6"
    assert noaa.validate_row_format(row) == "the length of the row is not standard"

    # wrong date
    row = "160080 99999  20190231    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    assert noaa.validate_row_format(row) == "the reference time for the row is not parsable"

    # wrong values
    row = "160080 99999  20190101    3A.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    assert noaa.validate_row_format(row) == 'The row contains not numeric values'


def test_validate_format():
    # right file
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    assert not noaa.validate_format(filepath, parameters_filepath=parameters_filepath)

    # wrong file name
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.csv')
    err_msgs = noaa.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, 'file extension must be .op')]

    # missing right header
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong1_160080-99999-2019.op')
    err_msgs = noaa.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs and err_msgs == [(0, "file doesn't include a correct header")]

    # compilation of errors on rows
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong2_160080-99999-2019.op')
    err_msgs = noaa.validate_format(filepath, parameters_filepath=parameters_filepath)
    assert err_msgs == [
        (2, 'the length of the row is not standard'),
        (3, 'the reference time for the row is not parsable'),
        (4, 'the precipitation flag is not parsable'),
        (5, 'The number of components in the row is wrong'),
        (6, 'The row contains not numeric values'),
        (10, 'duplication of rows with different data'),
        (12, 'it is not strictly after the previous')
    ]


def test_parse():
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    metadata = {'cod_utente': '160080', 'wban': '99999', 'source': 'noaa/160080-99999-2019.op',
                'format': 'NOAA'}
    expected_data = [
        [metadata, date(2019, 1, 1), 'Tmedia', 1.0556, True],
        [metadata, date(2019, 1, 1), 'DEWP', -4.6111, True],
        [metadata, date(2019, 1, 1), 'P', None, True],
        [metadata, date(2019, 1, 1), 'STP', 859.6, True],
        [metadata, date(2019, 1, 1), 'VISIB', 17702.74, True],
        [metadata, date(2019, 1, 1), 'FF', 4.0638, True],
        [metadata, date(2019, 1, 1), 'MXSPD', 5.7098, True],
        [metadata, date(2019, 1, 1), 'GUST', None, True],
        [metadata, date(2019, 1, 1), 'Tmax', 5.0, True],
        [metadata, date(2019, 1, 1), 'Tmin', -2.0, True],
        [metadata, date(2019, 1, 1), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 1), 'SNDP', 40.64, True],
        [metadata, date(2019, 1, 1), 'UR media', 69.445, True],
        [metadata, date(2019, 1, 2), 'Tmedia', -3.6111, True],
        [metadata, date(2019, 1, 2), 'DEWP', -9.1667, True],
        [metadata, date(2019, 1, 2), 'P', None, True],
        [metadata, date(2019, 1, 2), 'STP', 855.1, True],
        [metadata, date(2019, 1, 2), 'VISIB', 7724.832, True],
        [metadata, date(2019, 1, 2), 'FF', 4.0638, True],
        [metadata, date(2019, 1, 2), 'MXSPD', 5.7098, True],
        [metadata, date(2019, 1, 2), 'GUST', None, True],
        [metadata, date(2019, 1, 2), 'Tmax', 0.2222, True],
        [metadata, date(2019, 1, 2), 'Tmin', -7.0, True],
        [metadata, date(2019, 1, 2), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 2), 'SNDP', 40.64, True],
        [metadata, date(2019, 1, 2), 'UR media', 71.111, True],
        [metadata, date(2019, 1, 3), 'Tmedia', -7.0556, True],
        [metadata, date(2019, 1, 3), 'DEWP', -15.3889, True],
        [metadata, date(2019, 1, 3), 'P', None, True],
        [metadata, date(2019, 1, 3), 'STP', 860.1, True],
        [metadata, date(2019, 1, 3), 'VISIB', 11426.314, True],
        [metadata, date(2019, 1, 3), 'FF', 4.0638, True],
        [metadata, date(2019, 1, 3), 'MXSPD', 5.0926, True],
        [metadata, date(2019, 1, 3), 'GUST', None, True],
        [metadata, date(2019, 1, 3), 'Tmax', -3.2222, True],
        [metadata, date(2019, 1, 3), 'Tmin', -8.2222, True],
        [metadata, date(2019, 1, 3), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 3), 'SNDP', 40.64, True],
        [metadata, date(2019, 1, 3), 'UR media', 51.667, True],
        [metadata, date(2019, 1, 4), 'Tmedia', -5.0, True],
        [metadata, date(2019, 1, 4), 'DEWP', -11.7222, True],
        [metadata, date(2019, 1, 4), 'P', None, True],
        [metadata, date(2019, 1, 4), 'STP', 859.6, True],
        [metadata, date(2019, 1, 4), 'VISIB', 20277.684, True],
        [metadata, date(2019, 1, 4), 'FF', 5.967, True],
        [metadata, date(2019, 1, 4), 'MXSPD', 8.7962, True],
        [metadata, date(2019, 1, 4), 'GUST', None, True],
        [metadata, date(2019, 1, 4), 'Tmax', -1.3889, True],
        [metadata, date(2019, 1, 4), 'Tmin', -8.2222, True],
        [metadata, date(2019, 1, 4), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 4), 'SNDP', 40.64, True],
        [metadata, date(2019, 1, 4), 'UR media', 65.417, True],
        [metadata, date(2019, 1, 5), 'Tmedia', -3.0556, True],
        [metadata, date(2019, 1, 5), 'DEWP', -4.6111, True],
        [metadata, date(2019, 1, 5), 'P', None, True],
        [metadata, date(2019, 1, 5), 'STP', 857.4, True],
        [metadata, date(2019, 1, 5), 'VISIB', 7724.832, True],
        [metadata, date(2019, 1, 5), 'FF', 3.7037, True],
        [metadata, date(2019, 1, 5), 'MXSPD', 5.7098, True],
        [metadata, date(2019, 1, 5), 'GUST', None, True],
        [metadata, date(2019, 1, 5), 'Tmax', 4.0, True],
        [metadata, date(2019, 1, 5), 'Tmin', -5.6111, True],
        [metadata, date(2019, 1, 5), 'PREC', 2.032, True],
        [metadata, date(2019, 1, 5), 'SNDP', 88.9, True],
        [metadata, date(2019, 1, 5), 'UR media', 80.972, True],
        [metadata, date(2019, 1, 6), 'Tmedia', -1.5, True],
        [metadata, date(2019, 1, 6), 'DEWP', -4.8333, True],
        [metadata, date(2019, 1, 6), 'P', None, True],
        [metadata, date(2019, 1, 6), 'STP', 857.8, True],
        [metadata, date(2019, 1, 6), 'VISIB', 12713.786, True],
        [metadata, date(2019, 1, 6), 'FF', 3.858, True],
        [metadata, date(2019, 1, 6), 'MXSPD', 4.5782, True],
        [metadata, date(2019, 1, 6), 'GUST', None, True],
        [metadata, date(2019, 1, 6), 'Tmax', 1.2222, True],
        [metadata, date(2019, 1, 6), 'Tmin', -2.7778, True],
        [metadata, date(2019, 1, 6), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 6), 'SNDP', 119.38, True],
        [metadata, date(2019, 1, 6), 'UR media', 79.722, True],
        [metadata, date(2019, 1, 7), 'Tmedia', -2.3333, True],
        [metadata, date(2019, 1, 7), 'DEWP', -5.7222, True],
        [metadata, date(2019, 1, 7), 'P', None, True],
        [metadata, date(2019, 1, 7), 'STP', 858.9, True],
        [metadata, date(2019, 1, 7), 'VISIB', 11909.116, True],
        [metadata, date(2019, 1, 7), 'FF', 3.6522, True],
        [metadata, date(2019, 1, 7), 'MXSPD', 4.5782, True],
        [metadata, date(2019, 1, 7), 'GUST', None, True],
        [metadata, date(2019, 1, 7), 'Tmax', 0.2222, True],
        [metadata, date(2019, 1, 7), 'Tmin', -5.0, True],
        [metadata, date(2019, 1, 7), 'PREC', 0.0, True],
        [metadata, date(2019, 1, 7), 'SNDP', 119.38, True],
        [metadata, date(2019, 1, 7), 'UR media', 83.334, True],
        [metadata, date(2019, 1, 8), 'Tmedia', -2.1667, True],
        [metadata, date(2019, 1, 8), 'DEWP', -4.4444, True],
        [metadata, date(2019, 1, 8), 'P', None, True],
        [metadata, date(2019, 1, 8), 'STP', 848.0, True],
        [metadata, date(2019, 1, 8), 'VISIB', 5632.69, True],
        [metadata, date(2019, 1, 8), 'FF', 2.9321, True],
        [metadata, date(2019, 1, 8), 'MXSPD', 4.1152, True],
        [metadata, date(2019, 1, 8), 'GUST', None, True],
        [metadata, date(2019, 1, 8), 'Tmax', 1.7778, True],
        [metadata, date(2019, 1, 8), 'Tmin', -5.7778, True],
        [metadata, date(2019, 1, 8), 'PREC', 3.048, True],
        [metadata, date(2019, 1, 8), 'SNDP', 149.86, True],
        [metadata, date(2019, 1, 8), 'UR media', 87.778, True],
    ]
    effective = noaa.parse(filepath, parameters_filepath=parameters_filepath)
    for i, record in enumerate(effective):
        assert effective[i][1:] == expected_data[i][1:]
        expected_md = expected_data[i][0]
        expected_md['row'] = i // 13 + 2
        assert effective[i][0] == expected_md


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    assert noaa.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong1_160080-99999-2019.op')
    assert not noaa.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    assert not noaa.is_format_compliant(filepath)
