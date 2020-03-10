from datetime import datetime
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
        'FF': [0.0, 198.272],
        'P': [960.0, 1060.0],
        'PREC': [0.0, 38.937],
        'Tmax': [-22.0, 122.0],
        'Tmedia': [-31.0, 113.0],
        'Tmin': [-40.0, 104.0]
    }
    parameter_thresholds = noaa.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_extract_metadata():
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    md = noaa.extract_metadata(filepath, parameters_filepath)
    assert md == [dict(), dict()]


def test_parse_row():
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    parameters_map = noaa.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = [
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'Tmedia', 33.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'DEWP', 23.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'STP', 859.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'VISIB', 11.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'FF', 7.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'MXSPD', 11.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'Tmax', 41.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'Tmin', 28.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'SNDP', 1.6, True]
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
    expected_data = [
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'Tmedia', 33.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'DEWP', 23.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'STP', 859.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'VISIB', 11.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'FF', 7.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'MXSPD', 11.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'Tmax', 41.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'Tmin', 28.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), 'SNDP', 1.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'Tmedia', 25.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'DEWP', 15.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'STP', 855.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'VISIB', 4.8, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'FF', 7.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'MXSPD', 11.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'Tmax', 32.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'Tmin', 19.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), 'SNDP', 1.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'Tmedia', 19.3, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'DEWP', 4.3, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'STP', 860.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'VISIB', 7.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'FF', 7.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'MXSPD', 9.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'Tmax', 26.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'Tmin', 17.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), 'SNDP', 1.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'Tmedia', 23.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'DEWP', 10.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'STP', 859.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'VISIB', 12.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'FF', 11.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'MXSPD', 17.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'Tmax', 29.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'Tmin', 17.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), 'SNDP', 1.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'Tmedia', 26.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'DEWP', 23.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'STP', 857.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'VISIB', 4.8, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'FF', 7.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'MXSPD', 11.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'Tmax', 39.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'Tmin', 21.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'PREC', 0.08, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), 'SNDP', 3.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'Tmedia', 29.3, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'DEWP', 23.3, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'STP', 857.8, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'VISIB', 7.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'FF', 7.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'MXSPD', 8.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'Tmax', 34.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'Tmin', 27.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), 'SNDP', 4.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'Tmedia', 27.8, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'DEWP', 21.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'STP', 858.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'VISIB', 7.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'FF', 7.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'MXSPD', 8.9, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'Tmax', 32.4, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'Tmin', 23.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'PREC', 0.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), 'SNDP', 4.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'Tmedia', 28.1, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'DEWP', 24.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'P', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'STP', 848.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'VISIB', 3.5, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'FF', 5.7, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'MXSPD', 8.0, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'GUST', None, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'Tmax', 35.2, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'Tmin', 21.6, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'PREC', 0.12, True],
        [{'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), 'SNDP', 5.9, True]
    ]
    effective = noaa.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == expected_data


def test_is_format_compliant():
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    assert noaa.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong1_160080-99999-2019.op')
    assert not noaa.is_format_compliant(filepath)
    filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    assert not noaa.is_format_compliant(filepath)
