from datetime import datetime
from os.path import join, exists

import pytest
from pprint import pprint
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


def test_parse_row():
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    parameters_map = noaa.load_parameter_file(parameters_filepath=parameters_filepath)

    # full parsing
    expected = ({'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), {
        'DEWP': (23.7, True),
        'FF': (7.9, True),
        'GUST': (None, True),
        'MXSPD': (11.1, True),
        'P': (None, True),
        'PREC': (0.0, True),
        'SNDP': (1.6, True),
        'STP': (859.6, True),
        'Tmax': (41.0, True),
        'Tmedia': (33.9, True),
        'Tmin': (28.4, True),
        'VISIB': (11.0, True)
    })
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


def test_validate_format(tmpdir):
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
    expected_data = [(
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 1, 0, 0), {
            'DEWP': (23.7, True),
            'FF': (7.9, True),
            'GUST': (None, True),
            'MXSPD': (11.1, True),
            'P': (None, True),
            'PREC': (0.0, True),
            'SNDP': (1.6, True),
            'STP': (859.6, True),
            'Tmax': (41.0, True),
            'Tmedia': (33.9, True),
            'Tmin': (28.4, True),
            'VISIB': (11.0, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 2, 0, 0), {
            'DEWP': (15.5, True),
            'FF': (7.9, True),
            'GUST': (None, True),
            'MXSPD': (11.1, True),
            'P': (None, True),
            'PREC': (0.0, True),
            'SNDP': (1.6, True),
            'STP': (855.1, True),
            'Tmax': (32.4, True),
            'Tmedia': (25.5, True),
            'Tmin': (19.4, True),
            'VISIB': (4.8, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 3, 0, 0), {
            'DEWP': (4.3, True),
            'FF': (7.9, True),
            'GUST': (None, True),
            'MXSPD': (9.9, True),
            'P': (None, True),
            'PREC': (0.0, True),
            'SNDP': (1.6, True),
            'STP': (860.1, True),
            'Tmax': (26.2, True),
            'Tmedia': (19.3, True),
            'Tmin': (17.2, True),
            'VISIB': (7.1, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 4, 0, 0), {
            'DEWP': (10.9, True),
            'FF': (11.6, True),
            'GUST': (None, True),
            'MXSPD': (17.1, True),
            'P': (None, True),
            'PREC': (0.0, True),
            'SNDP': (1.6, True),
            'STP': (859.6, True),
            'Tmax': (29.5, True),
            'Tmedia': (23.0, True),
            'Tmin': (17.2, True),
            'VISIB': (12.6, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 5, 0, 0), {
            'DEWP': (23.7, True),
            'FF': (7.2, True),
            'GUST': (None, True),
            'MXSPD': (11.1, True),
            'P': (None, True),
            'PREC': (0.08, True),
            'SNDP': (3.5, True),
            'STP': (857.4, True),
            'Tmax': (39.2, True),
            'Tmedia': (26.5, True),
            'Tmin': (21.9, True),
            'VISIB': (4.8, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 6, 0, 0), {
            'DEWP': (23.3, True),
            'FF': (7.5, True),
            'GUST': (None, True),
            'MXSPD': (8.9, True),
            'P': (None, True),
            'PREC': (0.0, True),
            'SNDP': (4.7, True),
            'STP': (857.8, True),
            'Tmax': (34.2, True),
            'Tmedia': (29.3, True),
            'Tmin': (27.0, True),
            'VISIB': (7.9, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 7, 0, 0), {
            'DEWP': (21.7, True),
            'FF': (7.1, True),
            'GUST': (None, True),
            'MXSPD': (8.9, True),
            'P': (None, True),
            'PREC': (0.0, True),
            'SNDP': (4.7, True),
            'STP': (858.9, True),
            'Tmax': (32.4, True),
            'Tmedia': (27.8, True),
            'Tmin': (23.0, True),
            'VISIB': (7.4, True)
        }
        ), (
        {'code': '160080', 'wban': '99999'}, datetime(2019, 1, 8, 0, 0), {
            'DEWP': (24.0, True),
            'FF': (5.7, True),
            'GUST': (None, True),
            'MXSPD': (8.0, True),
            'P': (None, True),
            'PREC': (0.12, True),
            'SNDP': (5.9, True),
            'STP': (848.0, True),
            'Tmax': (35.2, True),
            'Tmedia': (28.1, True),
            'Tmin': (21.6, True),
            'VISIB': (3.5, True)
        }
        )
    ]
    effective = noaa.parse(filepath, parameters_filepath=parameters_filepath)
    assert effective == expected_data


def test_write_data(tmpdir):
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    data = noaa.parse(filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        'station;latitude;date;parameter;value;valid\n',
        '160080;;2019-01-01T00:00:00;Tmedia;33.9;True\n',
        '160080;;2019-01-01T00:00:00;DEWP;23.7;True\n',
        '160080;;2019-01-01T00:00:00;STP;859.6;True\n',
        '160080;;2019-01-01T00:00:00;VISIB;11.0;True\n',
        '160080;;2019-01-01T00:00:00;FF;7.9;True\n',
        '160080;;2019-01-01T00:00:00;MXSPD;11.1;True\n',
        '160080;;2019-01-01T00:00:00;Tmax;41.0;True\n',
        '160080;;2019-01-01T00:00:00;Tmin;28.4;True\n',
        '160080;;2019-01-01T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-01T00:00:00;SNDP;1.6;True\n',
        '160080;;2019-01-02T00:00:00;Tmedia;25.5;True\n',
        '160080;;2019-01-02T00:00:00;DEWP;15.5;True\n',
        '160080;;2019-01-02T00:00:00;STP;855.1;True\n',
        '160080;;2019-01-02T00:00:00;VISIB;4.8;True\n',
        '160080;;2019-01-02T00:00:00;FF;7.9;True\n',
        '160080;;2019-01-02T00:00:00;MXSPD;11.1;True\n',
        '160080;;2019-01-02T00:00:00;Tmax;32.4;True\n',
        '160080;;2019-01-02T00:00:00;Tmin;19.4;True\n',
        '160080;;2019-01-02T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-02T00:00:00;SNDP;1.6;True\n',
        '160080;;2019-01-03T00:00:00;Tmedia;19.3;True\n',
        '160080;;2019-01-03T00:00:00;DEWP;4.3;True\n',
        '160080;;2019-01-03T00:00:00;STP;860.1;True\n',
        '160080;;2019-01-03T00:00:00;VISIB;7.1;True\n',
        '160080;;2019-01-03T00:00:00;FF;7.9;True\n',
        '160080;;2019-01-03T00:00:00;MXSPD;9.9;True\n',
        '160080;;2019-01-03T00:00:00;Tmax;26.2;True\n',
        '160080;;2019-01-03T00:00:00;Tmin;17.2;True\n',
        '160080;;2019-01-03T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-03T00:00:00;SNDP;1.6;True\n',
        '160080;;2019-01-04T00:00:00;Tmedia;23.0;True\n',
        '160080;;2019-01-04T00:00:00;DEWP;10.9;True\n',
        '160080;;2019-01-04T00:00:00;STP;859.6;True\n',
        '160080;;2019-01-04T00:00:00;VISIB;12.6;True\n',
        '160080;;2019-01-04T00:00:00;FF;11.6;True\n',
        '160080;;2019-01-04T00:00:00;MXSPD;17.1;True\n',
        '160080;;2019-01-04T00:00:00;Tmax;29.5;True\n',
        '160080;;2019-01-04T00:00:00;Tmin;17.2;True\n',
        '160080;;2019-01-04T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-04T00:00:00;SNDP;1.6;True\n',
        '160080;;2019-01-05T00:00:00;Tmedia;26.5;True\n',
        '160080;;2019-01-05T00:00:00;DEWP;23.7;True\n',
        '160080;;2019-01-05T00:00:00;STP;857.4;True\n',
        '160080;;2019-01-05T00:00:00;VISIB;4.8;True\n',
        '160080;;2019-01-05T00:00:00;FF;7.2;True\n',
        '160080;;2019-01-05T00:00:00;MXSPD;11.1;True\n',
        '160080;;2019-01-05T00:00:00;Tmax;39.2;True\n',
        '160080;;2019-01-05T00:00:00;Tmin;21.9;True\n',
        '160080;;2019-01-05T00:00:00;PREC;0.08;True\n',
        '160080;;2019-01-05T00:00:00;SNDP;3.5;True\n',
        '160080;;2019-01-06T00:00:00;Tmedia;29.3;True\n',
        '160080;;2019-01-06T00:00:00;DEWP;23.3;True\n',
        '160080;;2019-01-06T00:00:00;STP;857.8;True\n',
        '160080;;2019-01-06T00:00:00;VISIB;7.9;True\n',
        '160080;;2019-01-06T00:00:00;FF;7.5;True\n',
        '160080;;2019-01-06T00:00:00;MXSPD;8.9;True\n',
        '160080;;2019-01-06T00:00:00;Tmax;34.2;True\n',
        '160080;;2019-01-06T00:00:00;Tmin;27.0;True\n',
        '160080;;2019-01-06T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-06T00:00:00;SNDP;4.7;True\n',
        '160080;;2019-01-07T00:00:00;Tmedia;27.8;True\n',
        '160080;;2019-01-07T00:00:00;DEWP;21.7;True\n',
        '160080;;2019-01-07T00:00:00;STP;858.9;True\n',
        '160080;;2019-01-07T00:00:00;VISIB;7.4;True\n',
        '160080;;2019-01-07T00:00:00;FF;7.1;True\n',
        '160080;;2019-01-07T00:00:00;MXSPD;8.9;True\n',
        '160080;;2019-01-07T00:00:00;Tmax;32.4;True\n',
        '160080;;2019-01-07T00:00:00;Tmin;23.0;True\n',
        '160080;;2019-01-07T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-07T00:00:00;SNDP;4.7;True\n',
        '160080;;2019-01-08T00:00:00;Tmedia;28.1;True\n',
        '160080;;2019-01-08T00:00:00;DEWP;24.0;True\n',
        '160080;;2019-01-08T00:00:00;STP;848.0;True\n',
        '160080;;2019-01-08T00:00:00;VISIB;3.5;True\n',
        '160080;;2019-01-08T00:00:00;FF;5.7;True\n',
        '160080;;2019-01-08T00:00:00;MXSPD;8.0;True\n',
        '160080;;2019-01-08T00:00:00;Tmax;35.2;True\n',
        '160080;;2019-01-08T00:00:00;Tmin;21.6;True\n',
        '160080;;2019-01-08T00:00:00;PREC;0.12;True\n',
        '160080;;2019-01-08T00:00:00;SNDP;5.9;True\n'
    ]
    assert not exists(out_filepath)
    noaa.write_data(data, out_filepath)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows

    expected_rows = [
        'station;latitude;date;parameter;value;valid\n',
        '160080;;2019-01-01T00:00:00;Tmedia;33.9;True\n',
        '160080;;2019-01-01T00:00:00;DEWP;23.7;True\n',
        '160080;;2019-01-01T00:00:00;STP;859.6;True\n',
        '160080;;2019-01-01T00:00:00;FF;7.9;True\n',
        '160080;;2019-01-01T00:00:00;MXSPD;11.1;True\n',
        '160080;;2019-01-01T00:00:00;Tmax;41.0;True\n',
        '160080;;2019-01-01T00:00:00;Tmin;28.4;True\n',
        '160080;;2019-01-01T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-02T00:00:00;Tmedia;25.5;True\n',
        '160080;;2019-01-02T00:00:00;DEWP;15.5;True\n',
        '160080;;2019-01-02T00:00:00;STP;855.1;True\n',
        '160080;;2019-01-02T00:00:00;FF;7.9;True\n',
        '160080;;2019-01-02T00:00:00;MXSPD;11.1;True\n',
        '160080;;2019-01-02T00:00:00;Tmax;32.4;True\n',
        '160080;;2019-01-02T00:00:00;Tmin;19.4;True\n',
        '160080;;2019-01-02T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-03T00:00:00;Tmedia;19.3;True\n',
        '160080;;2019-01-03T00:00:00;DEWP;4.3;True\n',
        '160080;;2019-01-03T00:00:00;STP;860.1;True\n',
        '160080;;2019-01-03T00:00:00;FF;7.9;True\n',
        '160080;;2019-01-03T00:00:00;MXSPD;9.9;True\n',
        '160080;;2019-01-03T00:00:00;Tmax;26.2;True\n',
        '160080;;2019-01-03T00:00:00;Tmin;17.2;True\n',
        '160080;;2019-01-03T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-04T00:00:00;Tmedia;23.0;True\n',
        '160080;;2019-01-04T00:00:00;DEWP;10.9;True\n',
        '160080;;2019-01-04T00:00:00;STP;859.6;True\n',
        '160080;;2019-01-04T00:00:00;FF;11.6;True\n',
        '160080;;2019-01-04T00:00:00;MXSPD;17.1;True\n',
        '160080;;2019-01-04T00:00:00;Tmax;29.5;True\n',
        '160080;;2019-01-04T00:00:00;Tmin;17.2;True\n',
        '160080;;2019-01-04T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-05T00:00:00;Tmedia;26.5;True\n',
        '160080;;2019-01-05T00:00:00;DEWP;23.7;True\n',
        '160080;;2019-01-05T00:00:00;STP;857.4;True\n',
        '160080;;2019-01-05T00:00:00;FF;7.2;True\n',
        '160080;;2019-01-05T00:00:00;MXSPD;11.1;True\n',
        '160080;;2019-01-05T00:00:00;Tmax;39.2;True\n',
        '160080;;2019-01-05T00:00:00;Tmin;21.9;True\n',
        '160080;;2019-01-05T00:00:00;PREC;0.08;True\n',
        '160080;;2019-01-06T00:00:00;Tmedia;29.3;True\n',
        '160080;;2019-01-06T00:00:00;DEWP;23.3;True\n',
        '160080;;2019-01-06T00:00:00;STP;857.8;True\n',
        '160080;;2019-01-06T00:00:00;FF;7.5;True\n',
        '160080;;2019-01-06T00:00:00;MXSPD;8.9;True\n',
        '160080;;2019-01-06T00:00:00;Tmax;34.2;True\n',
        '160080;;2019-01-06T00:00:00;Tmin;27.0;True\n',
        '160080;;2019-01-06T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-07T00:00:00;Tmedia;27.8;True\n',
        '160080;;2019-01-07T00:00:00;DEWP;21.7;True\n',
        '160080;;2019-01-07T00:00:00;STP;858.9;True\n',
        '160080;;2019-01-07T00:00:00;FF;7.1;True\n',
        '160080;;2019-01-07T00:00:00;MXSPD;8.9;True\n',
        '160080;;2019-01-07T00:00:00;Tmax;32.4;True\n',
        '160080;;2019-01-07T00:00:00;Tmin;23.0;True\n',
        '160080;;2019-01-07T00:00:00;PREC;0.0;True\n',
        '160080;;2019-01-08T00:00:00;Tmedia;28.1;True\n',
        '160080;;2019-01-08T00:00:00;DEWP;24.0;True\n',
        '160080;;2019-01-08T00:00:00;STP;848.0;True\n',
        '160080;;2019-01-08T00:00:00;FF;5.7;True\n',
        '160080;;2019-01-08T00:00:00;MXSPD;8.0;True\n',
        '160080;;2019-01-08T00:00:00;Tmax;35.2;True\n',
        '160080;;2019-01-08T00:00:00;Tmin;21.6;True\n',
        '160080;;2019-01-08T00:00:00;PREC;0.12;True\n',
    ]
    noaa.write_data(data, out_filepath, omit_parameters=('VISIB', 'SNDP'))
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows


def test_row_weak_climatologic_check():
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    parameters_map = noaa.load_parameter_file(parameters_filepath)
    parameters_thresholds = noaa.load_parameter_thresholds(parameters_filepath)

    # right row
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    row_parsed = noaa.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = noaa.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # two errors
    assert parameters_thresholds['PREC'] == [0.0, 38.937]
    assert parameters_thresholds['Tmedia'] == [-31.0, 113.0]
    row = "160080 99999  20190101   -32.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4  40.00F   1.6  000000"
    row_parsed = noaa.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = noaa.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert err_msgs == ["The value of 'Tmedia' is out of range [-31.0, 113.0]",
                        "The value of 'PREC' is out of range [0.0, 38.937]",
                        ]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['PREC'] == (40.0, False)
    assert parsed_row_updated[2]['Tmedia'] == (-32.9, False)
    parsed_row_updated[2]['PREC'] = (40.0, True)
    parsed_row_updated[2]['Tmedia'] = (-32.9, True)
    assert parsed_row_updated == row_parsed

    # no check if no parameters_thresholds
    err_msgs, parsed_row_updated = noaa.row_weak_climatologic_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is already invalid
    row_parsed[2]['PREC'] = (40.0, False)
    row_parsed[2]['Tmedia'] = (-32.9, False)
    err_msgs, parsed_row_updated = noaa.row_weak_climatologic_check(
        row_parsed, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[2]['PREC'] = (40.0, True)
    row_parsed[2]['Tmedia'] = (-32.9, True)


def test_row_internal_consistence_check():
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    parameters_map = noaa.load_parameter_file(parameters_filepath)
    limiting_params = {'Tmedia': ('Tmin', 'Tmax')}

    # right row
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    28.4   0.00F   1.6  000000"
    row_parsed = noaa.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = noaa.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # wrong value
    row = "160080 99999  20190101    33.9 24    23.7 24  9999.9  0   859.6 24   11.0 24    " \
          "7.9 24   11.1  999.9    41.0    35.4   0.00F   1.6  000000"
    row_parsed = noaa.parse_row(row, parameters_map)
    err_msgs, parsed_row_updated = noaa.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert err_msgs == ["The values of 'Tmedia' and 'Tmin' are not consistent"]
    assert parsed_row_updated[:2] == row_parsed[:2]
    assert parsed_row_updated[2]['Tmedia'] == (33.9, False)
    parsed_row_updated[2]['Tmedia'] = (33.9, True)
    assert parsed_row_updated == row_parsed

    # no check if no limiting parameters
    err_msgs, parsed_row_updated = noaa.row_internal_consistence_check(row_parsed)
    assert not err_msgs
    assert parsed_row_updated == row_parsed

    # no check if the value is invalid
    row_parsed[2]['Tmedia'] = (33.9, False)
    err_msgs, parsed_row_updated = noaa.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[2]['Tmedia'] = (33.9, True)

    # no check if one of the thresholds is invalid
    row_parsed[2]['Tmin'] = (35.4, False)
    err_msgs, parsed_row_updated = noaa.row_internal_consistence_check(
        row_parsed, limiting_params)
    assert not err_msgs
    assert parsed_row_updated == row_parsed
    row_parsed[2]['Tmin'] = (35.4, True)


def test_do_weak_climatologic_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')

    # right file
    filepath = join(TEST_DATA_PATH, 'noaa', '160080-99999-2019.op')
    parsed = noaa.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = noaa.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with specific errors
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong3_160080-99999-2019.op')
    parsed = noaa.parse(filepath, parameters_filepath=parameters_filepath)
    err_msgs, parsed_after_check = noaa.do_weak_climatologic_check(filepath, parameters_filepath)
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
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong2_160080-99999-2019.op')
    err_msgs, _ = noaa.do_weak_climatologic_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, parsed_after_check = noaa.do_weak_climatologic_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'File extension must be .op')]
    assert not parsed_after_check


def test_do_internal_consistence_check(tmpdir):
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    filepath = join(TEST_DATA_PATH, 'noaa', 'loc01_00201_201201010000_201301010100.dat')
    parsed = noaa.parse(filepath, parameters_filepath=parameters_filepath)

    # right file
    limiting_params = {'3': ('4', '5')}
    err_msgs, parsed_after_check = noaa.do_internal_consistence_check(
        filepath, parameters_filepath, limiting_params)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with errors
    limiting_params = {'9': ('10', '4')}
    err_msgs, parsed_after_check = noaa.do_internal_consistence_check(
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
    err_msgs, parsed_after_check = noaa.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert not err_msgs
    assert parsed_after_check == parsed

    # with only formatting errors
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong_00201_201201010000_201301010100.dat')
    err_msgs, _ = noaa.do_internal_consistence_check(filepath, parameters_filepath)
    assert not err_msgs

    # global error
    filepath = str(tmpdir.join('report.txt'))
    err_msgs, parsed_after_check = noaa.do_internal_consistence_check(
        filepath, parameters_filepath)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
    assert not parsed_after_check


def test_parse_and_check(tmpdir):
    filepath = join(TEST_DATA_PATH, 'noaa', 'wrong_00202_201201010000_201301010100.dat')
    parameters_filepath = join(TEST_DATA_PATH, 'noaa', 'noaa_params.csv')
    limiting_params = {'9': ('10', '4')}
    err_msgs, data_parsed = noaa.parse_and_check(
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
    err_msgs, _ = noaa.parse_and_check(
        filepath, parameters_filepath, limiting_params)
    assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
