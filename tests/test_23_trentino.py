
from datetime import datetime
from os.path import join, exists

from sciafeed import trentino
from . import TEST_DATA_PATH
from pprint import pprint
import pytest


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    parameter_map = trentino.load_parameter_file(test_filepath)
    for key, value in parameter_map.items():
        assert 'CSV_CODE' in value
        assert 'par_code' in value
        assert 'description' in value


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    expected_thresholds = {
        'PREC': [0.0, 989.0], 'Tmax': [-30.0, 50.0], 'Tmin': [-40.0, 40.0]
    }
    parameter_thresholds = trentino.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_parse_filename():
    # TODO
    pass


def test_validate_filename():
    # TODO
    pass


def test_guess_fieldnames():
    parmap_filepath = join(TEST_DATA_PATH, 'trentino', 'trentino_params.csv')
    parameters_map = trentino.load_parameter_file(parmap_filepath)
    # right file
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'T0001.csv')
    fieldnames, station, extra_info = trentino.guess_fieldnames(test_filepath, parameters_map)
    assert fieldnames == ['date', 'Tmin', 'quality']
    assert station == 'T0001'
    assert extra_info == {
        'code': 'T0001',
        'lat': 46.06227631,
        'lon': 11.23670156,
        'height': 475.0,
        'desc': 'Pergine Valsugana (Convento)'
    }

    # wrong file: missing header
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'wrong1.csv')
    with pytest.raises(ValueError) as err:
        trentino.guess_fieldnames(test_filepath, parameters_map)
    assert str(err.value) == 'trentino header not compliant'

    # tolerated file: strange header
    test_filepath = join(TEST_DATA_PATH, 'trentino', 'wrong2.csv')
    fieldnames, station, extra_info = trentino.guess_fieldnames(test_filepath, parameters_map)
    assert fieldnames == ['date', 'Tmin', 'quality']
    assert station == 'T0001'
    assert not extra_info
