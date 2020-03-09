
from datetime import datetime
# from os.path import join

from sciafeed import checks

# from . import TEST_DATA_PATH


def test_data_internal_consistence_check():
    # right data
    input_data = [
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '1', 9.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '2', 355.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '3', 68.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '9', 83.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '12', 10205.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '1', 6.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '2', 310.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '3', 65.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '9', 86.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '12', 10198.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '1', 3.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '2', 288.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '3', 63.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '9', 86.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '12', 10196.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '1', 11.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '2', 357.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '3', 63.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '9', 87.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '12', 10189.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 3, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '1', 9.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '2', 1.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '3', 64.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '9', 88.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '12', 10184.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '1', 30.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '2', 6.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '3', 67.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '9', 89.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '12', 10181.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '1', 31.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '2', 6.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '3', 65.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '9', 93.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '12', 10181.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 20.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '1', 5.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '2', 342.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '3', 66.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '9', 95.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '12', 10182.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 8, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '1', 35.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '2', 12.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '3', 106.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '9', 88.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '12', 10179.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '1', 13.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '2', 154.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '3', 121.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '9', 72.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '12', 10182.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 10, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '1', 54.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '2', 218.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '3', 123.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '9', 69.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '12', 10177.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 11, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '1', 61.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '2', 225.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '3', 125.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '9', 73.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '12', 10167.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 12, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '1', 65.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '2', 226.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '3', 122.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '9', 74.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '12', 10162.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 13, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '1', 46.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '2', 221.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '3', 117.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '9', 78.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '12', 10161.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 14, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '1', 19.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '2', 233.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '3', 110.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '9', 82.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '12', 10161.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 15, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '1', 28.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '2', 355.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '3', 100.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '9', 96.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '12', 10158.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 16, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '1', 24.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '2', 345.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '3', 99.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '9', 96.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '12', 10156.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 17, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '1', 26.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '2', 357.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '3', 101.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '9', 97.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '12', 10155.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 18, 0), '19', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '1', 26.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '2', 2.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '3', 99.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '4', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '5', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '6', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '7', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '8', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '9', 100.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '10', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '11', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '12', 10154.0, True],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '13', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '14', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '15', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '16', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '17', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '18', None, False],
        [{'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '19', None, False]
    ]
    limiting_params = {'3': ('4', '5')}
    err_msgs, out_data = checks.data_internal_consistence_check(input_data, limiting_params)
    assert not err_msgs
    assert input_data == out_data

    # with errors
    limiting_params = {'3': ('1', '2')}
    err_msgs, out_data = checks.data_internal_consistence_check(input_data, limiting_params)
    assert err_msgs == [
        "The values of '3' and '2' are not consistent",
        "The values of '3' and '2' are not consistent",
        "The values of '3' and '2' are not consistent",
        "The values of '3' and '2' are not consistent",
        "The values of '3' and '2' are not consistent"
    ]

    assert out_data[78] == [
        {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '3', 64.0, False]
    out_data[78][-1] = True
    assert out_data[97] == [
        {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '3', 67.0, False]
    out_data[97][-1] = True
    assert out_data[116] == [
        {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '3', 65.0, False]
    out_data[116][-1] = True
    assert out_data[173] == [
        {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '3', 106.0, False]
    out_data[173][-1] = True
    assert out_data[363] == [
        {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '3', 99.0, False]
    out_data[363][-1] = True
    assert out_data == input_data

    # no limiting parameters: no check
    err_msgs, out_data = checks.data_internal_consistence_check(input_data)
    assert not err_msgs
    assert out_data == input_data


def test_data_weak_climatologic_check():
    parameters_thresholds = {
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

    # right data
    input_data = [
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 20.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False]
    ]
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data, parameters_thresholds)
    assert not err_msgs
    assert out_data == input_data

    # two errors
    assert parameters_thresholds['1'] == [0, 1020]
    assert parameters_thresholds['9'] == [20, 100]
    input_data = [
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 101.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False]
    ]
    err_msgs, out_data = checks.data_weak_climatologic_check(
        input_data, parameters_thresholds)
    assert err_msgs == ["The value of '1' is out of range [0.0, 1020.0]",
                        "The value of '9' is out of range [20.0, 100.0]"]
    assert out_data[0] == [
        {'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, False]
    assert out_data[8] == [
        {'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 101.0, False]
    out_data[0][-1] = True
    out_data[8][-1] = True
    assert out_data == input_data

    # no check if no parameters_thresholds
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data)
    assert not err_msgs
    assert out_data == input_data

    # no check if the value is already invalid
    input_data = [
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False]
    ]
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data, parameters_thresholds)
    assert not err_msgs
    assert out_data == out_data

    # no check if thresholds are not defined
    assert '12' not in parameters_thresholds
    input_data = [
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 99999.0, True],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False],
        [{'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False]
    ]
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data, parameters_thresholds)
    assert not err_msgs
    assert out_data == input_data


def test_do_file_weak_climatologic_check(tmpdir):
    pass
#     parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
#
#     # right file
#     filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
#     parsed = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
#     err_msgs, parsed_after_check = arpa19.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with specific errors
#     filepath = join(TEST_DATA_PATH, 'arpa19', 'wrong_70002_201301010000_201401010100.dat')
#     parsed = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
#     err_msgs, parsed_after_check = arpa19.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert err_msgs == [
#         (1, "The value of '1' is out of range [0.0, 1020.0]"),
#         (2, "The value of '2' is out of range [0.0, 360.0]"),
#         (3, "The value of '3' is out of range [-350.0, 450.0]"),
#     ]
#     assert parsed_after_check[0] == [
#         {'code': '70002', 'lat': 43.876999}, datetime(2013, 1, 1, 0, 0), '1', 2000.0, False]
#     parsed_after_check[0][-1] = True
#     assert parsed_after_check[20] == [
#         {'code': '70002', 'lat': 43.876999}, datetime(2013, 1, 1, 1, 0), '2', 361.0, False]
#     parsed_after_check[20][-1] = True
#     assert parsed_after_check[40] == [
#         {'code': '70002', 'lat': 43.876999}, datetime(2013, 1, 1, 2, 0), '3', -351.0, False]
#     parsed_after_check[40][-1] = True
#     assert parsed_after_check == parsed
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'arpa19', 'wrong_70001_201301010000_201401010100.dat')
#     err_msgs, _ = arpa19.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = arpa19.do_weak_climatologic_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check
#


def test_do_file_internal_consistence_check(tmpdir):
    pass
#     parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')
#     filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
#     parsed = arpa19.parse(filepath, parameters_filepath=parameters_filepath)
#
#     # right file
#     limiting_params = {'3': ('4', '5')}
#     err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with errors
#     limiting_params = {'3': ('1', '2')}
#     err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [
#         (5, "The values of '3' and '2' are not consistent"),
#         (6, "The values of '3' and '2' are not consistent"),
#         (7, "The values of '3' and '2' are not consistent"),
#         (10, "The values of '3' and '2' are not consistent"),
#         (20, "The values of '3' and '2' are not consistent")
#     ]
#     assert parsed_after_check[78] == [
#         {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 4, 0), '3', 64.0, False]
#     parsed_after_check[78][-1] = True
#     assert parsed_after_check[97] == [
#         {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 5, 0), '3', 67.0, False]
#     parsed_after_check[97][-1] = True
#     assert parsed_after_check[116] == [
#         {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 6, 0), '3', 65.0, False]
#     parsed_after_check[116][-1] = True
#     assert parsed_after_check[173] == [
#         {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 9, 0), '3', 106.0, False]
#     parsed_after_check[173][-1] = True
#     assert parsed_after_check[363] == [
#         {'code': '70001', 'lat': 43.876999}, datetime(2013, 1, 1, 19, 0), '3', 99.0, False]
#     parsed_after_check[363][-1] = True
#     assert parsed_after_check == parsed
#
#     # no limiting parameters: no check
#     err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'arpa19', 'wrong_70001_201301010000_201401010100.dat')
#     err_msgs, _ = arpa19.do_internal_consistence_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = arpa19.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check
#
#

    # parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
    #
    # # right file
    # filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    # parsed = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
    # err_msgs, parsed_after_check = arpafvg.do_weak_climatologic_check(
    #     filepath, parameters_filepath)
    # assert not err_msgs
    # assert parsed_after_check == parsed
    #
    # # with specific errors
    # filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00002_2018010101_2019010101.dat')
    # parsed = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
    # err_msgs, parsed_after_check = arpafvg.do_weak_climatologic_check(filepath, parameters_filepath)
    # assert err_msgs == [
    #     (1, "The value of 'FF' is out of range [0.0, 102.0]"),
    #     (2, "The value of 'DD' is out of range [0.0, 360.0]"),
    #     (3, "The value of 'PREC' is out of range [0.0, 989.0]")
    # ]
    # assert parsed_after_check[:2] == parsed[:2]
    # assert parsed_after_check[2][datetime(2018, 1, 1, 1, 0)]['FF'] == (103.0, False)
    # assert parsed_after_check[2][datetime(2018, 1, 1, 2, 0)]['DD'] == (361.0, False)
    # assert parsed_after_check[2][datetime(2018, 1, 1, 3, 0)]['PREC'] == (1000.0, False)
    # # with only formatting errors
    # filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    # err_msgs, _ = arpafvg.do_weak_climatologic_check(filepath, parameters_filepath)
    # assert not err_msgs
    #
    # # global error
    # filepath = str(tmpdir.join('report.txt'))
    # err_msgs, parsed_after_check = arpafvg.do_weak_climatologic_check(
    #     filepath, parameters_filepath)
    # assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
    # assert not parsed_after_check


# def test_do_weak_climatologic_check(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
#
#     # right file
#     filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
#     parsed = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
#     err_msgs, parsed_after_check = arpa21.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with specific errors
#     filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00202_201201010000_201301010100.dat')
#     parsed = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
#     err_msgs, parsed_after_check = arpa21.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert err_msgs == [
#         (1, "The value of '3' is out of range [-350.0, 450.0]"),
#         (2, "The value of '4' is out of range [-400.0, 400.0]"),
#         (3, "The value of '5' is out of range [-300.0, 500.0]")
#     ]
#     assert parsed_after_check[:2] == parsed[:2]
#     assert parsed_after_check[2][datetime(2012, 1, 1, 0, 0)]['3'] == (-570.0, False)
#     assert parsed_after_check[2][datetime(2012, 1, 1, 1, 0)]['4'] == (520.0, False)
#     assert parsed_after_check[2][datetime(2012, 1, 1, 2, 0)]['5'] == (580.0, False)
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00201_201201010000_201301010100.dat')
#     err_msgs, _ = arpa21.do_weak_climatologic_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = arpa21.do_weak_climatologic_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check
#
#
# def test_do_internal_consistence_check(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')
#     filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
#     parsed = arpa21.parse(filepath, parameters_filepath=parameters_filepath)
#
#     # right file
#     limiting_params = {'3': ('4', '5')}
#     err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with errors
#     limiting_params = {'9': ('10', '4')}
#     err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [
#         (1, "The values of '9' and '4' are not consistent"),
#         (2, "The values of '9' and '4' are not consistent"),
#         (3, "The values of '9' and '4' are not consistent"),
#         (4, "The values of '9' and '4' are not consistent"),
#         (5, "The values of '9' and '4' are not consistent"),
#         (6, "The values of '9' and '4' are not consistent"),
#         (7, "The values of '9' and '4' are not consistent"),
#         (8, "The values of '9' and '4' are not consistent"),
#         (9, "The values of '9' and '4' are not consistent"),
#         (20, "The values of '9' and '4' are not consistent")
#     ]
#     assert parsed_after_check[:2] == parsed[:2]
#     assert parsed_after_check[2][datetime(2012, 1, 1, 0, 0)]['9'] == (83.0, False)
#     assert parsed_after_check[2][datetime(2012, 1, 1, 1, 0)]['9'] == (81.0, False)
#     assert parsed_after_check[2][datetime(2012, 1, 1, 2, 0)]['9'] == (79.0, False)
#
#     # no limiting parameters: no check
#     err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00201_201201010000_201301010100.dat')
#     err_msgs, _ = arpa21.do_internal_consistence_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = arpa21.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check


# def test_do_internal_consistence_check(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')
#     filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
#     parsed = arpafvg.parse(filepath, parameters_filepath=parameters_filepath)
#
#     # right file
#     limiting_params = {'Tmedia': ('FF', 'DD')}  # 2: 6:5
#     err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with errors
#     limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
#     err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [
#         (1, "The values of 'PREC' and 'Bagnatura_f' are not consistent"),
#         (2, "The values of 'PREC' and 'Bagnatura_f' are not consistent"),
#         (3, "The values of 'PREC' and 'Bagnatura_f' are not consistent")
#     ]
#     assert parsed_after_check[:2] == parsed[:2]
#     assert parsed_after_check[2][datetime(2018, 1, 1, 1, 0)]['PREC'] == (0.0, False)
#     assert parsed_after_check[2][datetime(2018, 1, 1, 2, 0)]['PREC'] == (0.0, False)
#     assert parsed_after_check[2][datetime(2018, 1, 1, 3, 0)]['PREC'] == (0.0, False)
#
#     # no limiting parameters: no check
#     err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
#     err_msgs, _ = arpafvg.do_internal_consistence_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     err_msgs, parsed_after_check = arpafvg.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .dat, found .txt')]
#     assert not parsed_after_check


# def test_do_internal_consistence_check(tmpdir):
#     parameters_filepath = join(TEST_DATA_PATH, 'bolzano', 'bolzano_params.csv')
#     filepath = join(TEST_DATA_PATH, 'bolzano', 'MonteMaria.xls')
#     parsed = bolzano.parse(filepath, parameters_filepath=parameters_filepath)
#
#     # file with errors
#     limiting_params = {'Tmin': ('PREC', 'Tmax')}
#     err_msgs, parsed_after_check = bolzano.do_internal_consistence_check(
#         filepath, parameters_filepath, limiting_params)
#     assert err_msgs == [
#         (15, "The values of 'Tmin' and 'PREC' are not consistent"),
#         (16, "The values of 'Tmin' and 'PREC' are not consistent"),
#         (17, "The values of 'Tmin' and 'PREC' are not consistent"),
#         (18, "The values of 'Tmin' and 'PREC' are not consistent"),
#         (19, "The values of 'Tmin' and 'PREC' are not consistent"),
#         (20, "The values of 'Tmin' and 'PREC' are not consistent"),
#         (21, "The values of 'Tmin' and 'PREC' are not consistent")
#     ]
#     assert parsed_after_check == ('02500MS', {
#         datetime(1981, 1, 1, 0, 0): {
#             'PREC': (0.0, True),
#             'Tmax': (9.0, True),
#             'Tmin': (3.0, True)},
#         datetime(1981, 1, 2, 0, 0): {
#             'PREC': (0.4, True),
#             'Tmax': (5.0, True),
#             'Tmin': (-4.0, False)},
#         datetime(1981, 1, 3, 0, 0): {
#             'PREC': (0.0, True),
#             'Tmax': (5.0, True),
#             'Tmin': (-4.0, False)},
#         datetime(1981, 1, 4, 0, 0): {
#             'PREC': (14.5, True),
#             'Tmax': (9.0, True),
#             'Tmin': (1.0, False)},
#         datetime(1981, 1, 5, 0, 0): {
#             'PREC': (5.1, True),
#             'Tmax': (3.0, True),
#             'Tmin': (-8.0, False)},
#         datetime(1981, 1, 6, 0, 0): {
#             'PREC': (1.0, True),
#             'Tmax': (-5.0, True),
#             'Tmin': (-8.0, False)},
#         datetime(1981, 1, 7, 0, 0): {
#             'PREC': (6.1, True),
#             'Tmax': (-5.0, True),
#             'Tmin': (-9.0, False)},
#         datetime(1981, 1, 8, 0, 0): {
#             'PREC': (0.0, True),
#             'Tmax': (-7.0, True),
#             'Tmin': (-13.0, False)}})
#
#     # no limiting parameters: no check
#     err_msgs, parsed_after_check = bolzano.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert not err_msgs
#     assert parsed_after_check == parsed
#
#     # with only formatting errors
#     filepath = join(TEST_DATA_PATH, 'bolzano', 'wrong2.xls')
#     err_msgs, _ = bolzano.do_internal_consistence_check(filepath, parameters_filepath)
#     assert not err_msgs
#
#     # global error
#     filepath = str(tmpdir.join('report.txt'))
#     with open(filepath, 'w'):
#         pass
#     err_msgs, parsed_after_check = bolzano.do_internal_consistence_check(
#         filepath, parameters_filepath)
#     assert err_msgs == [(0, 'Extension expected must be .xls, found .txt')]
#     assert not parsed_after_check

