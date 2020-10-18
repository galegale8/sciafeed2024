
from datetime import datetime
from decimal import Decimal
import math
from pprint import pprint
from sciafeed import checks


def set_row_index(input_data):
    new_input_data = []
    for i, record in enumerate(input_data, 1):
        record_md = record[0].copy()
        record_md['row'] = i
        new_input_data.append((record_md,) + record[1:])
    return new_input_data


def test_data_internal_consistence_check():
    # right data
    metadata = {'cod_utente': '70001', 'lat': 43.876999, 'cod_rete': '15'}
    input_data = [
        (metadata, datetime(2013, 1, 1, 0, 0), '1', 9.0, True),
        (metadata, datetime(2013, 1, 1, 0, 0), '2', 355.0, True),
        (metadata, datetime(2013, 1, 1, 0, 0), '3', 68.0, True),
        (metadata, datetime(2013, 1, 1, 0, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '9', 83.0, True),
        (metadata, datetime(2013, 1, 1, 0, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '12', 10205.0, True),
        (metadata, datetime(2013, 1, 1, 0, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 0, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '1', 6.0, True),
        (metadata, datetime(2013, 1, 1, 1, 0), '2', 310.0, True),
        (metadata, datetime(2013, 1, 1, 1, 0), '3', 65.0, True),
        (metadata, datetime(2013, 1, 1, 1, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '9', 86.0, True),
        (metadata, datetime(2013, 1, 1, 1, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '12', 10198.0, True),
        (metadata, datetime(2013, 1, 1, 1, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 1, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '1', 3.0, True),
        (metadata, datetime(2013, 1, 1, 2, 0), '2', 288.0, True),
        (metadata, datetime(2013, 1, 1, 2, 0), '3', 63.0, True),
        (metadata, datetime(2013, 1, 1, 2, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '9', 86.0, True),
        (metadata, datetime(2013, 1, 1, 2, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '12', 10196.0, True),
        (metadata, datetime(2013, 1, 1, 2, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 2, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '1', 11.0, True),
        (metadata, datetime(2013, 1, 1, 3, 0), '2', 357.0, True),
        (metadata, datetime(2013, 1, 1, 3, 0), '3', 63.0, True),
        (metadata, datetime(2013, 1, 1, 3, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '9', 87.0, True),
        (metadata, datetime(2013, 1, 1, 3, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '12', 10189.0, True),
        (metadata, datetime(2013, 1, 1, 3, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 3, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '1', 9.0, True),
        (metadata, datetime(2013, 1, 1, 4, 0), '2', 1.0, True),
        (metadata, datetime(2013, 1, 1, 4, 0), '3', 64.0, True),
        (metadata, datetime(2013, 1, 1, 4, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '9', 88.0, True),
        (metadata, datetime(2013, 1, 1, 4, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '12', 10184.0, True),
        (metadata, datetime(2013, 1, 1, 4, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 4, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '1', 30.0, True),
        (metadata, datetime(2013, 1, 1, 5, 0), '2', 6.0, True),
        (metadata, datetime(2013, 1, 1, 5, 0), '3', 67.0, True),
        (metadata, datetime(2013, 1, 1, 5, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '9', 89.0, True),
        (metadata, datetime(2013, 1, 1, 5, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '12', 10181.0, True),
        (metadata, datetime(2013, 1, 1, 5, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 5, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '1', 31.0, True),
        (metadata, datetime(2013, 1, 1, 6, 0), '2', 6.0, True),
        (metadata, datetime(2013, 1, 1, 6, 0), '3', 65.0, True),
        (metadata, datetime(2013, 1, 1, 6, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '9', 93.0, True),
        (metadata, datetime(2013, 1, 1, 6, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '12', 10181.0, True),
        (metadata, datetime(2013, 1, 1, 6, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 6, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '1', 20.0, True),
        (metadata, datetime(2013, 1, 1, 7, 0), '2', 358.0, True),
        (metadata, datetime(2013, 1, 1, 7, 0), '3', 65.0, True),
        (metadata, datetime(2013, 1, 1, 7, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '9', 93.0, True),
        (metadata, datetime(2013, 1, 1, 7, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True),
        (metadata, datetime(2013, 1, 1, 7, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 7, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '1', 5.0, True),
        (metadata, datetime(2013, 1, 1, 8, 0), '2', 342.0, True),
        (metadata, datetime(2013, 1, 1, 8, 0), '3', 66.0, True),
        (metadata, datetime(2013, 1, 1, 8, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '9', 95.0, True),
        (metadata, datetime(2013, 1, 1, 8, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '12', 10182.0, True),
        (metadata, datetime(2013, 1, 1, 8, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 8, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '1', 35.0, True),
        (metadata, datetime(2013, 1, 1, 9, 0), '2', 12.0, True),
        (metadata, datetime(2013, 1, 1, 9, 0), '3', 106.0, True),
        (metadata, datetime(2013, 1, 1, 9, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '9', 88.0, True),
        (metadata, datetime(2013, 1, 1, 9, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '12', 10179.0, True),
        (metadata, datetime(2013, 1, 1, 9, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 9, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '1', 13.0, True),
        (metadata, datetime(2013, 1, 1, 10, 0), '2', 154.0, True),
        (metadata, datetime(2013, 1, 1, 10, 0), '3', 121.0, True),
        (metadata, datetime(2013, 1, 1, 10, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '9', 72.0, True),
        (metadata, datetime(2013, 1, 1, 10, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '12', 10182.0, True),
        (metadata, datetime(2013, 1, 1, 10, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 10, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '1', 54.0, True),
        (metadata, datetime(2013, 1, 1, 11, 0), '2', 218.0, True),
        (metadata, datetime(2013, 1, 1, 11, 0), '3', 123.0, True),
        (metadata, datetime(2013, 1, 1, 11, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '9', 69.0, True),
        (metadata, datetime(2013, 1, 1, 11, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '12', 10177.0, True),
        (metadata, datetime(2013, 1, 1, 11, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 11, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '1', 61.0, True),
        (metadata, datetime(2013, 1, 1, 12, 0), '2', 225.0, True),
        (metadata, datetime(2013, 1, 1, 12, 0), '3', 125.0, True),
        (metadata, datetime(2013, 1, 1, 12, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '9', 73.0, True),
        (metadata, datetime(2013, 1, 1, 12, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '12', 10167.0, True),
        (metadata, datetime(2013, 1, 1, 12, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 12, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '1', 65.0, True),
        (metadata, datetime(2013, 1, 1, 13, 0), '2', 226.0, True),
        (metadata, datetime(2013, 1, 1, 13, 0), '3', 122.0, True),
        (metadata, datetime(2013, 1, 1, 13, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '9', 74.0, True),
        (metadata, datetime(2013, 1, 1, 13, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '12', 10162.0, True),
        (metadata, datetime(2013, 1, 1, 13, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 13, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '1', 46.0, True),
        (metadata, datetime(2013, 1, 1, 14, 0), '2', 221.0, True),
        (metadata, datetime(2013, 1, 1, 14, 0), '3', 117.0, True),
        (metadata, datetime(2013, 1, 1, 14, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '9', 78.0, True),
        (metadata, datetime(2013, 1, 1, 14, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '12', 10161.0, True),
        (metadata, datetime(2013, 1, 1, 14, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 14, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '1', 19.0, True),
        (metadata, datetime(2013, 1, 1, 15, 0), '2', 233.0, True),
        (metadata, datetime(2013, 1, 1, 15, 0), '3', 110.0, True),
        (metadata, datetime(2013, 1, 1, 15, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '9', 82.0, True),
        (metadata, datetime(2013, 1, 1, 15, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '12', 10161.0, True),
        (metadata, datetime(2013, 1, 1, 15, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 15, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '1', 28.0, True),
        (metadata, datetime(2013, 1, 1, 16, 0), '2', 355.0, True),
        (metadata, datetime(2013, 1, 1, 16, 0), '3', 100.0, True),
        (metadata, datetime(2013, 1, 1, 16, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '9', 96.0, True),
        (metadata, datetime(2013, 1, 1, 16, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '12', 10158.0, True),
        (metadata, datetime(2013, 1, 1, 16, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 16, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '1', 24.0, True),
        (metadata, datetime(2013, 1, 1, 17, 0), '2', 345.0, True),
        (metadata, datetime(2013, 1, 1, 17, 0), '3', 99.0, True),
        (metadata, datetime(2013, 1, 1, 17, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '9', 96.0, True),
        (metadata, datetime(2013, 1, 1, 17, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '12', 10156.0, True),
        (metadata, datetime(2013, 1, 1, 17, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 17, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '1', 26.0, True),
        (metadata, datetime(2013, 1, 1, 18, 0), '2', 357.0, True),
        (metadata, datetime(2013, 1, 1, 18, 0), '3', 101.0, True),
        (metadata, datetime(2013, 1, 1, 18, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '9', 97.0, True),
        (metadata, datetime(2013, 1, 1, 18, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '12', 10155.0, True),
        (metadata, datetime(2013, 1, 1, 18, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 18, 0), '19', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '1', 26.0, True),
        (metadata, datetime(2013, 1, 1, 19, 0), '2', 2.0, True),
        (metadata, datetime(2013, 1, 1, 19, 0), '3', 99.0, True),
        (metadata, datetime(2013, 1, 1, 19, 0), '4', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '5', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '6', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '7', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '8', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '9', 100.0, True),
        (metadata, datetime(2013, 1, 1, 19, 0), '10', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '11', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '12', 10154.0, True),
        (metadata, datetime(2013, 1, 1, 19, 0), '13', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '14', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '15', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '16', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '17', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '18', None, False),
        (metadata, datetime(2013, 1, 1, 19, 0), '19', None, False),
    ]
    input_data = set_row_index(input_data)
    limiting_params = {'3': ('4', '5')}
    err_msgs, out_data = checks.data_internal_consistence_check(input_data, limiting_params)
    assert not err_msgs
    assert input_data == out_data

    # with errors
    limiting_params = {'3': ('1', '2')}
    err_msgs, out_data = checks.data_internal_consistence_check(input_data, limiting_params)
    assert err_msgs == [
        (79, "The values of '3' and '2' are not consistent"),
        (98, "The values of '3' and '2' are not consistent"),
        (117, "The values of '3' and '2' are not consistent"),
        (174, "The values of '3' and '2' are not consistent"),
        (364, "The values of '3' and '2' are not consistent"),
    ]
    for err_indx, _ in err_msgs:
        assert out_data[err_indx-1][-1] is False

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
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 20.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False),
    ]
    input_data = set_row_index(input_data)
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data, parameters_thresholds)
    assert not err_msgs
    assert out_data == input_data

    # two errors
    assert parameters_thresholds['1'] == [0, 1020]
    assert parameters_thresholds['9'] == [20, 100]
    input_data = [
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 101.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False),
    ]
    input_data = set_row_index(input_data)
    err_msgs, out_data = checks.data_weak_climatologic_check(
        input_data, parameters_thresholds)
    assert err_msgs == [
        (1, "The value of '1' is out of range [0.0, 1020.0]"),
        (9, "The value of '9' is out of range [20.0, 100.0]")
    ]
    for err_indx, _ in err_msgs:
        assert out_data[err_indx-1][-1] is False

    # no check if no parameters_thresholds
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data)
    assert not err_msgs
    assert out_data == input_data

    # no check if the value is already invalid
    input_data = [
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 10182.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False),
    ]
    input_data = set_row_index(input_data)
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data, parameters_thresholds)
    assert not err_msgs
    assert out_data == out_data

    # no check if thresholds are not defined
    assert '12' not in parameters_thresholds
    input_data = [
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '1', 1021.0, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '2', 358.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '3', 65.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '4', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '5', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '6', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '7', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '8', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '9', 93.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '10', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '11', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '12', 99999.0, True),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '13', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '14', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '15', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '16', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '17', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '18', None, False),
        ({'lat': 43.876999}, datetime(2013, 1, 1, 7, 0), '19', None, False),
    ]
    input_data = set_row_index(input_data)
    err_msgs, out_data = checks.data_weak_climatologic_check(input_data, parameters_thresholds)
    assert not err_msgs
    assert out_data == input_data


def compare_noindexes(records_before, records_new, indexes_to_exclude=(3,)):
    """
    utiliy functions to compare 2 list of lists without considering a list of indexes
    """
    assert len(records_before) == len(records_new), 'records length is not the same'
    for i, record in enumerate(records_before):
        for j, term in enumerate(record):
            if j in indexes_to_exclude:
                continue
            assert term == records_new[i][j], '%s != %s (index %s %s)' \
                                              % (term, records_new[i][j], i, j)


def test_check1():
    flag = -12
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 5, 22, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 23, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 24, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 25, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 26, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('1'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check1(records, len_threshold=3, flag=flag)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 22, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 23, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 24, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 25, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 26, 0, 0), Decimal('0'), -12],
    ]
    # testing messages
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 3, -12, 2)',
        'Checked %s records' % len(records),
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]

    # test with something are invalid
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 5, 22, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 23, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 24, 0, 0), Decimal('0'), -13],
        [1, datetime(2001, 5, 25, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 26, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 27, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('1'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
    ]
    original_records = [r[:] for r in records]
    new_records, msgs = checks.check1(records, len_threshold=3, flag=-12)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 22, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 23, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 25, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 26, 0, 0), Decimal('0'), -12],
        [1, datetime(2001, 5, 27, 0, 0), Decimal('0'), -12],
    ]
    # testing messages
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 3, -12, 2)',
        'Checked %s records' % (len(records) - 1),
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]


def test_check2():
    flag = -13
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 5, 22, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 23, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 24, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 25, 0, 0), Decimal('1'), 1],
        [1, datetime(2001, 5, 26, 0, 0), Decimal('1'), 1],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('1'), 1],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('1'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('9.6'), -12],
        [2, datetime(2001, 5, 20, 0, 0), Decimal('9.6'), -12],
        [2, datetime(2001, 5, 21, 0, 0), Decimal('9.6'), -12],
        [2, datetime(2001, 5, 22, 0, 0), None, 1],
        [2, datetime(2001, 5, 23, 0, 0), Decimal('0'), -12],
        [2, datetime(2001, 5, 24, 0, 0), Decimal('1'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check2(
        records, len_threshold=2, flag=flag, exclude_values=(0, None))
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -13],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -13],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), -13],
        [1, datetime(2001, 5, 25, 0, 0), Decimal('1'), -13],
        [1, datetime(2001, 5, 26, 0, 0), Decimal('1'), -13],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('1'), -13],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('1'), -13],
        [2, datetime(2001, 5, 24, 0, 0), Decimal('1'), -13],
    ]
    # testing messages
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 2, -13, 2)',
        'Checked 9 records',
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]

    # try with a temperature-like set, for Tmin
    records = [
     [1, datetime(2001, 5, 17, 0, 0), Decimal('19.3'), 1, Decimal('17.2'), 1, Decimal('17.9'), 1],
     [1, datetime(2001, 5, 18, 0, 0), Decimal('22'), 1, Decimal('17.2'), 1, Decimal('18.9'), 1],
     [1, datetime(2001, 5, 19, 0, 0), Decimal('22'), 1, None, 1, Decimal('17.6'), 1],
     [1, datetime(2001, 5, 20, 0, 0), Decimal('22'), 1, Decimal('17.2'), 1, Decimal('16.3'), 1],
     [1, datetime(2001, 5, 21, 0, 0), Decimal('15.7'), 1, Decimal('11.8'), 1, Decimal('14.1'), 1],
     [1, datetime(2001, 5, 22, 0, 0), Decimal('21'), 1, Decimal('14.1'), 1, Decimal('16.5'), 1],
     [1, datetime(2001, 5, 23, 0, 0), Decimal('23.6'), 1, Decimal('14.2'), 1, Decimal('19'), 1],
     [1, datetime(2001, 5, 24, 0, 0), None, 1, Decimal('12.7'), 1, Decimal('19.8'), 1],
     [1, datetime(2001, 5, 25, 0, 0), None, 1, Decimal('12.7'), 1, Decimal('19.4'), 1],
     [1, datetime(2001, 5, 26, 0, 0), Decimal('27.3'), 1, Decimal('12.7'), 1, Decimal('20.6'), 1],
     [2, datetime(2001, 5, 17, 0, 0), Decimal('27.3'), 1, Decimal('12.7'), 1, Decimal('21.6'), 1],
     [2, datetime(2001, 5, 18, 0, 0), Decimal('27.3'), 1, Decimal('0'), 1, Decimal('24.3'), 1],
     [2, datetime(2001, 5, 19, 0, 0), Decimal('32.5'), 1, Decimal('0'), 1, Decimal('25.6'), 1],
     [2, datetime(2001, 5, 21, 0, 0), Decimal('15.7'), 1, Decimal('11.8'), 1, Decimal('14.1'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check2(
        records, len_threshold=2, flag=-13, exclude_values=(None, ), val_index=4)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(5, ))
    # testing effective found
    found = [r for r in new_records if r[5] == flag]
    assert found == [
     [1, datetime(2001, 5, 17, 0, 0), Decimal('19.3'), 1, Decimal('17.2'), -13, Decimal('17.9'), 1],
     [1, datetime(2001, 5, 18, 0, 0), Decimal('22'), 1, Decimal('17.2'), -13, Decimal('18.9'), 1],
     [1, datetime(2001, 5, 20, 0, 0), Decimal('22'), 1, Decimal('17.2'), -13, Decimal('16.3'), 1],
     [1, datetime(2001, 5, 24, 0, 0), None, 1, Decimal('12.7'), -13, Decimal('19.8'), 1],
     [1, datetime(2001, 5, 25, 0, 0), None, 1, Decimal('12.7'), -13, Decimal('19.4'), 1],
     [1, datetime(2001, 5, 26, 0, 0), Decimal('27.3'), 1, Decimal('12.7'), -13, Decimal('20.6'), 1],
     [2, datetime(2001, 5, 18, 0, 0), Decimal('27.3'), 1, Decimal('0'), -13, Decimal('24.3'), 1],
     [2, datetime(2001, 5, 19, 0, 0), Decimal('32.5'), 1, Decimal('0'), -13, Decimal('25.6'), 1],
    ]
    # testing messages
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 2, -13, 4)',
        'Checked 13 records',
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]


def test_check3():
    flag = -15
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 21, 0, 0), None, 1],
        [1, datetime(2001, 6, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 6, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 6, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 6, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 7, 16, 0, 0), Decimal('0.3'), 1],
        [1, datetime(2001, 7, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 7, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 7, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 7, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 8, 17, 0, 0), Decimal('0.1'), 1],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('0.2'), 1],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('0.3'), 1],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 9, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 9, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 9, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 9, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 10, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 10, 22, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 11, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 11, 22, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 1, 17, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 1, 18, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 1, 19, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 6, 17, 0, 0), Decimal('0.1'), 1],
        [2, datetime(2001, 6, 18, 0, 0), Decimal('0.2'), 1],
        [2, datetime(2001, 6, 19, 0, 0), Decimal('0.3'), 1],
        [2, datetime(2001, 6, 20, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 7, 16, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 7, 17, 0, 0), Decimal('0.1'), 1],
        [2, datetime(2001, 7, 18, 0, 0), Decimal('0.2'), 1],
        [2, datetime(2001, 7, 19, 0, 0), Decimal('0.3'), 1],
        [2, datetime(2001, 7, 20, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 9, 17, 0, 0), Decimal('0.1'), 1],
        [2, datetime(2001, 9, 18, 0, 0), Decimal('0.2'), 1],
        [2, datetime(2001, 9, 19, 0, 0), Decimal('0.3'), 1],
        [2, datetime(2001, 9, 20, 0, 0), Decimal('0.4'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check3(records, flag=flag, min_not_null=3)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 6, 17, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 6, 18, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 6, 19, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 6, 20, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 9, 17, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 9, 18, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 9, 19, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 9, 20, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 1, 17, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 1, 18, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 1, 19, 0, 0), Decimal('0'), -15],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), -15],
        [2, datetime(2001, 6, 17, 0, 0), Decimal('0.1'), -15],
        [2, datetime(2001, 6, 18, 0, 0), Decimal('0.2'), -15],
        [2, datetime(2001, 6, 19, 0, 0), Decimal('0.3'), -15],
        [2, datetime(2001, 6, 20, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 9, 17, 0, 0), Decimal('0.1'), -15],
        [2, datetime(2001, 9, 18, 0, 0), Decimal('0.2'), -15],
        [2, datetime(2001, 9, 19, 0, 0), Decimal('0.3'), -15],
        [2, datetime(2001, 9, 20, 0, 0), Decimal('0.4'), -15],
    ]
    # testing messages
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 3, -15, 2)',
        'Checked 44 records',
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]

    # with min_not_null=None
    new_records, msgs = checks.check3(records, flag=flag)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 6, 17, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 6, 18, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 6, 19, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 6, 20, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 9, 17, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 9, 18, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 9, 19, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 9, 20, 0, 0), Decimal('0.4'), -15],
        [1, datetime(2001, 10, 21, 0, 0), Decimal('9.6'), -15],
        [1, datetime(2001, 10, 22, 0, 0), Decimal('0'), -15],
        [1, datetime(2001, 11, 21, 0, 0), Decimal('9.6'), -15],
        [1, datetime(2001, 11, 22, 0, 0), Decimal('0'), -15],
        [2, datetime(2001, 1, 17, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 1, 18, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 1, 19, 0, 0), Decimal('0'), -15],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), -15],
        [2, datetime(2001, 6, 17, 0, 0), Decimal('0.1'), -15],
        [2, datetime(2001, 6, 18, 0, 0), Decimal('0.2'), -15],
        [2, datetime(2001, 6, 19, 0, 0), Decimal('0.3'), -15],
        [2, datetime(2001, 6, 20, 0, 0), Decimal('0.4'), -15],
        [2, datetime(2001, 9, 17, 0, 0), Decimal('0.1'), -15],
        [2, datetime(2001, 9, 18, 0, 0), Decimal('0.2'), -15],
        [2, datetime(2001, 9, 19, 0, 0), Decimal('0.3'), -15],
        [2, datetime(2001, 9, 20, 0, 0), Decimal('0.4'), -15],
    ]

    # with another index:
    records2 = [r + r[2:] for r in records]
    original_records2 = [r[:] for r in records2]
    new_records2, msgs = checks.check3(records2, flag=-15, val_index=4)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records2, new_records2, indexes_to_exclude=(5,))
    # testing effective found
    found2 = [r for r in new_records2 if r[5] == flag]
    assert found2 == [r[:3] + [1] + r[2:] for r in found]


def test_check4():
    flag = -17
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 21, 0, 0), None, 1],
        [1, datetime(2001, 6, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 6, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 6, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2002, 7, 16, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 8, 17, 0, 0), Decimal('0.1'), 1],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('0.2'), 1],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('0.3'), 1],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 10, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 10, 22, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 11, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 11, 22, 0, 0), Decimal('0'), 1],
        [1, datetime(2002, 1, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2002, 1, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2002, 1, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2002, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2002, 5, 18, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2002, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2004, 8, 17, 0, 0), Decimal('0.1'), 1],
        [1, datetime(2004, 8, 18, 0, 0), Decimal('0.2'), 1],
        [1, datetime(2004, 8, 19, 0, 0), Decimal('0.3'), 1],
        [1, datetime(2004, 8, 20, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2005, 8, 17, 0, 0), Decimal('0.1'), 1],
        [1, datetime(2005, 8, 18, 0, 0), Decimal('0.2'), 1],
        [1, datetime(2005, 8, 19, 0, 0), Decimal('0.3'), 1],
        [1, datetime(2005, 8, 20, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 5, 21, 0, 0), None, 1],
        [2, datetime(2001, 10, 21, 0, 0), Decimal('9.6'), 1],
        [2, datetime(2001, 10, 22, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 11, 21, 0, 0), Decimal('9.6'), 1],
        [2, datetime(2001, 11, 22, 0, 0), Decimal('0'), 1],
        [2, datetime(2002, 10, 21, 0, 0), Decimal('9.6'), 1],
        [2, datetime(2002, 10, 22, 0, 0), Decimal('0'), 1],
        [2, datetime(2002, 11, 21, 0, 0), Decimal('9.6'), 1],
        [2, datetime(2002, 11, 22, 0, 0), Decimal('0'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check4(records, flag=flag, min_not_null=3)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), -17],
        [1, datetime(2001, 8, 17, 0, 0), Decimal('0.1'), -17],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('0.2'), -17],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('0.3'), -17],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2002, 5, 17, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2002, 5, 18, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2002, 5, 19, 0, 0), Decimal('0'), -17],
        [1, datetime(2004, 8, 17, 0, 0), Decimal('0.1'), -17],
        [1, datetime(2004, 8, 18, 0, 0), Decimal('0.2'), -17],
        [1, datetime(2004, 8, 19, 0, 0), Decimal('0.3'), -17],
        [1, datetime(2004, 8, 20, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2005, 8, 17, 0, 0), Decimal('0.1'), -17],
        [1, datetime(2005, 8, 18, 0, 0), Decimal('0.2'), -17],
        [1, datetime(2005, 8, 19, 0, 0), Decimal('0.3'), -17],
        [1, datetime(2005, 8, 20, 0, 0), Decimal('0.4'), -17],
    ]
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 3, -17, 2)',
        'Checked 40 records',
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]

    # with min_not_null=None
    new_records, msgs = checks.check4(records, flag=flag)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records)
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), -17],
        [1, datetime(2001, 8, 17, 0, 0), Decimal('0.1'), -17],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('0.2'), -17],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('0.3'), -17],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2002, 5, 17, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2002, 5, 18, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2002, 5, 19, 0, 0), Decimal('0'), -17],
        [1, datetime(2004, 8, 17, 0, 0), Decimal('0.1'), -17],
        [1, datetime(2004, 8, 18, 0, 0), Decimal('0.2'), -17],
        [1, datetime(2004, 8, 19, 0, 0), Decimal('0.3'), -17],
        [1, datetime(2004, 8, 20, 0, 0), Decimal('0.4'), -17],
        [1, datetime(2005, 8, 17, 0, 0), Decimal('0.1'), -17],
        [1, datetime(2005, 8, 18, 0, 0), Decimal('0.2'), -17],
        [1, datetime(2005, 8, 19, 0, 0), Decimal('0.3'), -17],
        [1, datetime(2005, 8, 20, 0, 0), Decimal('0.4'), -17],
        [2, datetime(2001, 10, 21, 0, 0), Decimal('9.6'), -17],
        [2, datetime(2001, 10, 22, 0, 0), Decimal('0'), -17],
        [2, datetime(2001, 11, 21, 0, 0), Decimal('9.6'), -17],
        [2, datetime(2001, 11, 22, 0, 0), Decimal('0'), -17],
        [2, datetime(2002, 10, 21, 0, 0), Decimal('9.6'), -17],
        [2, datetime(2002, 10, 22, 0, 0), Decimal('0'), -17],
        [2, datetime(2002, 11, 21, 0, 0), Decimal('9.6'), -17],
        [2, datetime(2002, 11, 22, 0, 0), Decimal('0'), -17],
    ]


def test_check5():
    flag = -19
    records = [
     [1, datetime(2001, 5, 17, 0, 0), Decimal('19.3'), 1, Decimal('17.2'), 1, Decimal('17.9'), 1],
     [1, datetime(2001, 5, 18, 0, 0), Decimal('22'), 1, Decimal('17.2'), 1, Decimal('18.9'), 1],
     [1, datetime(2001, 5, 19, 0, 0), Decimal('22'), 1, None, 1, Decimal('22'), 1],
     [1, datetime(2001, 5, 20, 0, 0), Decimal('22.1'), 1, Decimal('22.1'), 1, Decimal('16.3'), 1],
     [1, datetime(2001, 5, 21, 0, 0), Decimal('22.2'), 1, Decimal('22.2'), 1, Decimal('14.1'), 1],
     [1, datetime(2001, 5, 22, 0, 0), Decimal('21'), 1, Decimal('21.1'), 1, Decimal('21'), 1],
     [1, datetime(2001, 5, 23, 0, 0), Decimal('23.6'), 1, None, 1, Decimal('19'), 1],
     [1, datetime(2001, 5, 24, 0, 0), None, 1, Decimal('12.7'), 1, Decimal('19.8'), 1],
     [1, datetime(2001, 5, 25, 0, 0), None, 1, Decimal('12.7'), 1, Decimal('19.4'), 1],
     [1, datetime(2001, 5, 26, 0, 0), Decimal('27.3'), 1, Decimal('27.3'), 1, Decimal('20.6'), 1],
     [1, datetime(2001, 5, 27, 0, 0), Decimal('27.4'), 1, Decimal('27.4'), 1, Decimal('20.6'), 1],
     [2, datetime(2001, 5, 17, 0, 0), Decimal('27.5'), 1, Decimal('27.5'), 1, Decimal('21.6'), 1],
     [2, datetime(2001, 5, 18, 0, 0), Decimal('27.6'), 1, Decimal('27.3'), 1, Decimal('24.3'), 1],
     [2, datetime(2001, 5, 19, 0, 0), Decimal('32.5'), 1, Decimal('32.5'), 1, Decimal('25.6'), 1],
     [2, datetime(2001, 5, 21, 0, 0), Decimal('15.7'), 1, Decimal('15.7'), 1, Decimal('14.1'), 1],
     [2, datetime(2001, 5, 22, 0, 0), Decimal('15.7'), 1, Decimal('15.7'), 1, Decimal('14.1'), 1],
     [2, datetime(2001, 5, 23, 0, 0), Decimal('15.7'), 1, Decimal('15.7'), -1, Decimal('14.1'), 1],
     [2, datetime(2001, 5, 24, 0, 0), Decimal('15.7'), -1, Decimal('15.7'), 1, Decimal('14.1'), 1],
     [2, datetime(2001, 5, 25, 0, 0), Decimal('15.7'), 1, Decimal('15.7'), 1, Decimal('14.1'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check5(records, len_threshold=2, flag=flag)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, 5))
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
     [1, datetime(2001, 5, 20, 0, 0), Decimal('22.1'), -19, Decimal('22.1'), -19, Decimal('16.3'), 1],
     [1, datetime(2001, 5, 21, 0, 0), Decimal('22.2'), -19, Decimal('22.2'), -19, Decimal('14.1'), 1],
     [1, datetime(2001, 5, 26, 0, 0), Decimal('27.3'), -19, Decimal('27.3'), -19, Decimal('20.6'), 1],
     [1, datetime(2001, 5, 27, 0, 0), Decimal('27.4'), -19, Decimal('27.4'), -19, Decimal('20.6'), 1],
     [2, datetime(2001, 5, 19, 0, 0), Decimal('32.5'), -19, Decimal('32.5'), -19, Decimal('25.6'), 1],
     [2, datetime(2001, 5, 21, 0, 0), Decimal('15.7'), -19, Decimal('15.7'), -19, Decimal('14.1'), 1],
     [2, datetime(2001, 5, 22, 0, 0), Decimal('15.7'), -19, Decimal('15.7'), -19, Decimal('14.1'), 1],
     [2, datetime(2001, 5, 25, 0, 0), Decimal('15.7'), -19, Decimal('15.7'), -19, Decimal('14.1'), 1],
    ]
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: 2, -19)',
        'Checked 15 records',
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]


def test_check6():
    flag = -20
    records = [
     [1, datetime(2001, 5, 17, 0, 0), Decimal('0'), 1, Decimal('17.2'), 1, Decimal('17.9'), 1],
     [1, datetime(2001, 5, 18, 0, 0), Decimal('22'), 1, Decimal('0'), 1, Decimal('18.9'), 1],
     [1, datetime(2001, 5, 19, 0, 0), Decimal('22'), 1, None, 1, Decimal('22'), 1],
     [1, datetime(2001, 5, 20, 0, 0), Decimal('0'), 1, Decimal('0'), 1, Decimal('16.3'), 1],
     [1, datetime(2001, 5, 21, 0, 0), Decimal('0'), 1, Decimal('0'), 1, Decimal('14.1'), 1],
     [1, datetime(2001, 5, 22, 0, 0), Decimal('21'), 1, Decimal('21.1'), 1, Decimal('21'), 1],
     [1, datetime(2001, 5, 23, 0, 0), Decimal('23.6'), 1, None, 1, Decimal('19'), 1],
     [1, datetime(2001, 5, 24, 0, 0), None, 1, Decimal('12.7'), 1, Decimal('19.8'), 1],
     [1, datetime(2001, 5, 25, 0, 0), None, 1, Decimal('12.7'), 1, Decimal('19.4'), 1],
     [1, datetime(2001, 5, 26, 0, 0), Decimal('0'), 1, Decimal('0'), -1, Decimal('20.6'), 1],
     [1, datetime(2001, 5, 27, 0, 0), Decimal('27.4'), 1, Decimal('27.4'), 1, Decimal('20.6'), 1],
     [2, datetime(2001, 5, 17, 0, 0), Decimal('27.5'), 1, Decimal('27.5'), 1, Decimal('21.6'), 1],
     [2, datetime(2001, 5, 18, 0, 0), Decimal('27.6'), 1, Decimal('27.3'), 1, Decimal('24.3'), 1],
     [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1, Decimal('0'), 1, Decimal('25.6'), 1],
     [2, datetime(2001, 5, 21, 0, 0), Decimal('0'), 1, Decimal('0'), 1, Decimal('14.1'), 1],
    ]
    original_records = [r[:] for r in records]

    new_records, msgs = checks.check6(records, flag=flag)

    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, 5))
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 20, 0, 0), Decimal('0'), -20, Decimal('0'), -20, Decimal('16.3'), 1],
        [1, datetime(2001, 5, 21, 0, 0), Decimal('0'), -20, Decimal('0'), -20, Decimal('14.1'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('0'), -20, Decimal('0'), -20, Decimal('25.6'), 1],
        [2, datetime(2001, 5, 21, 0, 0), Decimal('0'), -20, Decimal('0'), -20, Decimal('14.1'), 1],
    ]
    num_found = len(found)
    assert msgs == [
        'starting check (parameters: -20)',
        'Checked 15 records',
        'Found %s records with flags reset to %s' % (num_found, flag),
        'Check completed'
    ]


def test_check7():
    flag = -21
    records = [
     [1, datetime(2001, 5, 17, 0, 0), Decimal('0'), 1, Decimal('17.2'), 1, Decimal('17.9'), 1],
     [1, datetime(2001, 5, 18, 0, 0), Decimal('14'), 1, Decimal('0'), 1, Decimal('18.9'), 1],
     [1, datetime(2001, 5, 19, 0, 0), Decimal('16'), 1, None, 1, Decimal('22'), 1],
     [1, datetime(2001, 5, 20, 0, 0), Decimal('15.1'), 1, Decimal('0'), 1, Decimal('16.3'), 1],
     [1, datetime(2001, 5, 21, 0, 0), Decimal('-4'), 1, Decimal('0'), 1, Decimal('14.1'), 1],
     [1, datetime(2001, 5, 22, 0, 0), Decimal('-4'), -1, Decimal('0'), 1, Decimal('14.1'), 1],
    ]
    original_records = [r[:] for r in records]

    # only min
    new_records, msgs = checks.check7(records, min_threshold=15, flag=flag, val_index=2)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, ))
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0'), -21, Decimal('17.2'), 1, Decimal('17.9'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('14'), -21, Decimal('0'), 1, Decimal('18.9'), 1],
        [1, datetime(2001, 5, 21, 0, 0), Decimal('-4'), -21, Decimal('0'), 1, Decimal('14.1'), 1],
    ]

    # only max
    new_records, msgs = checks.check7(records, max_threshold=15, flag=-21, val_index=2)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, ))
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2001, 5, 19, 0, 0), Decimal('16'), -21, None, 1, Decimal('22'), 1],
        [1, datetime(2001, 5, 20, 0, 0), Decimal('15.1'), -21, Decimal('0'), 1, Decimal('16.3'), 1],
    ]

    # min and max
    new_records, msgs = checks.check7(
        records, min_threshold=-10, max_threshold=10, flag=-21, val_index=2)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, ))
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
     [1, datetime(2001, 5, 18, 0, 0), Decimal('14'), -21, Decimal('0'), 1, Decimal('18.9'), 1],
     [1, datetime(2001, 5, 19, 0, 0), Decimal('16'), -21, None, 1, Decimal('22'), 1],
     [1, datetime(2001, 5, 20, 0, 0), Decimal('15.1'), -21, Decimal('0'), 1, Decimal('16.3'), 1],
    ]

    # change index
    new_records, msgs = checks.check7(
        records, min_threshold=-10, max_threshold=10, flag=-21, val_index=4)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(5, ))
    # testing effective found
    found = [r for r in new_records if r[5] == flag]
    assert found == [
     [1, datetime(2001, 5, 17, 0, 0), Decimal('0'), 1, Decimal('17.2'), -21, Decimal('17.9'), 1],
    ]
    assert msgs == [
        'starting check (parameters: -10, 10, -21, 4)',
        'Checked 5 records',
        'Found 1 records with flags reset to -21',
        'Check completed',
    ]


def test_gap_top_checks():
    terms = [3, 4, 6, 1, 0, 11, 0, 1, 12, 130, 131]
    threshold = checks.gap_top_checks(terms, 4)
    assert threshold == 11
    threshold = checks.gap_top_checks(terms, 40)
    assert threshold == 130
    threshold = checks.gap_top_checks(terms, 150)
    assert threshold == math.inf
    assert checks.gap_top_checks([], 150) == math.inf


def test_gap_bottom_checks():
    terms = [3, 4, -6, 1, 0, -3, 11, 0, 1, 12, 130, 131]
    threshold = checks.gap_bottom_checks(terms, 4)
    assert threshold == 12
    threshold = checks.gap_bottom_checks(terms, 110)
    assert threshold == 12
    threshold = checks.gap_bottom_checks(terms, 120)
    assert threshold == -math.inf
    assert checks.gap_bottom_checks([], 120) == -math.inf


def test_check8():
    flag_sup = -23
    flag_inf = -24
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.5'), 1],
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 5, 21, 0, 0), None, 1],
        [1, datetime(2001, 6, 17, 0, 0), Decimal('3'), 1],
        [1, datetime(2001, 6, 18, 0, 0), Decimal('8'), 1],
        [1, datetime(2001, 6, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2002, 7, 16, 0, 0), Decimal('0'), 1],
        [1, datetime(2001, 8, 17, 0, 0), Decimal('22'), 1],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('23'), 1],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('24'), 1],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('25'), 1],
        [1, datetime(2001, 10, 21, 0, 0), Decimal('9.6'), 1],
        [1, datetime(2001, 10, 22, 0, 0), Decimal('13'), 1],
        [1, datetime(2001, 11, 21, 0, 0), Decimal('1'), 1],
        [1, datetime(2001, 11, 22, 0, 0), Decimal('-3'), 1],
        [1, datetime(2002, 1, 17, 0, 0), Decimal('1'), 1],
        [1, datetime(2002, 1, 18, 0, 0), Decimal('-2'), 1],
        [1, datetime(2002, 1, 19, 0, 0), Decimal('-21'), 1],
        [1, datetime(2002, 5, 17, 0, 0), Decimal('4.4'), 1],
        [1, datetime(2002, 5, 18, 0, 0), Decimal('8'), 1],
        [1, datetime(2002, 5, 19, 0, 0), Decimal('0'), 1],
        [1, datetime(2004, 8, 17, 0, 0), Decimal('1'), 1],
        [1, datetime(2004, 8, 18, 0, 0), Decimal('2'), 1],
        [1, datetime(2004, 8, 19, 0, 0), Decimal('3'), 1],
        [1, datetime(2004, 8, 20, 0, 0), Decimal('4'), 1],
        [1, datetime(2005, 8, 17, 0, 0), Decimal('1'), 1],
        [1, datetime(2005, 8, 18, 0, 0), Decimal('2'), 1],
        [1, datetime(2005, 8, 19, 0, 0), Decimal('3'), 1],
        [1, datetime(2005, 8, 20, 0, 0), Decimal('4'), 1],
        [2, datetime(2001, 5, 17, 0, 0), Decimal('22'), 1],
        [2, datetime(2001, 5, 18, 0, 0), Decimal('24'), 1],
        [2, datetime(2001, 5, 19, 0, 0), Decimal('20'), 1],
        [2, datetime(2001, 5, 21, 0, 0), None, 1],
        [2, datetime(2001, 10, 21, 0, 0), Decimal('9'), 1],
        [2, datetime(2001, 10, 22, 0, 0), Decimal('0'), 1],
        [2, datetime(2001, 11, 21, 0, 0), Decimal('3'), 1],
        [2, datetime(2001, 11, 22, 0, 0), Decimal('4'), 1],
        [2, datetime(2002, 10, 21, 0, 0), Decimal('11'), 1],
        [2, datetime(2002, 10, 22, 0, 0), Decimal('-1'), 1],
        [2, datetime(2002, 11, 21, 0, 0), Decimal('33'), 1],
        [2, datetime(2002, 11, 22, 0, 0), Decimal('0'), 1],
        [2, datetime(2002, 11, 23, 0, 0), Decimal('33'), 1],
        [2, datetime(2002, 11, 25, 0, 0), Decimal('34'), 1],
    ]
    original_records = [r[:] for r in records]

    # split = False (case precipitation)
    new_records, msgs = checks.check8(
        records, threshold=10, split=False, flag_sup=flag_sup, flag_inf=flag_inf, val_index=2)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, ))
    # testing effective found
    found_sup = [r for r in new_records if r[3] == flag_sup]
    assert found_sup == [
        [1, datetime(2001, 8, 17, 0, 0), Decimal('22'), -23],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('23'), -23],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('24'), -23],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('25'), -23],
        [1, datetime(2002, 1, 17, 0, 0), Decimal('1'), -23],
        [1, datetime(2002, 1, 18, 0, 0), Decimal('-2'), -23],
        [2, datetime(2002, 11, 21, 0, 0), Decimal('33'), -23],
        [2, datetime(2002, 11, 23, 0, 0), Decimal('33'), -23],
        [2, datetime(2002, 11, 25, 0, 0), Decimal('34'), -23],
    ]
    found_inf = [r for r in new_records if r[3] == flag_inf]
    assert found_inf == []
    assert msgs == [
        'starting check (parameters: 10, False, -23, -24, 2)',
        'Checked 42 records',
        'Found 9 records with flags reset to -23',
        'Found 0 records with flags reset to -24',
        'Check completed'
    ]

    # test split = True (temperature)
    new_records, msgs = checks.check8(
        records, threshold=10, split=True, flag_sup=-23, flag_inf=-24, val_index=2)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3, ))
    # testing effective found
    found_sup = [r for r in new_records if r[3] == flag_sup]
    assert found_sup == [
        [1, datetime(2001, 8, 17, 0, 0), Decimal('22'), -23],
        [1, datetime(2001, 8, 18, 0, 0), Decimal('23'), -23],
        [1, datetime(2001, 8, 19, 0, 0), Decimal('24'), -23],
        [1, datetime(2001, 8, 20, 0, 0), Decimal('25'), -23],

    ]
    found_inf = [r for r in new_records if r[3] == flag_inf]
    assert found_inf == [
        [1, datetime(2002, 1, 19, 0, 0), Decimal('-21'), -24]
    ]
    assert msgs == [
        'starting check (parameters: 10, True, -23, -24, 2)',
        'Checked 42 records',
        'Found 4 records with flags reset to -23',
        'Found 1 records with flags reset to -24',
        'Check completed',
    ]


def test_check9():
    flag = -25
    records = [
        [1, datetime(2001, 5, 17, 0, 0), Decimal('0.4'), 1],  # nc: sample 9
        [1, datetime(2001, 5, 18, 0, 0), Decimal('0.5'), 1],  # ok: sample 10 -> ok
        [1, datetime(2001, 5, 19, 0, 0), Decimal('0'), 1],  # ok:sample > 10 -> ok
        [1, datetime(2001, 5, 20, 0, 0), None, 1],  # ok: sample 11 -> ok
        [1, datetime(2001, 5, 21, 0, 0), Decimal('-1'), 1],  # nc: sample 7
        [1, datetime(2001, 5, 22, 0, 0), Decimal('-33'), -1],  # nc: sample 6

        [1, datetime(2001, 6, 17, 0, 0), Decimal('3'), 1],
        [1, datetime(2001, 6, 18, 0, 0), Decimal('8'), 1],
        [1, datetime(2001, 6, 19, 0, 0), Decimal('0'), 1],

        [1, datetime(2002, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2002, 5, 18, 0, 0), Decimal('5'), 1],  # ok
        [1, datetime(2002, 5, 19, 0, 0), Decimal('0'), 1],  # ok
        [1, datetime(2002, 5, 20, 0, 0), None, 1],
        [1, datetime(2002, 5, 21, 0, 0), Decimal('4'), 1],
        [1, datetime(2002, 5, 22, 0, 0), Decimal('-0.4'), -1],

        [1, datetime(2003, 5, 17, 0, 0), Decimal('0.4'), 1],
        [1, datetime(2003, 5, 18, 0, 0), Decimal('33'), 1],  # no
        [1, datetime(2003, 5, 19, 0, 0), Decimal('2'), 1],  # ok
        [1, datetime(2003, 5, 20, 0, 0), Decimal('2'), 1],
        [1, datetime(2003, 5, 21, 0, 0), Decimal('4'), 1],
        [1, datetime(2003, 5, 22, 0, 0), Decimal('25'), -1],
    ]
    original_records = [r[:] for r in records]
    # import pdb; pdb.set_trace()
    new_records, msgs = checks.check9(
        records, num_dev_std=2, window_days=5, min_num=10, flag=-25, val_index=2)
    # test no change in-place
    assert records == original_records
    # test preserving order and other values
    compare_noindexes(records, new_records, indexes_to_exclude=(3,))
    # testing effective found
    found = [r for r in new_records if r[3] == flag]
    assert found == [
        [1, datetime(2003, 5, 18, 0, 0), Decimal('33'), -25],
    ]
    assert msgs == [
        'starting check (parameters: 2, 5, 10, -25, 2)',
        'Checked 16 records',
        'Found 1 records with flags reset to -25',
        'Check completed',
    ]