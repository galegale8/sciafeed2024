
from datetime import datetime
from os.path import join, exists

from sciafeed import arpa19, arpa21, arpaer, arpafvg, rmn
from sciafeed import formats

from . import TEST_DATA_PATH


def test_guess_format(tmpdir):
    # arpa19
    test_filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
    label, module = formats.guess_format(test_filepath)
    assert label, module == ('ARPA-19', arpa19)
    # arpa21
    test_filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    label, module = formats.guess_format(test_filepath)
    assert label, module == ('ARPA-21', arpa21)
    # arpaer
    test_filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    label, module = formats.guess_format(test_filepath)
    assert label, module == ('ARPA-ER', arpaer)
    # arpafvg
    test_filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    label, module = formats.guess_format(test_filepath)
    assert label, module == ('ARPA-FVG', arpafvg)
    # rmn
    test_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    label, module = formats.guess_format(test_filepath)
    assert label, module == ('RMN', rmn)
    # unknown
    test_filepath = str(tmpdir.join('loc01_00001_2018010101_2019010101.dat'))
    with open(test_filepath, 'w') as fp:
        fp.write("Hello, I'm an unknown format")
    label, module = formats.guess_format(test_filepath)
    assert label, module == ('Unknown', None)


def test_make_report(tmpdir):
    # ------------  arpa19 ------------
    parameters_filepath = join(TEST_DATA_PATH, 'arpa19', 'arpa19_params.csv')

    # no errors
    in_filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')
    limiting_params = {'3': ('4', '5')}
    out_filepath = str(tmpdir.join('report.txt'))
    outdata_filepath = str(tmpdir.join('data.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    assert "No errors found" in msgs

    # some formatting errors
    in_filepath = join(TEST_DATA_PATH, 'arpa19', 'wrong_70001_201301010000_201401010100.dat')
    limiting_params = {'3': ('4', '5')}
    out_filepath = str(tmpdir.join('report2.txt'))
    outdata_filepath = str(tmpdir.join('data2.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        "Row 2: The spacing in the row is wrong",
        'Row 3: the latitude changes',
        'Row 5: it is not strictly after the previous',
        'Row 21: duplication of rows with different data',
        'Row 22: the time is not coherent with the filename',
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs

    # some errors
    in_filepath = join(TEST_DATA_PATH, 'arpa19', 'wrong_70002_201301010000_201401010100.dat')
    limiting_params = {'3': ('1', '2')}
    out_filepath = str(tmpdir.join('report3.txt'))
    outdata_filepath = str(tmpdir.join('data3.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        "Row 1: The value of '1' is out of range [0.0, 1020.0]",
        "Row 2: The value of '2' is out of range [0.0, 360.0]",
        "Row 3: The value of '3' is out of range [-350.0, 450.0]",
        "Row 5: The values of '3' and '2' are not consistent",
        "Row 6: The values of '3' and '2' are not consistent",
        "Row 7: The values of '3' and '2' are not consistent",
        "Row 10: The values of '3' and '2' are not consistent",
        "Row 20: The values of '3' and '2' are not consistent"
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs
    assert data_parsed == ('70002', 43.876999, {
        datetime(2013, 1, 1, 0, 0): {
            '1': (2000.0, False),
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
            '9': (83.0, True)},
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
            '2': (361.0, False),
            '3': (65.0, True),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)},
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
            '3': (-351.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (86.0, True)},
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
            '9': (87.0, True)},
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
            '3': (64.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (88.0, True)},
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
            '3': (67.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (89.0, True)},
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
            '3': (65.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (93.0, True)},
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
            '9': (93.0, True)},
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
            '9': (95.0, True)},
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
            '3': (106.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (88.0, True)},
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
            '9': (72.0, True)},
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
            '9': (69.0, True)},
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
            '9': (73.0, True)},
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
            '9': (74.0, True)},
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
            '9': (78.0, True)},
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
            '9': (82.0, True)},
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
            '9': (96.0, True)},
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
            '9': (96.0, True)},
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
            '9': (97.0, True)},
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
            '3': (99.0, False),
            '4': (None, False),
            '5': (None, False),
            '6': (None, False),
            '7': (None, False),
            '8': (None, False),
            '9': (100.0, True)}
        }
    )

    # ------------  arpa21 ------------
    parameters_filepath = join(TEST_DATA_PATH, 'arpa21', 'arpa21_params.csv')

    # no errors
    in_filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')
    limiting_params = {'3': ('4', '5')}
    out_filepath = str(tmpdir.join('report4.txt'))
    outdata_filepath = str(tmpdir.join('data4.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    assert "No errors found" in msgs

    # some formatting errors
    in_filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00201_201201010000_201301010100.dat')
    limiting_params = {'3': ('4', '5')}
    out_filepath = str(tmpdir.join('report5.txt'))
    outdata_filepath = str(tmpdir.join('data5.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        "Row 2: The spacing in the row is wrong",
        "Row 3: the latitude changes",
        "Row 5: it is not strictly after the previous",
        "Row 21: duplication of rows with different data",
        "Row 22: the time is not coherent with the filename",
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs

    # some errors
    in_filepath = join(TEST_DATA_PATH, 'arpa21', 'wrong_00202_201201010000_201301010100.dat')
    limiting_params = {'9': ('10', '4')}
    out_filepath = str(tmpdir.join('report6.txt'))
    outdata_filepath = str(tmpdir.join('data6.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        "Row 1: The value of '3' is out of range [-350.0, 450.0]",
        "Row 1: The values of '9' and '4' are not consistent",
        "Row 2: The value of '4' is out of range [-400.0, 400.0]",
        "Row 3: The value of '5' is out of range [-300.0, 500.0]",
        "Row 3: The values of '9' and '4' are not consistent",
        "Row 4: The values of '9' and '4' are not consistent",
        "Row 5: The values of '9' and '4' are not consistent",
        "Row 6: The values of '9' and '4' are not consistent",
        "Row 7: The values of '9' and '4' are not consistent",
        "Row 8: The values of '9' and '4' are not consistent",
        "Row 9: The values of '9' and '4' are not consistent",
        "Row 20: The values of '9' and '4' are not consistent",
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs
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
            '9': (100.0, False)}
    })

    # ------------  arpafvg ------------
    parameters_filepath = join(TEST_DATA_PATH, 'arpafvg', 'arpafvg_params.csv')

    # no errors
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')
    limiting_params = {'Tmedia': ('FF', 'DD')}  # 2: 6:5
    out_filepath = str(tmpdir.join('report7.txt'))
    outdata_filepath = str(tmpdir.join('data7.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    assert "No errors found" in msgs

    # some formatting errors
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00001_2018010101_2019010101.dat')
    limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
    out_filepath = str(tmpdir.join('report8.txt'))
    outdata_filepath = str(tmpdir.join('data8.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        'Row 1: The number of components in the row is wrong',
        'Row 3: duplication of rows with different data',
        'Row 4: the latitude changes',
        'Row 6: it is not strictly after the previous',
        'Row 7: the time is not coherent with the filename',
        "Row 2: The values of 'PREC' and 'Bagnatura_f' are not consistent",
        "Row 5: The values of 'PREC' and 'Bagnatura_f' are not consistent",
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs

    # some errors
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'wrong_00002_2018010101_2019010101.dat')
    limiting_params = {'PREC': ('Bagnatura_f', 'DD')}  # 1: 4:5
    out_filepath = str(tmpdir.join('report9.txt'))
    outdata_filepath = str(tmpdir.join('data9.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        "Row 1: The value of 'FF' is out of range [0.0, 102.0]",
        "Row 1: The values of 'PREC' and 'Bagnatura_f' are not consistent",
        "Row 2: The value of 'DD' is out of range [0.0, 360.0]",
        "Row 2: The values of 'PREC' and 'Bagnatura_f' are not consistent",
        "Row 3: The value of 'PREC' is out of range [0.0, 989.0]",
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs
    assert data_parsed == ('00002', 46.077222, {
        datetime(2018, 1, 1, 1, 0): {
            'Bagnatura_f': (58.0, True),
            'DD': (357.0, True),
            'FF': (103.0, False),
            'INSOL': (0.0, True),
            'PREC': (0.0, False),
            'Pstaz': (1001.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (2.8, True),
            'UR media': (86.0, True)},
        datetime(2018, 1, 1, 2, 0): {
            'Bagnatura_f': (59.0, True),
            'DD': (361.0, False),
            'FF': (1.6, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, False),
            'Pstaz': (1001.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.1, True),
            'UR media': (85.0, True)},
        datetime(2018, 1, 1, 3, 0): {
            'Bagnatura_f': (39.0, True),
            'DD': (345.0, True),
            'FF': (1.2, True),
            'INSOL': (0.0, True),
            'PREC': (1000.0, False),
            'Pstaz': (1000.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (82.0, True)},
        datetime(2018, 1, 1, 4, 0): {
            'Bagnatura_f': (0.0, True),
            'DD': (313.0, True),
            'FF': (1.8, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (999.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (82.0, True)},
        datetime(2018, 1, 1, 5, 0): {
            'Bagnatura_f': (0.0, True),
            'DD': (348.0, True),
            'FF': (0.9, True),
            'INSOL': (0.0, True),
            'PREC': (0.0, True),
            'Pstaz': (998.0, True),
            'RADSOL': (0.0, True),
            'Tmedia': (3.4, True),
            'UR media': (83.0, True)}
    })

    # ------------  rmn ------------
    parameters_filepath = join(TEST_DATA_PATH, 'rmn', 'rmn_params.csv')

    # no errors
    in_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')
    limiting_params = {'Tmedia': ('FF', 'UR media')}
    out_filepath = str(tmpdir.join('report10.txt'))
    outdata_filepath = str(tmpdir.join('data10.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    assert "No errors found" in msgs

    # some formatting errors
    in_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong3.csv')
    out_filepath = str(tmpdir.join('report11.txt'))
    outdata_filepath = str(tmpdir.join('data11.csv'))
    assert not exists(out_filepath)
    assert not exists(outdata_filepath)
    msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    assert exists(out_filepath)
    assert exists(outdata_filepath)
    err_msgs = [
        'Row 4: the reference time for the row is not parsable',
        'Row 16: the row is duplicated with different values',
        'Row 17: the row is not strictly after the previous',
        "Row 23: the value '180gradi' is not numeric"
    ]
    for err_msg in err_msgs:
        assert err_msg in msgs

    # some errors
    limiting_params = {'Tmedia': ('UR media', 'DD')}
    in_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_wrong5.csv')
    out_filepath = str(tmpdir.join('report12.txt'))
    outdata_filepath = str(tmpdir.join('data12.csv'))
    err_msgs, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath, parameters_filepath=parameters_filepath,
        limiting_params=limiting_params)
    expected_msgs = [
        'Row 10: the row is not strictly after the previous',
        "Row 4: The value of 'DD' is out of range [0.0, 360.0]",
        "Row 4: The values of 'Tmedia' and 'UR media' are not consistent",
        "Row 5: The value of 'FF' is out of range [0.0, 102.0]",
        "Row 6: The value of 'Tmedia' is out of range [-35.0, 45.0]",
        "Row 16: The values of 'Tmedia' and 'UR media' are not consistent",
        "Row 22: The values of 'Tmedia' and 'UR media' are not consistent",
        "Row 28: The values of 'Tmedia' and 'UR media' are not consistent",
        "Row 34: The values of 'Tmedia' and 'UR media' are not consistent",
        "Row 40: The values of 'Tmedia' and 'UR media' are not consistent",
    ]
    for err_msg in expected_msgs:
        assert err_msg in err_msgs
    assert data_parsed == ('ANCONA', {
        datetime(2018, 1, 1, 0, 0): {
            'DD': (361.0, False),
            'FF': (1.9, True),
            'P': (1018.1, True),
            'Tmedia': (7.2, False),
            'UR media': (63.0, True)},
        datetime(2018, 1, 1, 0, 10): {
            'DD': (180.0, True),
            'FF': (-1.6, False),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 20): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P': (None, True),
            'Tmedia': (47.0, False),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 30): {
            'DD': (180.0, True),
            'FF': (0.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 40): {
            'DD': (180.0, True),
            'FF': (0.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 0, 50): {
            'DD': (180.0, True),
            'FF': (0.8, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 10): {
            'DD': (180.0, True),
            'FF': (1.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 20): {
            'DD': (180.0, True),
            'FF': (1.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 30): {
            'DD': (180.0, True),
            'FF': (3.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 40): {
            'DD': (180.0, True),
            'FF': (2.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 1, 50): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 0): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (1016.9, True),
            'Tmedia': (9.0, False),
            'UR media': (58.0, True)},
        datetime(2018, 1, 1, 2, 10): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 20): {
            'DD': (180.0, True),
            'FF': (3.7, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 30): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 40): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 2, 50): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 0): {
            'DD': (180.0, True),
            'FF': (3.9, True),
            'P': (1016.2, True),
            'Tmedia': (8.7, False),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 3, 10): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 20): {
            'DD': (180.0, True),
            'FF': (4.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 30): {
            'DD': (180.0, True),
            'FF': (4.2, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 40): {
            'DD': (180.0, True),
            'FF': (4.1, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 3, 50): {
            'DD': (180.0, True),
            'FF': (4.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 0): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (1015.2, True),
            'Tmedia': (10.1, False),
            'UR media': (59.0, True)},
        datetime(2018, 1, 1, 4, 10): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 20): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 30): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 40): {
            'DD': (180.0, True),
            'FF': (5.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 4, 50): {
            'DD': (180.0, True),
            'FF': (5.4, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 0): {
            'DD': (180.0, True),
            'FF': (5.8, True),
            'P': (1014.3, True),
            'Tmedia': (9.7, False),
            'UR media': (62.0, True)},
        datetime(2018, 1, 1, 5, 10): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 20): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 30): {
            'DD': (180.0, True),
            'FF': (5.0, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 40): {
            'DD': (180.0, True),
            'FF': (5.3, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 5, 50): {
            'DD': (180.0, True),
            'FF': (4.5, True),
            'P': (None, True),
            'Tmedia': (None, True),
            'UR media': (None, True)},
        datetime(2018, 1, 1, 6, 0): {
            'DD': (180.0, True),
            'FF': (4.6, True),
            'P': (1014.1, True),
            'Tmedia': (9.5, False),
            'UR media': (64.0, True)}
    })

    # ------------  arpaer ------------
    in_filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    limiting_params = dict()
    out_filepath = str(tmpdir.join('report13.txt'))
    outdata_filepath = str(tmpdir.join('data13.csv'))
    report_strings, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath,
        parameters_filepath, limiting_params)
    expected_rows = [
        "Row 1: The value of 'UR media' is out of range [20.0, 100.0]",
    ]
    for row in expected_rows:
        assert row in report_strings

    in_filepath = join(TEST_DATA_PATH, 'arpaer', 'wrong_results1.json')
    report_strings, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath,
        parameters_filepath, limiting_params)
    expected_rows = [
        'Row 2: information of the station is not parsable',
        'Row 3: information of the date is wrong',
        "Row 1: The value of 'UR media' is out of range [20.0, 100.0]",
    ]
    for row in expected_rows:
        assert row in report_strings

    # ------------  unknown ------------
    in_filepath = str(tmpdir.join('loc01_00001_2018010101_2019010101.dat'))
    with open(in_filepath, 'w') as fp:
        fp.write("Hello, I'm an unknown format")
    report_strings, data_parsed = formats.make_report(
        in_filepath, out_filepath, outdata_filepath,
        parameters_filepath, limiting_params)
    assert report_strings == ["file %r has unknown format" % in_filepath, '']
    assert data_parsed is None
