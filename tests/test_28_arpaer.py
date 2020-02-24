
from datetime import datetime
import json
from os.path import join, exists

from sciafeed import arpaer
from . import TEST_DATA_PATH

from pprint import pprint


class DummyResponseObject:
    dummy_json = {
        'result': {
            'fields': [{'id': '_id', 'type': 'int4'},
                       {'id': '_full_text', 'type': 'tsvector'},
                       {'id': 'ident', 'type': 'text'},
                       {'id': 'lon', 'type': 'int4'},
                       {'id': 'lat', 'type': 'int4'},
                       {'id': 'network', 'type': 'text'},
                       {'id': 'date', 'type': 'timestamp'},
                       {'id': 'data', 'type': 'json'},
                       {'id': 'version', 'type': 'text'}],
            'records': [{'_full_text': "'-02':5 '-06':6 '0':13,20,32,50,51 "
                                       "'0.1':2 '00':8 '00z':9 '1':57 "
                                       "'1.22':55 '2':26 '2020':4,23 "
                                       "'240.0':38,41 '254':49 '44.9025':47 "
                                       "'4490250':3 '6':29 '7.68972':35 "
                                       "'768972':61 'b01019':14 'b01194':42 "
                                       "'b04001':21 'b04002':24 'b04003':27 "
                                       "'b04004':11 'b04005':30 'b04006':18 "
                                       "'b05001':45 'b06001':33 'b07030':36 "
                                       "'b07031':39 'b13215':53 'carignano':16 "
                                       "'fidupo':1,44 'level':56 "
                                       "'null':58,59,60 'po':17 't00':7 "
                                       "'timerang':48 "
                                       "'v':12,15,19,22,25,28,31,34,37,40,43,46,54 "
                                       "'var':10,52",
                         '_id': 18718272,
                         'data': [{'vars': {'B01019': {'v': 'Carignano Po'},
                                            'B01194': {'v': 'fidupo'},
                                            'B04001': {'v': 2020},
                                            'B04002': {'v': 2},
                                            'B04003': {'v': 6},
                                            'B04004': {'v': 0},
                                            'B04005': {'v': 0},
                                            'B04006': {'v': 0},
                                            'B05001': {'v': 44.9025},
                                            'B06001': {'v': 7.68972},
                                            'B07030': {'v': 240.0},
                                            'B07031': {'v': 240.0}}},
                                  {'level': [1, None, None, None],
                                   'timerange': [254, 0, 0],
                                   'vars': {'B13215': {'v': 1.22}}}],
                         'date': '2020-02-06T00:00:00',
                         'ident': None,
                         'lat': 4490250,
                         'lon': 768972,
                         'network': 'fidupo',
                         'version': '0.1'}],
            },
        'success': True,
    }
    dummy_json_error = {
        'error': {
            '__type': 'Validation Error',
            'info': {
                'orig': ['error description'],
                'params': [{}],
            },
            'query': ['(ProgrammingError) ...']
        },
        'success': False
    }

    dummy_json_3 = {
        'result': {'records': [
            {'_id': 18716105,
             'data': [
                 {'vars': {
                    'B01019': {'v': "San Nicolo'"},
                    'B01194': {'v': 'agrmet'},
                    'B04001': {'v': 2020},
                    'B04002': {'v': 2},
                    'B04003': {'v': 6},
                    'B04004': {'v': 0},
                    'B04005': {'v': 0},
                    'B04006': {'v': 0},
                    'B05001': {'v': 45.04139},
                    'B06001': {'v': 9.58959},
                    'B07030': {'v': 68.0},
                    'B07031': {'v': 68.0}}},
                 {'level': [1, None, None, None],
                  'timerange': [1, 0, 900],
                  'vars': {'B13011': {'v': 0.0}}},
                 {'level': [1, None, None, None],
                  'timerange': [1, 0, 3600],
                  'vars': {'B13011': {'v': 0.0}}},
                 {'level': [1, None, None, None],
                  'timerange': [1, 0, 86400],
                  'vars': {'B13011': {'v': 0.0}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [0, 0, 3600],
                  'vars': {'B12101': {'v': 274.45},
                           'B13003': {'v': 56}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [0, 0, 86400],
                  'vars': {'B12101': {'v': 280.38},
                           'B13003': {'v': 38}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [2, 0, 3600],
                  'vars': {'B12101': {'v': 275.65},
                           'B13003': {'v': 65}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [2, 0, 86400],
                  'vars': {'B12101': {'v': 286.85},
                           'B13003': {'v': 70}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [3, 0, 3600],
                  'vars': {'B12101': {'v': 272.65},
                           'B13003': {'v': 44}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [3, 0, 86400],
                  'vars': {'B12101': {'v': 272.55},
                           'B13003': {'v': 14}}},
                 {'level': [103, 2000, None, None],
                  'timerange': [254, 0, 0],
                  'vars': {'B12101': {'v': 273.65},
                           'B13003': {'v': 55}}},
                 {'level': [106, 250, None, None],
                  'timerange': [254, 0, 0],
                  'vars': {'B13227': {'v': 43.7}}},
                 {'level': [106, 450, None, None],
                  'timerange': [254, 0, 0],
                  'vars': {'B13227': {'v': 44.1}}},
                 {'level': [106, 700, None, None],
                  'timerange': [254, 0, 0],
                  'vars': {'B13227': {'v': 43.0}}}],
             'date': '2020-02-06T00:00:00',
             'ident': None,
             'lat': 4504139,
             'lon': 958959,
             'network': 'agrmet',
             'version': '0.1'
             },
            {'_id': 18718208,
             'data': [
                  {'vars': {
                        'B01019': {'v': "San Nicolo'"},
                        'B01194': {'v': 'agrmet'},
                        'B04001': {'v': 2020},
                        'B04002': {'v': 2},
                        'B04003': {'v': 6},
                        'B04004': {'v': 1},
                        'B04005': {'v': 0},
                        'B04006': {'v': 0},
                        'B05001': {'v': 45.04139},
                        'B06001': {'v': 9.58959},
                        'B07030': {'v': 68.0},
                        'B07031': {'v': 68.0}}},
                  {'level': [1, None, None, None],
                   'timerange': [1, 0, 900],
                   'vars': {'B13011': {'v': 0.0}}},
                  {'level': [1, None, None, None],
                   'timerange': [1, 0, 3600],
                   'vars': {'B13011': {'v': 0.0}}},
                  {'level': [103, 2000, None, None],
                   'timerange': [0, 0, 3600],
                   'vars': {'B12101': {'v': 271.75},
                            'B13003': {'v': 70}}},
                  {'level': [103, 2000, None, None],
                   'timerange': [2, 0, 3600],
                   'vars': {'B12101': {'v': 273.55},
                            'B13003': {'v': 77}}},
                  {'level': [103, 2000, None, None],
                   'timerange': [3, 0, 3600],
                   'vars': {'B12101': {'v': 270.95},
                            'B13003': {'v': 54}}},
                  {'level': [103, 2000, None, None],
                   'timerange': [254, 0, 0],
                   'vars': {'B12101': {'v': 271.15},
                            'B13003': {'v': 75}}},
                  {'level': [106, 250, None, None],
                   'timerange': [254, 0, 0],
                   'vars': {'B13227': {'v': 43.7}}},
                  {'level': [106, 450, None, None],
                   'timerange': [254, 0, 0],
                   'vars': {'B13227': {'v': 44.1}}},
                  {'level': [106, 700, None, None],
                   'timerange': [254, 0, 0],
                   'vars': {'B13227': {'v': 43.0}}}],
             'date': '2020-02-06T01:00:00',
             'ident': None,
             'lat': 4504139,
             'lon': 958959,
             'network': 'agrmet',
             'version': '0.1'}],
            },
        'success': True}

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def json(self):
        if self.kwargs.get('return_error'):
            return self.dummy_json_error
        if self.kwargs.get('return_more'):
            return self.dummy_json_3
        return self.dummy_json


def dummy_request_get(*args, **kwargs):
    return DummyResponseObject(*args, **kwargs)


def dummy_request_get2(*args, **kwargs):
    kwargs['return_error'] = True
    return DummyResponseObject(*args, **kwargs)


def dummy_request_get3(*args, **kwargs):
    kwargs['return_more'] = True
    return DummyResponseObject(*args, **kwargs)


def test_build_sql():
    # no filters
    expected = 'SELECT * FROM "atable" '
    effective = arpaer.build_sql('atable')
    assert effective == expected

    # only 1 AND filter
    start = datetime(2020, 1, 6)
    effective = arpaer.build_sql('atable', start=start)
    expected = 'SELECT * FROM "atable"  WHERE date >= \'2020-01-06 00:00\''
    assert effective == expected

    # only 2 AND filters
    start = datetime(2020, 1, 6)
    end = datetime(2021, 2, 7)
    effective = arpaer.build_sql('atable', start=start, end=end)
    expected = 'SELECT * FROM "atable"  WHERE date >= \'2020-01-06 00:00\' ' \
               'AND date <= \'2021-02-07 00:00\''
    assert effective == expected

    # only 1 OR filter
    only_bcodes = ('B11001', )
    effective = arpaer.build_sql('atable', only_bcodes=only_bcodes)
    expected = 'SELECT * FROM "atable"  WHERE data::text LIKE \'%B11001%\''
    assert effective == expected

    # only 2 OR filter
    only_bcodes = ('B11001', 'B11002')
    effective = arpaer.build_sql('atable', only_bcodes=only_bcodes)
    expected = 'SELECT * FROM "atable"  WHERE (data::text LIKE \'%B11001%\' ' \
               'OR data::text LIKE \'%B11002%\')'
    assert effective == expected

    # more filters
    effective = arpaer.build_sql('atable', start=start, end=end,
                                 only_bcodes=only_bcodes, extra_field=10)
    expected = 'SELECT * FROM "atable"  WHERE date >= \'2020-01-06 00:00\' ' \
               'AND date <= \'2021-02-07 00:00\' AND extra_field  = \'10\' ' \
               'AND (data::text LIKE \'%B11001%\' OR data::text LIKE \'%B11002%\')'
    assert effective == expected

    # limit results
    effective = arpaer.build_sql('atable', start=start, end=end, limit=3,
                                 only_bcodes=only_bcodes, extra_field=10)
    expected = 'SELECT * FROM "atable"  WHERE date >= \'2020-01-06 00:00\' ' \
               'AND date <= \'2021-02-07 00:00\' AND extra_field  = \'10\' ' \
               'AND (data::text LIKE \'%B11001%\' OR data::text LIKE \'%B11002%\') limit 3'
    assert effective == expected


def test_sql2results(mocker):
    start = datetime(2020, 2, 6)
    end = datetime(2020, 2, 7)
    sql = arpaer.build_sql(arpaer.TABLE_NAME, start=start, end=end, limit=1)

    # testing a query with results
    mocker.patch('requests.get', new=dummy_request_get)
    expected_results = DummyResponseObject.dummy_json['result']['records']
    effective_results = arpaer.sql2results(sql)
    assert effective_results == expected_results

    # testing a query without results
    mocker.patch('requests.get', new=dummy_request_get2)
    effective_results = arpaer.sql2results(sql)
    assert effective_results == []


def test_get_json_results(mocker):
    start = datetime(2020, 2, 6)
    end = datetime(2020, 2, 7)
    mocker.patch('requests.get', new=dummy_request_get)
    expected_results = DummyResponseObject.dummy_json['result']['records']
    effective_results = arpaer.get_json_results(start=start, end=end, limit=1)
    assert effective_results == expected_results


def test_save_json_results(tmpdir):
    expected_filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    with open(expected_filepath) as fp:
        expected_lines = fp.readlines()
    json_results = DummyResponseObject.dummy_json_3['result']['records']
    filepath = str(tmpdir.join('out.json'))
    assert not exists(filepath)
    arpaer.save_json_results(filepath, json_results)
    assert exists(filepath)
    with open(filepath) as fp:
        effective_lines = fp.readlines()
    assert effective_lines == expected_lines


def test_load_json_results():
    expected = DummyResponseObject.dummy_json_3['result']['records']
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    effective = arpaer.load_json_results(filepath)
    assert effective == expected


def test_query_and_save(tmpdir, mocker):
    start = datetime(2020, 2, 6)
    end = datetime(2020, 2, 7)
    limit = 3
    save_path = str(tmpdir.join('out.json'))
    mocker.patch('requests.get', new=dummy_request_get3)
    expected = DummyResponseObject.dummy_json_3['result']['records']

    assert not exists(save_path)
    arpaer.query_and_save(save_path, start=start, end=end, limit=limit)
    assert exists(save_path)
    effective = arpaer.load_json_results(save_path)
    assert effective == expected


def test_load_btable():
    effective = arpaer.load_btable()
    for key in effective:
        for field in ['description', 'format', 'length', 'unit']:
            assert field in effective[key]
    # spot check
    assert 'B49218' in effective
    assert effective['B49220'] == {
        'par_code': 'B49220',
        'description': 'Density of particles with diameter > 34 <= 37 um',
        'format': '-1',
        'length': '5',
        'unit': 'NUMERIC/M**3'}
    assert effective['B48205'] == {
        'par_code': 'B48205',
        'description': 'Conta Vitacee',
        'format': '0',
        'length': '7',
        'unit': 'NUMERIC'}


def test_load_parameter_file():
    test_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    parameter_map = arpaer.load_parameter_file(test_filepath)
    for bcode, props in parameter_map.items():
        assert bcode.startswith('B')
        assert 'min' in props
        assert 'max' in props
        assert 'par_code' in props
        assert 'description' in props


def test_load_parameter_thresholds():
    test_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    expected_thresholds = {
        'Bagnatura_f': [0.0, 3600.0],
        'DD': [0.0, 360.0],
        'FF': [0.0, 102.0],
        'INSOL': [0.0, 60.0],
        'P': [96000.0, 106000.0],
        'PREC': [0.0, 989.0],
        'RADSOL': [0.0, 4184000.0],
        'Tmedia': [-35.0, 45.0],
        'UR media': [20.0, 100.0]
    }
    parameter_thresholds = arpaer.load_parameter_thresholds(test_filepath)
    assert parameter_thresholds == expected_thresholds


def test_validate_row_format():
    # right row
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    with open(filepath) as fp:
        row = fp.readline()
    row_json = json.loads(row)
    err_str = arpaer.validate_row_format(row)
    assert not err_str

    # wrong json
    wrong_row = row[:len(row)-10]
    err_str = arpaer.validate_row_format(wrong_row)
    assert err_str == 'it does not seem a valid JSON'

    # not lat
    wrong_row_json = row_json.copy()
    del wrong_row_json['lat']
    wrong_row = json.dumps(wrong_row_json)
    err_str = arpaer.validate_row_format(wrong_row)
    assert err_str == 'information of the station is not parsable'

    # wrong date
    wrong_row_json = row_json.copy()
    wrong_row_json['date'] = '2013-20-11T12:12:12'
    wrong_row = json.dumps(wrong_row_json)
    err_str = arpaer.validate_row_format(wrong_row)
    assert err_str == 'information of the date is wrong'

    # missing measures
    wrong_row_json = row_json.copy()
    del wrong_row_json['data'][1]['level']
    wrong_row = json.dumps(wrong_row_json)
    err_str = arpaer.validate_row_format(wrong_row)
    assert err_str == 'information of the measurements is not parsable'


def test_parse_row():
    json_result = DummyResponseObject.dummy_json['result']['records'][0]
    row = json.dumps(json_result)
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    parameters_map = arpaer.load_parameter_file(parameters_filepath)

    expected = (
        {'is_fixed': True,
         'lat': 4490250,
         'lon': 768972,
         'network': 'fidupo',
         'station': 'Carignano Po'},
        datetime(2020, 2, 6, 0, 0),
        [('River level', 1, 254, 1.22, True)])
    effective = arpaer.parse_row(row, parameters_map)
    assert effective == expected


def test_parse():
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    data = arpaer.parse(filepath, parameters_filepath)
    assert len(data) == 2
    for result in data:
        assert len(result) == 3
        assert result[0] == {
            'is_fixed': True,
            'lat': 4504139,
            'lon': 958959,
            'network': 'agrmet',
            'station': "San Nicolo'"
        }
    stat_props, date, measures = data[0]
    assert date == datetime(2020, 2, 6, 0, 0)
    assert measures ==[
        ('PREC', 1, 1, 0.0, True),
        ('PREC', 1, 1, 0.0, True),
        ('PREC', 1, 1, 0.0, True),
        ('UR media', 103, 0, 56, True),
        ('UR media', 103, 0, 38, True),
        ('UR media', 103, 2, 65, True),
        ('UR media', 103, 2, 70, True),
        ('UR media', 103, 3, 44, True),
        ('UR media', 103, 3, 14, True),
        ('UR media', 103, 254, 55, True)
    ]
    stat_props, date, measures = data[1]
    assert date == datetime(2020, 2, 6, 1, 0)
    assert measures == [
        ('PREC', 1, 1, 0.0, True),
        ('PREC', 1, 1, 0.0, True),
        ('UR media', 103, 0, 70, True),
        ('UR media', 103, 2, 77, True),
        ('UR media', 103, 3, 54, True),
        ('UR media', 103, 254, 75, True)
    ]


def test_write_data(tmpdir, mocker):
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    data = arpaer.parse(filepath, parameters_filepath)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        'station;latitude;longitude;network;date;parameter;level;trange;value;valid\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;PREC;1;1;0.0;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;PREC;1;1;0.0;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;PREC;1;1;0.0;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;0;56;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;0;38;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;2;65;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;2;70;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;3;44;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;3;14;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;254;55;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;PREC;1;1;0.0;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;PREC;1;1;0.0;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;0;70;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;2;77;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;3;54;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;254;75;True\n"
    ]
    assert not exists(out_filepath)
    arpaer.write_data(data, out_filepath)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows
    expected_rows = [
        'station;latitude;longitude;network;date;parameter;level;trange;value;valid\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;0;56;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;0;38;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;2;65;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;2;70;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;3;44;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;3;14;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 00:00:00;UR media;103;254;55;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;0;70;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;2;77;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;3;54;True\n",
        "San Nicolo';4504139;958959;agrmet;2020-02-06 01:00:00;UR media;103;254;75;True\n"
    ]
    out_filepath = str(tmpdir.join('datafile2.csv'))
    assert not exists(out_filepath)
    arpaer.write_data(data, out_filepath, omit_parameters=('PREC',))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows


def test_validate_format():
    # right file
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    err_msgs = arpaer.validate_format(filepath)
    assert not err_msgs

    # wrong file
    filepath = join(TEST_DATA_PATH, 'arpaer', 'wrong_results1.json')
    err_msgs = arpaer.validate_format(filepath)
    assert err_msgs == [
        (2, 'information of the station is not parsable'),
        (3, 'information of the date is wrong')]


def test_row_weak_climatologic_check():
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    parameters_thresholds = arpaer.load_parameter_thresholds(parameters_filepath)
    data = arpaer.parse(filepath, parameters_filepath)
    parsed_row = data[0]

    # wrong row
    assert parameters_thresholds['UR media'] == [20.0, 100.0]
    err_msgs, parsed_row_updated = arpaer.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert err_msgs == ["The value of 'UR media' is out of range [20.0, 100.0]"]
    assert parsed_row_updated != parsed_row
    assert parsed_row_updated[2][8] == ('UR media', 103, 3, 14, False)
    # no check if the value is already flagged as wrong
    err_msgs, parsed_row_updated2 = arpaer.row_weak_climatologic_check(
        parsed_row_updated, parameters_thresholds)
    assert not err_msgs
    assert parsed_row_updated == parsed_row_updated2
    parsed_row_updated[2][8] = ('UR media', 103, 3, 14, True)
    assert parsed_row_updated == parsed_row

    # right row
    parameters_thresholds['UR media'] = [10.0, 100.0]
    err_msgs, parsed_row_updated = arpaer.row_weak_climatologic_check(
        parsed_row, parameters_thresholds)
    assert not err_msgs
    assert parsed_row == parsed_row_updated

    # no thresholds
    err_msgs, parsed_row_updated = arpaer.row_weak_climatologic_check(parsed_row)
    assert not err_msgs
    assert parsed_row == parsed_row_updated


def test_do_weak_climatologic_check():
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    err_msgs, parsed_row = arpaer.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs == [(1, "The value of 'UR media' is out of range [20.0, 100.0]")]
    assert parsed_row == [
        (
            {'is_fixed': True,
             'lat': 4504139,
             'lon': 958959,
             'network': 'agrmet',
             'station': "San Nicolo'"},
            datetime(2020, 2, 6, 0, 0),
            [
                ('PREC', 1, 1, 0.0, True),
                ('PREC', 1, 1, 0.0, True),
                ('PREC', 1, 1, 0.0, True),
                ('UR media', 103, 0, 56, True),
                ('UR media', 103, 0, 38, True),
                ('UR media', 103, 2, 65, True),
                ('UR media', 103, 2, 70, True),
                ('UR media', 103, 3, 44, True),
                ('UR media', 103, 3, 14, False),
                ('UR media', 103, 254, 55, True)
            ]
        ),
        (
            {'is_fixed': True,
             'lat': 4504139,
             'lon': 958959,
             'network': 'agrmet',
             'station': "San Nicolo'"},
            datetime(2020, 2, 6, 1, 0),
            [
                ('PREC', 1, 1, 0.0, True),
                ('PREC', 1, 1, 0.0, True),
                ('UR media', 103, 0, 70, True),
                ('UR media', 103, 2, 77, True),
                ('UR media', 103, 3, 54, True),
                ('UR media', 103, 254, 75, True)
            ]
        ),
    ]

    # no parsing of not well formatted rows
    filepath = join(TEST_DATA_PATH, 'arpaer', 'wrong_results1.json')
    err_msgs2, parsed_row2 = arpaer.do_weak_climatologic_check(filepath, parameters_filepath)
    assert err_msgs2 == err_msgs
    assert parsed_row2 == [parsed_row[0]]


def test_do_internal_consistence_check():
    # TODO
    pass


def test_parse_and_check():
    filepath = join(TEST_DATA_PATH, 'arpaer', 'results.json')
    parameters_filepath = join(TEST_DATA_PATH, 'arpaer', 'arpaer_params.csv')
    limiting_params = dict()
    err_msgs, data = arpaer.parse_and_check(filepath, parameters_filepath, limiting_params)
    assert err_msgs == [(1, "The value of 'UR media' is out of range [20.0, 100.0]")]
    assert len(data) == 2
    for result in data:
        assert len(result) == 3
        assert result[0] == {
            'is_fixed': True,
            'lat': 4504139,
            'lon': 958959,
            'network': 'agrmet',
            'station': "San Nicolo'"
        }
    stat_props, date, measures = data[0]
    assert date == datetime(2020, 2, 6, 0, 0)
    assert measures ==[
        ('PREC', 1, 1, 0.0, True),
        ('PREC', 1, 1, 0.0, True),
        ('PREC', 1, 1, 0.0, True),
        ('UR media', 103, 0, 56, True),
        ('UR media', 103, 0, 38, True),
        ('UR media', 103, 2, 65, True),
        ('UR media', 103, 2, 70, True),
        ('UR media', 103, 3, 44, True),
        ('UR media', 103, 3, 14, False),
        ('UR media', 103, 254, 55, True)
    ]
    stat_props, date, measures = data[1]
    assert date == datetime(2020, 2, 6, 1, 0)
    assert measures == [
        ('PREC', 1, 1, 0.0, True),
        ('PREC', 1, 1, 0.0, True),
        ('UR media', 103, 0, 70, True),
        ('UR media', 103, 2, 77, True),
        ('UR media', 103, 3, 54, True),
        ('UR media', 103, 254, 75, True)
    ]

    filepath = join(TEST_DATA_PATH, 'arpaer', 'wrong_results1.json')
    err_msgs, data = arpaer.parse_and_check(filepath, parameters_filepath, limiting_params)
    assert err_msgs == [
        (2, 'information of the station is not parsable'),
        (3, 'information of the date is wrong'),
        (1, "The value of 'UR media' is out of range [20.0, 100.0]")
    ]
