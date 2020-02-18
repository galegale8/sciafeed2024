
from datetime import datetime
from os.path import join, exists

from sciafeed import arpaer

from pprint import pprint
def test_load_btable():
    effective = arpaer.load_btable()
    for key in effective:
        for field in ['description', 'format', 'length', 'unit']:
            assert field in effective[key]
    # spot check
    assert 'B49218' in effective
    assert effective['B49220'] == {
        'description': 'Density of particles with diameter > 34 <= 37 um',
        'format': '-1',
        'length': '5',
        'unit': 'NUMERIC/M**3'}
    assert effective['B48205'] == {
        'description': 'Conta Vitacee',
        'format': '0',
        'length': '7',
        'unit': 'NUMERIC'}


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


def test_parse_json_result():
    json_result = DummyResponseObject.dummy_json['result']['records'][0]
    btable = arpaer.load_btable()
    expected = (
        {'is_fixed': True,
         'lat': 4490250,
         'lon': 768972,
         'network': 'fidupo',
         'station': 'Carignano Po'},
        datetime(2020, 2, 6, 0, 0),
        [('River level', 1, 254, 1.22, True)])
    effective = arpaer.parse_json_result(json_result, btable)
    assert effective == expected


def test_get_data(mocker):
    start = datetime(2020, 2, 6)
    end = datetime(2020, 2, 7)
    limit = 2
    mocker.patch('requests.get', new=dummy_request_get3)
    downloaded_data = arpaer.get_data(start=start, end=end, limit=limit)
    assert len(downloaded_data) == 1
    assert len(downloaded_data[0]) == 2
    stat_props, dates_measures = downloaded_data[0]
    assert stat_props == {
        'station': "San Nicolo'",
        'lat': 4504139,
        'lon': 958959,
        'network': 'agrmet',
        'is_fixed': True}
    assert len(dates_measures) == 2
    assert list(dates_measures.keys()) == [
        datetime(2020, 2, 6, 0, 0),
        datetime(2020, 2, 6, 1, 0)]
    assert dates_measures[datetime(2020, 2, 6, 0, 0)] == [
        ('TOTAL PRECIPITATION / TOTAL WATER EQUIVALENT', 1, 1, 0.0, True),
        ('TOTAL PRECIPITATION / TOTAL WATER EQUIVALENT', 1, 1, 0.0, True),
        ('TOTAL PRECIPITATION / TOTAL WATER EQUIVALENT', 1, 1, 0.0, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 0, 274.45, True),
        ('RELATIVE HUMIDITY', 103, 0, 56, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 0, 280.38, True),
        ('RELATIVE HUMIDITY', 103, 0, 38, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 2, 275.65, True),
        ('RELATIVE HUMIDITY', 103, 2, 65, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 2, 286.85, True),
        ('RELATIVE HUMIDITY', 103, 2, 70, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 3, 272.65, True),
        ('RELATIVE HUMIDITY', 103, 3, 44, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 3, 272.55, True),
        ('RELATIVE HUMIDITY', 103, 3, 14, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 254, 273.65, True),
        ('RELATIVE HUMIDITY', 103, 254, 55, True),
        ('Soil volumetric water content', 106, 254, 43.7, True),
        ('Soil volumetric water content', 106, 254, 44.1, True),
        ('Soil volumetric water content', 106, 254, 43.0, True)
    ]
    assert dates_measures[datetime(2020, 2, 6, 1, 0)] == [
        ('TOTAL PRECIPITATION / TOTAL WATER EQUIVALENT', 1, 1, 0.0, True),
        ('TOTAL PRECIPITATION / TOTAL WATER EQUIVALENT', 1, 1, 0.0, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 0, 271.75, True),
        ('RELATIVE HUMIDITY', 103, 0, 70, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 2, 273.55, True),
        ('RELATIVE HUMIDITY', 103, 2, 77, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 3, 270.95, True),
        ('RELATIVE HUMIDITY', 103, 3, 54, True),
        ('TEMPERATURE/DRY-BULB TEMPERATURE', 103, 254, 271.15, True),
        ('RELATIVE HUMIDITY', 103, 254, 75, True),
        ('Soil volumetric water content', 106, 254, 43.7, True),
        ('Soil volumetric water content', 106, 254, 44.1, True),
        ('Soil volumetric water content', 106, 254, 43.0, True)
    ]


def test_write_data(tmpdir, mocker):
    start = datetime(2020, 2, 6)
    end = datetime(2020, 2, 7)
    limit = 2
    mocker.patch('requests.get', new=dummy_request_get3)
    data = arpaer.get_data(start=start, end=end, limit=limit)
    out_filepath = str(tmpdir.join('datafile.csv'))
    expected_rows = [
        'station;latitude;longitude;network;date;parameter;level;trange;value;valid\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TOTAL PRECIPITATION / "
        'TOTAL WATER EQUIVALENT;1;1;0.0;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TOTAL PRECIPITATION / "
        'TOTAL WATER EQUIVALENT;1;1;0.0;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TOTAL PRECIPITATION / "
        'TOTAL WATER EQUIVALENT;1;1;0.0;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;0;274.45;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;0;56;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;0;280.38;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;0;38;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;2;275.65;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;2;65;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;2;286.85;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;2;70;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;3;272.65;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;3;44;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;3;272.55;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;3;14;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;254;273.65;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T00:00:00;RELATIVE "
        'HUMIDITY;103;254;55;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;TOTAL PRECIPITATION / "
        'TOTAL WATER EQUIVALENT;1;1;0.0;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;TOTAL PRECIPITATION / "
        'TOTAL WATER EQUIVALENT;1;1;0.0;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;0;271.75;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;RELATIVE "
        'HUMIDITY;103;0;70;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;2;273.55;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;RELATIVE "
        'HUMIDITY;103;2;77;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;3;270.95;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;RELATIVE "
        'HUMIDITY;103;3;54;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;TEMPERATURE/DRY-BULB "
        'TEMPERATURE;103;254;271.15;1\n',
        "San Nicolo';4504139;958959;agrmet;2020-02-06T01:00:00;RELATIVE "
        'HUMIDITY;103;254;75;1\n'
    ]
    assert not exists(out_filepath)
    arpaer.write_data(data, out_filepath, omit_parameters=('Soil volumetric water content', ))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows
