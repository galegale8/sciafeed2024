
from datetime import datetime
from os.path import exists

from sciafeed import export


def test_export2csv(tmpdir):
    stat_props = {'cod_utente': '70001', 'lat': 43.876999}
    data = [
        [stat_props, datetime(2013, 1, 1, 0, 0), '1', 9.0, True],
        [stat_props, datetime(2014, 1, 1, 0, 0), '2', 355.0, False],
        [stat_props, datetime(2013, 2, 1, 0, 0), '3', 68.0, True],
        [stat_props, datetime(2013, 1, 2, 0, 0), '4', None, True],
        [stat_props, datetime(2013, 1, 1, 3, 0), '5', None, False],
        [stat_props, datetime(2013, 1, 1, 0, 4), '6', 22, False],
    ]
    out_filepath = str(tmpdir.join('datafile.csv'))

    expected_rows = [
        'cod_utente;cod_rete;reghiscentral;date;parameter;value;valid\n',
        '70001;;ND;2013-01-01T00:00:00;1;9.0;1\n',
        '70001;;ND;2013-02-01T00:00:00;3;68.0;1\n',
        '70001;;ND;2014-01-01T00:00:00;2;355.0;0\n'
    ]
    assert not exists(out_filepath)
    export.export2csv(data, out_filepath, omit_parameters=('5', '6', '12'))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows

    expected_rows = [
        'cod_utente;cod_rete;reghiscentral;date;parameter;value;valid\n',
        '70001;;ND;2013-01-01T00:00:00;1;9.0;1\n',
        '70001;;ND;2013-01-01T00:04:00;6;22;0\n',
        '70001;;ND;2013-01-01T03:00:00;5;;0\n',
        '70001;;ND;2013-01-02T00:00:00;4;;1\n',
        '70001;;ND;2013-02-01T00:00:00;3;68.0;1\n',
        '70001;;ND;2014-01-01T00:00:00;2;355.0;0\n'
    ]
    export.export2csv(data, out_filepath, omit_missing=False)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows
