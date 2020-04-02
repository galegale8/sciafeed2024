
from datetime import datetime, date
from os.path import exists

from sciafeed import export


def test_export2csv(tmpdir):
    metadata = {'cod_utente': '70001', 'lat': 43.876999, 'source': 'afile/path', 'format':'arpa19'}
    data = [
        [metadata, datetime(2013, 1, 1, 0, 0), '1', 9.0, True],
        [metadata, datetime(2014, 1, 1, 0, 0), '2', 355.0, False],
        [metadata, datetime(2013, 2, 1, 0, 0), '3', 68.0, True],
        [metadata, datetime(2013, 1, 2, 0, 0), '4', None, True],
        [metadata, datetime(2013, 1, 1, 3, 0), '5', None, False],
        [metadata, datetime(2013, 1, 1, 0, 4), '6', 22, False],
    ]
    out_filepath = str(tmpdir.join('datafile.csv'))

    expected_rows = [
        'cod_utente;cod_rete;date;time;parameter;value;valid;source;format\n',
        '70001;;2013-01-01;00:00:00;1;9.0;1;afile/path;arpa19\n',
        '70001;;2013-02-01;00:00:00;3;68.0;1;afile/path;arpa19\n',
        '70001;;2014-01-01;00:00:00;2;355.0;0;afile/path;arpa19\n'
    ]
    assert not exists(out_filepath)
    export.export2csv(data, out_filepath, omit_parameters=('5', '6', '12'))
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows

    expected_rows = [
        'cod_utente;cod_rete;date;time;parameter;value;valid;source;format\n',
        '70001;;2013-01-01;00:00:00;1;9.0;1;afile/path;arpa19\n',
        '70001;;2013-01-01;00:04:00;6;22;0;afile/path;arpa19\n',
        '70001;;2013-01-01;03:00:00;5;;0;afile/path;arpa19\n',
        '70001;;2013-01-02;00:00:00;4;;1;afile/path;arpa19\n',
        '70001;;2013-02-01;00:00:00;3;68.0;1;afile/path;arpa19\n',
        '70001;;2014-01-01;00:00:00;2;355.0;0;afile/path;arpa19\n'
    ]
    export.export2csv(data, out_filepath, omit_missing=False)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows

    data = [
        [metadata, date(2013, 1, 1), '1', 9.0, True],
        [metadata, date(2014, 1, 1), '2', 355.0, False],
        [metadata, date(2013, 2, 1), '3', 68.0, True],
        [metadata, date(2013, 1, 2), '4', None, True],
        [metadata, date(2013, 1, 1), '5', None, False],
        [metadata, date(2013, 1, 1), '6', 22, False],
    ]
    out_filepath = str(tmpdir.join('datafile2.csv'))
    expected_rows = [
        'cod_utente;cod_rete;date;time;parameter;value;valid;source;format\n',
        '70001;;2013-01-01;;1;9.0;1;afile/path;arpa19\n',
        '70001;;2013-01-01;;5;;0;afile/path;arpa19\n',
        '70001;;2013-01-01;;6;22;0;afile/path;arpa19\n',
        '70001;;2013-01-02;;4;;1;afile/path;arpa19\n',
        '70001;;2013-02-01;;3;68.0;1;afile/path;arpa19\n',
        '70001;;2014-01-01;;2;355.0;0;afile/path;arpa19\n'
    ]
    assert not exists(out_filepath)
    export.export2csv(data, out_filepath, omit_missing=False)
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        rows = fp.readlines()
        assert rows == expected_rows
