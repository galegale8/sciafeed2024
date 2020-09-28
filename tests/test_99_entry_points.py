
import operator
from os.path import join, exists
from os import mkdir, listdir
import shutil

from click.testing import CliRunner

from . import TEST_DATA_PATH
from sciafeed import entry_points


def test_make_report(tmpdir):
    runner = CliRunner()

    # ------------  arpa19 ------------
    in_filepath = join(TEST_DATA_PATH, 'arpa19', 'loc01_70001_201301010000_201401010100.dat')

    # creating only a report
    out_filepath = str(tmpdir.join('report1.txt'))
    assert not exists(out_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-r', out_filepath])
    assert result.exit_code == 0
    assert result.output == ""
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        lines = fp.readlines()
        assert 'No errors found\n' in lines

    # creating a data file
    outdata_filepath = str(tmpdir.join('data1.csv'))
    assert not exists(outdata_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-d', outdata_filepath])
    assert result.exit_code == 0
    assert 'No errors found\n' in result.output
    assert exists(outdata_filepath)
    with open(outdata_filepath) as fp:
        lines = fp.readlines()
        assert len(lines) == 36
        assert lines[0] == \
               'cod_utente;cod_rete;date;time;parameter;value;valid;source;format;lat;lon\n'

    # ------------  arpa21 ------------
    in_filepath = join(TEST_DATA_PATH, 'arpa21', 'loc01_00201_201201010000_201301010100.dat')

    # creating only a report
    out_filepath = str(tmpdir.join('report2.txt'))
    assert not exists(out_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-r', out_filepath])
    assert result.exit_code == 0
    assert result.output == ""
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        lines = fp.readlines()
        assert 'No errors found\n' in lines

    # creating a data file
    outdata_filepath = str(tmpdir.join('data2.csv'))
    assert not exists(outdata_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-d', outdata_filepath])
    assert result.exit_code == 0
    assert 'No errors found\n' in result.output
    assert exists(outdata_filepath)
    with open(outdata_filepath) as fp:
        lines = fp.readlines()
        assert len(lines) == 57
        assert lines[0] == \
               'cod_utente;cod_rete;date;time;parameter;value;valid;source;format;lat;lon\n'

    # ------------  arpafvg ------------
    in_filepath = join(TEST_DATA_PATH, 'arpafvg', 'loc01_00001_2018010101_2019010101.dat')

    # creating only a report
    out_filepath = str(tmpdir.join('report3.txt'))
    assert not exists(out_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-r', out_filepath])
    assert result.exit_code == 0
    assert result.output == ""
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        lines = fp.readlines()
        assert 'No errors found\n' in lines

    # creating a data file
    outdata_filepath = str(tmpdir.join('data3.csv'))
    assert not exists(outdata_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-d', outdata_filepath])
    assert result.exit_code == 0
    assert 'No errors found\n' in result.output
    assert exists(outdata_filepath)
    with open(outdata_filepath) as fp:
        lines = fp.readlines()
        assert len(lines) == 46
        assert lines[0] == \
               'cod_utente;cod_rete;date;time;parameter;value;valid;source;format;lat;lon\n'

    # ------------  rmn ------------
    in_filepath = join(TEST_DATA_PATH, 'rmn', 'ancona_right.csv')

    # creating only a report
    out_filepath = str(tmpdir.join('report4.txt'))
    assert not exists(out_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-r', out_filepath])
    assert result.exit_code == 0
    assert result.output == ""
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        lines = fp.readlines()
        assert 'No errors found\n' in lines

    # creating a data file
    outdata_filepath = str(tmpdir.join('data4.csv'))
    assert not exists(outdata_filepath)
    result = runner.invoke(entry_points.make_report, [in_filepath, '-d', outdata_filepath])
    assert result.exit_code == 0
    assert 'No errors found\n' in result.output
    assert exists(outdata_filepath)
    with open(outdata_filepath) as fp:
        lines = fp.readlines()
        assert len(lines) == 36
        assert lines[0] == \
               'cod_utente;cod_rete;date;time;parameter;value;valid;source;format;lat;lon\n'


def test_make_reports(tmpdir):
    runner = CliRunner()

    # prepare a folder with all formats
    in_folder = str(tmpdir.join('in_folder'))
    mkdir(in_folder)
    files_to_parse = [
        ('arpa19', 'loc01_70001_201301010000_201401010100.dat', 'ARPA-19'),
        ('arpa21', 'loc01_00201_201201010000_201301010100.dat', 'ARPA-21'),
        # ('arpaer', 'results.json', 'ARPA-ER'),
        ('arpafvg', 'loc01_00001_2018010101_2019010101.dat', 'ARPA-FVG'),
        ('rmn', 'ancona_right.csv', 'RMN'),
    ]
    for format_folder, filename, label in files_to_parse:
        src_filepath = join(TEST_DATA_PATH, format_folder, filename)
        shutil.copy(src_filepath, in_folder)

    # run with report on screen and without data written
    result = runner.invoke(entry_points.make_reports, [in_folder])
    assert result.exit_code == 0
    for format_folder, filename, label in files_to_parse:
        line = "processing file '%s'\n" % filename
        assert line in result.output
        filepath = join(in_folder, filename)
        line = 'START OF ANALYSIS OF %s FILE %r\n' % (label, filepath)
        assert line in result.output

    # run with report on file
    out_filepath = str(tmpdir.join('report.txt'))
    assert not exists(out_filepath)
    result = runner.invoke(entry_points.make_reports, [in_folder, '-r', out_filepath])
    assert result.exit_code == 0
    filenames_sorted = sorted(files_to_parse, key=operator.itemgetter(1))
    expected_output = '\n'.join(["processing file %r" % f[1] for f in filenames_sorted]) + '\n'
    assert result.output == expected_output
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        lines = fp.readlines()
    for format_folder, filename, label in filenames_sorted:
        filepath = join(in_folder, filename)
        line = 'START OF ANALYSIS OF %s FILE %r\n' % (label, filepath)
        assert line in lines

    # try to overwrite the report
    result = runner.invoke(entry_points.make_reports, [in_folder, '-r', out_filepath])
    assert result.exit_code != 0
    expected_output = 'wrong "report_filepath": the report must not exist or will be overwritten\n'
    assert result.output == expected_output

    # run with data files
    outdata_folder = str(tmpdir.join('data'))
    assert not exists(outdata_folder)
    result = runner.invoke(entry_points.make_reports, [in_folder, '-d', outdata_folder])
    assert result.exit_code == 0
    assert exists(outdata_folder)
    data_files = listdir(outdata_folder)
    for format_folder, filename, label in files_to_parse:
        outdata_file = filename + '.csv'
        assert outdata_file in data_files

    # put also an unknown file inside the folder and an empty folder
    a_folder = join(in_folder, 'afolder')
    mkdir(a_folder)
    unknown_filepath = join(in_folder, 'afile.txt')
    with open(unknown_filepath, 'w') as fp:
        fp.write("Hello! I'm an unknown file")
    result = runner.invoke(entry_points.make_reports, [in_folder])
    assert result.exit_code == 0
    assert "file %r has unknown format" % unknown_filepath in result.output


def test_compute_daily_indicators(tmpdir):
    runner = CliRunner()
    data_folder = join(TEST_DATA_PATH, 'indicators', 'input')
    expected_folder = join(TEST_DATA_PATH, 'indicators', 'expected')
    indicators_folder = str(tmpdir.join('indicators_out'))
    report_path = join(indicators_folder, 'report.txt')
    result = runner.invoke(entry_points.compute_daily_indicators,
                           ['-d', data_folder, '-i', indicators_folder, '-r', report_path])
    assert result.exit_code == 0
    for fname in listdir(expected_folder):
        if fname == 'result.txt':
            # other test
            continue
        indicators_filepath = join(indicators_folder, fname)
        assert exists(indicators_filepath)
        with open(indicators_filepath) as fp_effective:
            indicators_effective = fp_effective.read()
        with open(join(expected_folder, fname)) as fp_expected:
            assert indicators_effective == fp_expected.read()
