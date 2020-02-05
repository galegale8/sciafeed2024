
from os.path import join, exists

from click.testing import CliRunner

from . import TEST_DATA_PATH
from sciafeed import entry_points


def test_make_arpa19_report(tmpdir):
    in_filepath = join(TEST_DATA_PATH, 'loc01_70001_201301010000_201401010100.dat')

    # creating only a report
    out_filepath = str(tmpdir.join('report.txt'))
    assert not exists(out_filepath)
    runner = CliRunner()
    result = runner.invoke(entry_points.make_arpa19_report, [in_filepath, '-o', out_filepath])
    assert result.exit_code == 0
    assert result.output == ""
    assert exists(out_filepath)
    with open(out_filepath) as fp:
        lines = fp.readlines()
        assert 'No errors found\n' in lines

    # creating a data file
    outdata_filepath = str(tmpdir.join('data.csv'))
    assert not exists(outdata_filepath)
    runner = CliRunner()
    result = runner.invoke(entry_points.make_arpa19_report, [in_filepath, '-d', outdata_filepath])
    assert result.exit_code == 0
    assert 'No errors found\n' in result.output
    assert exists(outdata_filepath)
    with open(outdata_filepath) as fp:
        lines = fp.readlines()
        assert len(lines) == 101
        assert lines[0] == 'station;latitude;date;parameter;value;valid\n'
