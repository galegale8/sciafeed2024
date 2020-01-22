
from click.testing import CliRunner

from sciafeed import entry_points


def test_hello_world():
    runner = CliRunner()
    result = runner.invoke(entry_points.hello_world, [])
    assert result.exit_code == 0
    assert result.output == "Hello world!\n"
