from click.testing import CliRunner

from s3fetch import __version__ as version
from s3fetch.cli import cli


def test_printing_version():
    runner = CliRunner()
    prog_name = runner.get_default_prog_name(cli)
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == f"{prog_name}, version {version}\n"
