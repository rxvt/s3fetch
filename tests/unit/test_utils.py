from threading import Lock

from s3fetch.utils import tprint


def test_printing_to_stdout(capfd):
    lock = Lock()
    tprint("this message should print to stdout.", lock)
    out, err = capfd.readouterr()
    assert out == "this message should print to stdout.\n"


def test_not_printing_when_quiet_mode_enabled(capfd):
    lock = Lock()
    tprint("this message should not print to stdout.", lock, quiet=True)
    out, err = capfd.readouterr()
    assert out != "this message should not print to stdout.\n"
