from s3fetch import utils


def test_create_exit_event():
    exit_event = utils.create_exit_event()
    assert exit_event.is_set() is False
    exit_event.set()
    assert exit_event.is_set() is True
    exit_event.clear()
    assert exit_event.is_set() is False
