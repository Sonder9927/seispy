import logging
import warnings
from io import StringIO
from unittest.mock import patch

import pytest

from seispy.progress import (
    BAR_FORMAT,
    call_with_warnings,
    progress_bar,
    progress_iter,
    resolve_worker_call,
)


def test_progress_bar_uses_one_consistent_lightweight_format():
    stream = StringIO()
    with (
        patch("seispy.progress.sys.stderr.isatty", return_value=True),
        progress_bar(total=2, desc="Testing", unit="file", file=stream) as bar,
    ):
        bar.update(2)

    output = stream.getvalue()
    assert output.count("100%") == 1
    assert "Testing" in output
    assert "2/2" in output
    assert BAR_FORMAT.startswith("{desc:<22}")


def test_progress_bar_is_disabled_when_stderr_is_not_a_tty():
    stream = StringIO()
    with (
        patch("seispy.progress.sys.stderr.isatty", return_value=False),
        progress_bar(total=1, desc="Testing", file=stream) as bar,
    ):
        assert bar.disable is True
        bar.update(1)

    assert stream.getvalue() == ""


def test_progress_bar_routes_python_warnings_through_logging():
    stream = StringIO()
    handler = logging.StreamHandler(stream)
    warning_logger = logging.getLogger("py.warnings")
    warning_logger.addHandler(handler)
    warning_logger.setLevel(logging.WARNING)
    try:
        with progress_bar(total=1, desc="Testing", disable=True):
            warnings.warn("example warning", UserWarning, stacklevel=2)
    finally:
        warning_logger.removeHandler(handler)

    assert "example warning" in stream.getvalue()


def test_progress_iter_yields_every_item():
    assert list(progress_iter(range(3), total=3, desc="Testing", disable=True)) == [
        0,
        1,
        2,
    ]


def _warn_in_worker():
    warnings.warn("worker warning", RuntimeWarning, stacklevel=2)
    return 42


def test_worker_warnings_are_captured_until_resolved(caplog):
    captured = call_with_warnings(_warn_in_worker)

    assert len(captured.warnings) == 1
    assert resolve_worker_call(captured) == 42
    assert "worker warning" in caplog.text


def test_worker_failure_is_returned_without_printing_from_worker():
    def fail():
        raise ValueError("broken")

    captured = call_with_warnings(fail)

    with pytest.raises(RuntimeError, match="ValueError: broken"):
        resolve_worker_call(captured)
