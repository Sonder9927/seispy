import logging
import warnings
from io import StringIO
from unittest.mock import patch

import pytest

from seispy.progress import (
    call_with_warnings,
    progress_bar,
    resolve_worker_call,
)


@pytest.mark.parametrize(
    "scenario",
    [
        "progress_bar_is_disabled_when_stderr_is_not_a_tty",
        "progress_bar_routes_python_warnings_through_logging",
    ],
)
def test_terminal_progress(scenario):
    if scenario == "progress_bar_is_disabled_when_stderr_is_not_a_tty":
        stream = StringIO()
        with (
            patch("seispy.progress.sys.stderr.isatty", return_value=False),
            progress_bar(total=1, desc="Testing", file=stream) as bar,
        ):
            assert bar.disable is True
            bar.update(1)
        assert stream.getvalue() == ""
    elif scenario == "progress_bar_routes_python_warnings_through_logging":
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


def _warn_in_worker():
    warnings.warn("worker warning", RuntimeWarning, stacklevel=2)
    return 42


@pytest.mark.parametrize(
    "scenario",
    [
        "worker_warnings_are_captured_until_resolved",
        "worker_failure_is_returned_without_printing_from_worker",
    ],
)
def test_worker_issues(caplog, scenario):
    if scenario == "worker_warnings_are_captured_until_resolved":
        captured = call_with_warnings(_warn_in_worker)

        assert len(captured.warnings) == 1
        assert resolve_worker_call(captured) == 42
        assert "worker warning" in caplog.text
    elif scenario == "worker_failure_is_returned_without_printing_from_worker":

        def fail():
            raise ValueError("broken")

        captured = call_with_warnings(fail)

        with pytest.raises(RuntimeError, match="ValueError: broken"):
            resolve_worker_call(captured)
