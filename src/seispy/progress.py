"""Consistent terminal progress that coexists with Python warnings and logging."""

from __future__ import annotations

import logging
import sys
import warnings
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Iterable, Iterator

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

BAR_FORMAT = (
    "{desc:<22} {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
    "[{elapsed}<{remaining}, {rate_fmt}] {postfix}"
)


@dataclass(frozen=True)
class CapturedWarning:
    """Pickle-safe warning raised while a worker call was running."""

    message: str
    category: str
    filename: str
    lineno: int


@dataclass(frozen=True)
class WorkerCall:
    """A worker value or failure plus warnings captured in that process."""

    value: Any = None
    error_type: str | None = None
    error_message: str | None = None
    warnings: tuple[CapturedWarning, ...] = ()


def _log_warning(message, category, filename, lineno, file=None, line=None) -> None:
    """Send Python warnings through logging so tqdm can redraw cleanly."""
    text = warnings.formatwarning(message, category, filename, lineno, line).rstrip()
    logging.getLogger("py.warnings").warning("%s", text)


def call_with_warnings(function, *args, **kwargs) -> WorkerCall:
    """Run one process-worker call without letting its warnings touch the TTY."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        try:
            value = function(*args, **kwargs)
        except Exception as exc:
            value = None
            error_type = type(exc).__name__
            error_message = str(exc)
        else:
            error_type = error_message = None
    captured = tuple(
        CapturedWarning(
            message=str(item.message),
            category=item.category.__name__,
            filename=item.filename,
            lineno=item.lineno,
        )
        for item in caught
    )
    return WorkerCall(value, error_type, error_message, captured)


def resolve_worker_call(call: WorkerCall) -> Any:
    """Display returned worker warnings above the active bar and unwrap its value."""
    warning_logger = logging.getLogger("py.warnings")
    for item in call.warnings:
        warning_logger.warning(
            "%s:%d: %s: %s",
            item.filename,
            item.lineno,
            item.category,
            item.message,
        )
    if call.error_type is not None:
        raise RuntimeError(f"{call.error_type}: {call.error_message}")
    return call.value


@contextmanager
def progress_bar(*, total: int, desc: str, unit: str = "item", **kwargs: Any):
    """Yield one TTY-aware progress bar while keeping log messages above it."""
    options = {
        "total": total,
        "desc": desc,
        "unit": unit,
        "dynamic_ncols": True,
        "position": 0,
        "leave": True,
        "mininterval": 0.25,
        "maxinterval": 2.0,
        "smoothing": 0.1,
        "bar_format": BAR_FORMAT,
        "disable": not sys.stderr.isatty(),
    }
    options.update(kwargs)
    previous_showwarning = warnings.showwarning
    warnings.showwarning = _log_warning
    try:
        with logging_redirect_tqdm(), tqdm(**options) as bar:
            yield bar
    finally:
        warnings.showwarning = previous_showwarning


def progress_iter(
    iterable: Iterable[Any],
    *,
    total: int,
    desc: str,
    unit: str = "item",
    **kwargs: Any,
) -> Iterator[Any]:
    """Iterate with the shared progress presentation and warning handling."""
    with progress_bar(total=total, desc=desc, unit=unit, **kwargs) as bar:
        for item in iterable:
            yield item
            bar.update(1)
