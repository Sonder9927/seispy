"""Reliable output commits and machine-readable batch reports."""

import json
import logging
import os
import tempfile
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from contextlib import AbstractContextManager
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Self


def temporary_output_path(destination: str | Path) -> Path:
    """Reserve a unique temporary name beside its final destination."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.stem}.",
        suffix=destination.suffix,
        dir=destination.parent,
    )
    os.close(descriptor)
    path = Path(name)
    path.unlink()
    return path


def validate_output(path: str | Path, description: str = "output") -> Path:
    path = Path(path)
    if not path.is_file() or path.stat().st_size == 0:
        raise OSError(f"{description} was not written")
    return path


def commit_output(
    temporary: str | Path, destination: str | Path, *, overwrite: bool = False
) -> Path:
    """Atomically commit a validated output, optionally replacing its target."""
    temporary = validate_output(temporary)
    destination = Path(destination)
    if overwrite:
        os.replace(temporary, destination)
    else:
        os.link(temporary, destination)
        temporary.unlink()
    return destination


def cleanup_outputs(paths: Iterable[str | Path]) -> None:
    for path in paths:
        Path(path).unlink(missing_ok=True)


def new_run_id() -> str:
    """Return a compact, sortable identifier for one batch run."""
    return f"{time.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _write_json_atomic(path: Path, value: Any) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary_output_path(path)
    try:
        temporary.write_text(
            json.dumps(_json_value(value), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(temporary, path)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return path


@dataclass(frozen=True, kw_only=True)
class BatchSummary(ABC):
    """Shared identity, timing, status, and artifacts for a batch run.

    Attributes:
        run_id: Unique identifier shared by the report and log.
        duration_seconds: Elapsed wall-clock time.
        report_path: Atomic JSON report path when enabled.
        log_path: Persistent text log path when enabled.
        status: Lifecycle state: running, completed, interrupted, or failed.
    """

    run_id: str
    duration_seconds: float
    report_path: Path | None = None
    log_path: Path | None = None
    status: Literal["running", "completed", "interrupted", "failed"] = "completed"

    @property
    @abstractmethod
    def has_issues(self) -> bool:
        """Whether the completed run needs attention."""

    @property
    def ok(self) -> bool:
        return self.status == "completed" and not self.has_issues

    def to_json(self, file: str | Path) -> Path:
        path = Path(file)
        return _write_json_atomic(path, asdict(self))

    def save_report(
        self,
        name: str,
        when: bool | None = None,
        directory: str | Path = "logs/reports",
    ) -> Self:
        """Write always, never, or only when the run has issues."""
        if when is False or (when is None and not self.has_issues):
            return self
        path = Path(directory) / f"{name}-{self.run_id}.json"
        updated = replace(self, report_path=path)
        updated.to_json(path)
        return updated


class BatchRun(AbstractContextManager):
    """Persist one batch run's lifecycle behind a small journal interface.

    Callers publish domain counters with :meth:`start` and :meth:`checkpoint`,
    then pass their final :class:`BatchSummary` to :meth:`complete`. The journal
    owns paths, atomic report updates, persistent logging, and interruption
    state transitions.
    """

    def __init__(
        self,
        name: str,
        artifact_root: str | Path,
        *,
        run_id: str | None = None,
        save_report: bool | None = True,
        save_log: bool = True,
        logger: logging.Logger | None = None,
        checkpoint_interval: float = 1.0,
    ) -> None:
        if checkpoint_interval < 0:
            raise ValueError("checkpoint_interval cannot be negative")
        self.name = name
        self.run_id = run_id or new_run_id()
        self.artifact_root = Path(artifact_root).expanduser().resolve()
        logs = self.artifact_root / "logs"
        self.report_path = (
            logs / "reports" / f"{name}-{self.run_id}.json"
            if save_report is not False
            else None
        )
        self.log_path = logs / f"{name}-{self.run_id}.log" if save_log else None
        self._save_report = save_report
        self._logger = logger
        self._checkpoint_interval = checkpoint_interval
        self._last_checkpoint = 0.0
        self._run_logger: logging.Logger | None = None
        self._handler: logging.Handler | None = None
        self._started = time.monotonic()
        self._state: dict[str, Any] = {}
        self._complete = False
        self._lock = threading.Lock()

    @property
    def duration_seconds(self) -> float:
        return round(time.monotonic() - self._started, 3)

    def __enter__(self) -> Self:
        if self.log_path is not None:
            self.log_path.parent.mkdir(parents=True, exist_ok=True)
            run_logger = logging.getLogger(f"{__name__}.run.{self.name}.{self.run_id}")
            run_logger.setLevel(logging.INFO)
            run_logger.propagate = False
            handler = logging.FileHandler(self.log_path, encoding="utf-8")
            handler.setFormatter(
                logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            )
            run_logger.addHandler(handler)
            self._run_logger = run_logger
            self._handler = handler
        self._state = {
            "run_id": self.run_id,
            "name": self.name,
            "status": "running",
            "started_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "duration_seconds": 0.0,
            "report_path": self.report_path,
            "log_path": self.log_path,
        }
        self._write_report()
        self._last_checkpoint = time.monotonic()
        self.info("run_id=%s started task=%s", self.run_id, self.name)
        return self

    def start(self, *, total: int, **fields: Any) -> None:
        self._state.update(fields, total=total, completed=0)
        self._write_report()
        self.info("run_id=%s planned total=%d", self.run_id, total)

    def checkpoint(self, *, completed: int, total: int, **fields: Any) -> None:
        self._state.update(fields, total=total, completed=completed)
        now = time.monotonic()
        if (
            completed < total
            and now - self._last_checkpoint < self._checkpoint_interval
        ):
            return
        self._write_report()
        self._last_checkpoint = now
        counters = " ".join(
            f"{key}={value}"
            for key, value in fields.items()
            if isinstance(value, (int, float, bool))
        )
        suffix = f" {counters}" if counters else ""
        self.info(
            "run_id=%s progress=%d/%d%s",
            self.run_id,
            completed,
            total,
            suffix,
        )

    def complete(self, summary: BatchSummary) -> BatchSummary:
        started_at = self._state.get("started_at")
        completed_tasks = self._state.get("completed")
        completed = replace(
            summary,
            duration_seconds=self.duration_seconds,
            report_path=self.report_path,
            log_path=self.log_path,
            status="completed",
        )
        self._state = {
            **_json_value(asdict(completed)),
            "name": self.name,
            "started_at": started_at,
            "updated_at": datetime.now(UTC).isoformat(),
            "completed": completed_tasks,
        }
        if self.report_path is not None:
            if self._save_report is None and not completed.has_issues:
                self.report_path.unlink(missing_ok=True)
                completed = replace(completed, report_path=None)
            else:
                self._write_report()
        self._complete = True
        self.info(
            "run_id=%s completed duration_seconds=%.3f issues=%s report=%s",
            self.run_id,
            completed.duration_seconds,
            completed.has_issues,
            completed.report_path,
        )
        return completed

    def info(self, message: str, *args: Any) -> None:
        self._log(logging.INFO, message, *args)

    def warning(self, message: str, *args: Any) -> None:
        self._log(logging.WARNING, message, *args)

    def error(self, message: str, *args: Any) -> None:
        self._log(logging.ERROR, message, *args)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> bool:
        if exc_type is not None:
            interrupted = issubclass(exc_type, (KeyboardInterrupt, SystemExit))
            status = "interrupted" if interrupted else "failed"
            self._state.update(
                status=status,
                updated_at=datetime.now(UTC).isoformat(),
                duration_seconds=self.duration_seconds,
                error=f"{exc_type.__name__}: {exc}",
            )
            self._write_report()
            self._log(
                logging.WARNING if interrupted else logging.ERROR,
                "run_id=%s %s progress=%s/%s error=%s: %s",
                self.run_id,
                status,
                self._state.get("completed", 0),
                self._state.get("total", "?"),
                exc_type.__name__,
                exc,
            )
        elif not self._complete:
            self._state.update(
                status="failed",
                updated_at=datetime.now(UTC).isoformat(),
                duration_seconds=self.duration_seconds,
                error="BatchRun exited without complete()",
            )
            self._write_report()
            self.error("run_id=%s failed: complete() was not called", self.run_id)
        self._close_handler()
        return False

    def _write_report(self) -> None:
        if self.report_path is None:
            return
        with self._lock:
            self._state.update(
                updated_at=datetime.now(UTC).isoformat(),
                duration_seconds=self.duration_seconds,
            )
            _write_json_atomic(self.report_path, self._state)

    def _log(self, level: int, message: str, *args: Any) -> None:
        if self._logger is not None:
            self._logger.log(level, message, *args)
        if self._run_logger is not None:
            self._run_logger.log(level, message, *args)

    def _close_handler(self) -> None:
        if self._run_logger is not None and self._handler is not None:
            self._run_logger.removeHandler(self._handler)
            self._handler.close()
        self._run_logger = None
        self._handler = None
