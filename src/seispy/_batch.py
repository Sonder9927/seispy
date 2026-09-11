"""Reliable output commits and machine-readable batch reports."""

import json
import os
import tempfile
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Iterable
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import Any, Self


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


@dataclass(frozen=True, kw_only=True)
class BatchSummary(ABC):
    """Shared identity, timing, status, and reporting for a batch run."""

    run_id: str
    duration_seconds: float
    report_path: Path | None = None

    @property
    @abstractmethod
    def has_issues(self) -> bool:
        """Whether the completed run needs attention."""

    @property
    def ok(self) -> bool:
        return not self.has_issues

    def to_json(self, file: str | Path) -> Path:
        path = Path(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(_json_value(asdict(self)), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path

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
