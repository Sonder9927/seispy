import json
import time
import uuid
from dataclasses import asdict, is_dataclass, replace
from pathlib import Path
from typing import Any, Protocol, TypeVar


class ReportSummary(Protocol):
    run_id: str
    report_path: Path | None

    @property
    def has_issues(self) -> bool: ...

    def to_json(self, file: str | Path) -> Path: ...


SummaryT = TypeVar("SummaryT", bound=ReportSummary)


def create_run_id() -> str:
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


class ReportMixin:
    """Common status and JSON export for batch summary dataclasses."""

    @property
    def has_issues(self) -> bool:
        """Whether the batch completed with domain-specific issues."""
        raise NotImplementedError

    @property
    def ok(self) -> bool:
        """Whether the batch completed without domain-specific issues."""
        return not self.has_issues

    def to_json(self, file: str | Path) -> Path:
        if not is_dataclass(self):
            raise TypeError("ReportMixin must be used with a dataclass")
        path = Path(file)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(_json_value(asdict(self)), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return path


def auto_save_report(
    summary: SummaryT,
    name: str,
    save_report: bool | None,
    report_dir: str | Path = "logs/reports",
) -> SummaryT:
    """Save always, never, or only on issues according to ``save_report``."""
    if save_report is False or (save_report is None and not summary.has_issues):
        return summary
    path = Path(report_dir) / f"{name}-{summary.run_id}.json"
    updated = replace(summary, report_path=path)
    updated.to_json(path)
    return updated
