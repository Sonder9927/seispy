"""Shared result models for station-correction workflows."""

from dataclasses import dataclass
from pathlib import Path

from seispy.workflow import BatchSummary


@dataclass(frozen=True)
class CorrectionIssue:
    """One sampled waveform-correction failure."""

    station: str
    source: Path
    error: str


@dataclass(frozen=True)
class CorrectionSummary(BatchSummary):
    """Common summary returned by station-correction workflows."""

    total: int
    succeeded: int
    failed: int
    issue_samples: tuple[CorrectionIssue, ...]
    output_dir: Path
    skipped: int = 0

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)


@dataclass(frozen=True)
class CorrectionCounts:
    """Compact worker result transferred between processes."""

    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    issue_samples: tuple[CorrectionIssue, ...] = ()
