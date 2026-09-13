"""One-pass header index for continuous SAC event cutting."""

from __future__ import annotations

from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import obspy
from obspy import UTCDateTime

from seispy.archive import WaveformIdentity


@dataclass(frozen=True)
class WaveformRecord:
    path: Path
    identity: WaveformIdentity
    starttime: UTCDateTime
    endtime: UTCDateTime


@dataclass(frozen=True)
class ArchiveIndexIssue:
    path: Path
    error: str


class WaveformArchiveIndex:
    """Immutable station/day index built with one header read per SAC file."""

    def __init__(self, buckets, issues=()):
        self._buckets = buckets
        self.issues = tuple(issues)

    @classmethod
    def build(
        cls,
        network_root: str | Path,
        *,
        stations: set[str] | None = None,
        pattern: str = "*.sac",
    ) -> "WaveformArchiveIndex":
        root = Path(network_root)
        archive_root = root.parent
        buckets = defaultdict(list)
        issues = []
        station_dirs = sorted(path for path in root.iterdir() if path.is_dir())
        for station_dir in station_dirs:
            if stations is not None and station_dir.name not in stations:
                continue
            for path in sorted(station_dir.glob(f"*/{pattern}")):
                try:
                    stream = obspy.read(path, headonly=True)
                    if len(stream) != 1:
                        raise ValueError("SAC file must contain exactly one trace")
                    trace = stream[0]
                    identity = WaveformIdentity.from_trace(trace)
                    if not identity.matches_sac_path(path, archive_root):
                        raise ValueError(
                            "filename or directory does not match the SAC header"
                        )
                    if identity.station != station_dir.name:
                        raise ValueError(
                            "station directory does not match the SAC header"
                        )
                    record = WaveformRecord(
                        path, identity, trace.stats.starttime, trace.stats.endtime
                    )
                    for day in _covered_days(record.starttime, record.endtime):
                        buckets[(identity.station, day)].append(record)
                except Exception as exc:
                    issues.append(
                        ArchiveIndexIssue(path, f"{type(exc).__name__}: {exc}")
                    )
        frozen = {key: tuple(records) for key, records in buckets.items()}
        return cls(frozen, issues)

    def overlapping(
        self, station: str, starttime, endtime
    ) -> tuple[WaveformRecord, ...]:
        """Return records whose header time spans intersect the requested window."""
        unique = {}
        for day in _covered_days(starttime, endtime):
            for record in self._buckets.get((station, day), ()):
                if intervals_overlap(
                    record.starttime, record.endtime, starttime, endtime
                ):
                    unique[record.path] = record
        return tuple(sorted(unique.values(), key=lambda item: item.path))


class WaveformReader:
    """Bounded LRU cache for full waveform reads during chronological cuts."""

    def __init__(self, max_files: int = 8):
        if max_files < 1:
            raise ValueError("max_files must be at least 1")
        self._max_files = max_files
        self._cache = OrderedDict()

    def read(self, record: WaveformRecord):
        try:
            stream = self._cache.pop(record.path)
        except KeyError:
            stream = obspy.read(record.path)
        self._cache[record.path] = stream
        while len(self._cache) > self._max_files:
            self._cache.popitem(last=False)
        return stream.copy()


def intervals_overlap(first_start, first_end, second_start, second_end) -> bool:
    """Return whether two inclusive time intervals overlap."""
    return first_start <= second_end and first_end >= second_start


def _covered_days(starttime, endtime):
    start = date(starttime.year, starttime.month, starttime.day)
    end = date(endtime.year, endtime.month, endtime.day)
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
