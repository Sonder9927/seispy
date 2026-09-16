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
    """Immutable NS/day index built with one header read per waveform file."""

    def __init__(self, buckets, issues=()):
        self._buckets = buckets
        self.issues = tuple(issues)

    @classmethod
    def build(
        cls,
        source_dir: str | Path,
        *,
        stations: set[tuple[str, str]] | None = None,
        pattern: str = "*.sac",
    ) -> "WaveformArchiveIndex":
        root = Path(source_dir).expanduser().resolve()
        if not root.is_dir():
            raise NotADirectoryError(f"Source directory does not exist: {root}")
        buckets = defaultdict(list)
        issues = []
        for path in sorted(item for item in root.rglob(pattern) if item.is_file()):
            try:
                stream = obspy.read(path, headonly=True)
                if len(stream) != 1:
                    raise ValueError("SAC file must contain exactly one trace")
                trace = stream[0]
                identity = WaveformIdentity.from_trace(trace)
                station_key = (identity.network, identity.station)
                if stations is not None and station_key not in stations:
                    continue
                record = WaveformRecord(
                    path, identity, trace.stats.starttime, trace.stats.endtime
                )
                for day in _covered_days(record.starttime, record.endtime):
                    buckets[(*station_key, day)].append(record)
            except Exception as exc:
                issues.append(ArchiveIndexIssue(path, f"{type(exc).__name__}: {exc}"))
        frozen = {key: tuple(records) for key, records in buckets.items()}
        return cls(frozen, issues)

    @classmethod
    def from_records(cls, records) -> "WaveformArchiveIndex":
        buckets = defaultdict(list)
        for record in records:
            key = (record.identity.network, record.identity.station)
            for day in _covered_days(record.starttime, record.endtime):
                buckets[(*key, day)].append(record)
        return cls({key: tuple(items) for key, items in buckets.items()})

    @property
    def station_keys(self) -> tuple[tuple[str, str], ...]:
        return tuple(sorted({key[:2] for key in self._buckets}))

    def records_for_station(self, network, station) -> tuple[WaveformRecord, ...]:
        records = {}
        for key, items in self._buckets.items():
            if key[:2] == (network, station):
                records.update((item.path, item) for item in items)
        return tuple(sorted(records.values(), key=lambda item: item.path))

    def overlapping(
        self, network: str, station: str, starttime, endtime
    ) -> tuple[WaveformRecord, ...]:
        """Return records whose header time spans intersect the requested window."""
        unique = {}
        for day in _covered_days(starttime, endtime):
            for record in self._buckets.get((network, station, day), ()):
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
