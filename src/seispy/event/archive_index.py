"""One-pass header index for continuous SAC/MiniSEED event cutting."""

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
    delta: float = 0.0
    trace_index: int = 0

    @property
    def key(self):
        """Identify a source trace independently of its day buckets."""
        return self.path, self.trace_index


@dataclass(frozen=True)
class ArchiveIndexIssue:
    path: Path
    error: str


class WaveformArchiveIndex:
    """Immutable NS/day index built with one header read per waveform file."""

    def __init__(self, buckets, issues=()):
        self._buckets = buckets
        self.issues = tuple(issues)
        stations = defaultdict(dict)
        for key, records in buckets.items():
            stations[key[:2]].update((record.key, record) for record in records)
        self._stations = {
            key: tuple(sorted(records.values(), key=lambda item: item.key))
            for key, records in stations.items()
        }
        self._padding = {
            key: max(record.delta for record in records) / 2
            for key, records in self._stations.items()
        }

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
                if not stream:
                    raise ValueError("Waveform file contains no traces")
                records = []
                for trace_index, trace in enumerate(stream):
                    if trace.stats.npts == 0:
                        continue
                    identity = WaveformIdentity.from_trace(trace)
                    station_key = (identity.network, identity.station)
                    if stations is not None and station_key not in stations:
                        continue
                    records.append(
                        WaveformRecord(
                            path,
                            identity,
                            trace.stats.starttime,
                            trace.stats.endtime,
                            trace.stats.delta,
                            trace_index,
                        )
                    )
                for record in records:
                    station_key = (record.identity.network, record.identity.station)
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
        return tuple(sorted(self._stations))

    def records_for_station(self, network, station) -> tuple[WaveformRecord, ...]:
        return self._stations.get((network, station), ())

    def overlapping(
        self, network: str, station: str, starttime, endtime
    ) -> tuple[WaveformRecord, ...]:
        """Return records whose header time spans intersect the requested window."""
        unique = {}
        padding = self._padding.get((network, station), 0)
        for day in _covered_days(starttime - padding, endtime + padding):
            for record in self._buckets.get((network, station, day), ()):
                if intervals_overlap(
                    record.starttime,
                    record.endtime,
                    starttime - record.delta / 2,
                    endtime + record.delta / 2,
                ):
                    unique[record.key] = record
        return tuple(sorted(unique.values(), key=lambda item: item.key))


class WaveformReader:
    """Bounded LRU cache for full waveform reads during chronological cuts."""

    def __init__(self, max_files: int = 8, max_bytes: int = 256 * 1024**2):
        if max_files < 1:
            raise ValueError("max_files must be at least 1")
        if max_bytes < 0:
            raise ValueError("max_bytes cannot be negative")
        self._max_files = max_files
        self._max_bytes = max_bytes
        self._cache = OrderedDict()
        self._cached_bytes = 0

    @property
    def cached_bytes(self):
        """Bytes of sample arrays retained in the cache, excluding metadata."""
        return self._cached_bytes

    def read(
        self, record: WaveformRecord, *, starttime=None, endtime=None, padding_samples=0
    ):
        """Return independent samples, copying only the requested window.

        Full reads remain available for overlap comparisons. A record larger
        than the byte budget is read but never retained. The budget controls
        retained arrays, not peak memory during reads, copies, or merging.
        """
        try:
            stream = self._cache.pop(record.path)
        except KeyError:
            stream = obspy.read(record.path)
            for trace in stream:
                trace.data.flags.writeable = False
            size = sum(trace.data.nbytes for trace in stream)
            if size <= self._max_bytes:
                self._cache[record.path] = stream
                self._cached_bytes += size
        else:
            self._cache[record.path] = stream
        while (
            len(self._cache) > self._max_files or self._cached_bytes > self._max_bytes
        ):
            _, removed = self._cache.popitem(last=False)
            self._cached_bytes -= sum(trace.data.nbytes for trace in removed)
        if record.trace_index >= len(stream):
            raise ValueError("Waveform trace disappeared after archive indexing")
        trace = stream[record.trace_index]
        if (
            WaveformIdentity.from_trace(trace) != record.identity
            or trace.stats.starttime != record.starttime
            or trace.stats.endtime != record.endtime
            or (record.delta and trace.stats.delta != record.delta)
        ):
            raise ValueError("Waveform trace headers changed after archive indexing")
        selected = obspy.Stream(traces=[trace])
        if starttime is None and endtime is None:
            return selected.copy()
        result = obspy.Stream()
        for trace in selected:
            padding = padding_samples * trace.stats.delta
            first = starttime - padding if starttime is not None else None
            last = endtime + padding if endtime is not None else None
            # Trace.slice copies metadata but shares sample storage. Copy only
            # after slicing so callers can mutate without touching the cache.
            result += trace.slice(first, last, nearest_sample=True).copy()
        return result


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
