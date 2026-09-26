"""Canonical waveform identity and archive paths.

Waveform headers are authoritative.  Directory names and filenames are derived
indexes and must never be used to infer a trace's identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
import re
from typing import Any, Iterable

from obspy import UTCDateTime


def _start_day(starttime: Any) -> date:
    """Return one start time's calendar day without requiring obspy types."""
    day = getattr(starttime, "date", None)
    if isinstance(day, date):
        return day
    return date(int(starttime.year), 1, 1) + timedelta(days=int(starttime.julday) - 1)


def coverage_day(stats: Any) -> date:
    """Return the UTC day holding the plurality of a trace's samples.

    A contiguous, uniformly sampled trace belongs to the day it predominantly
    covers, so a few seconds of pre-roll before midnight (or post-roll after it)
    never detach the trace from its day of record. The plurality day equals the
    day of the trace's median sample. A trace with no samples or no usable
    sampling rate falls back to its start day.
    """
    npts = int(getattr(stats, "npts", 0) or 0)
    sampling_rate = float(getattr(stats, "sampling_rate", 0.0) or 0.0)
    if npts <= 0 or sampling_rate <= 0.0:
        return _start_day(stats.starttime)
    median = UTCDateTime(stats.starttime) + (npts // 2) / sampling_rate
    return median.date


def start_day_token(starttime: Any, archive_day: date) -> str:
    """Return a julian-day marker when the first sample precedes its archive day.

    The marker is empty when the first sample already falls on the archive day;
    otherwise it is a compact prefix such as 126T, so a file carrying archive day
    127 is not misread as starting on day 127.
    """
    start_day = _start_day(starttime)
    if start_day == archive_day:
        return ""
    return f"{start_day.timetuple().tm_yday:03d}T"


def _window_coverage_day(starttime: Any, endtime: Any) -> date:
    """Return the UTC day a contiguous coverage window predominantly spans."""
    start = UTCDateTime(starttime)
    end = UTCDateTime(endtime)
    if end <= start:
        return start.date
    return (start + (end - start) / 2).date


@dataclass(frozen=True)
class WaveformIdentity:
    """Header-derived identity for one waveform trace.

    archive_day is the UTC day the trace predominantly covers (see the
    coverage_day helper). It drives the day slot of every derived path, while
    starttime still supplies the exact first-sample token.
    """

    network: str
    station: str
    location: str
    channel: str
    starttime: Any
    archive_day: date | None = None

    @classmethod
    def from_trace(cls, trace: Any) -> "WaveformIdentity":
        """Build an identity from a trace without consulting its filename."""
        stats = trace.stats
        return cls(
            str(stats.network),
            str(stats.station),
            str(stats.location),
            str(stats.channel),
            stats.starttime,
            coverage_day(stats),
        )

    @property
    def year(self) -> int:
        return self.day.year

    @property
    def julday(self) -> int:
        return int(self.day.timetuple().tm_yday)

    @property
    def day(self) -> date:
        return self.archive_day or _start_day(self.starttime)

    @property
    def day_key(self) -> tuple[str, str, str, str, int, int]:
        return (
            self.network,
            self.station,
            self.location,
            self.channel,
            self.year,
            self.julday,
        )

    def sac_filename(self, *, merged: bool = False) -> str:
        if merged:
            final = "merged"
        else:
            final = start_day_token(self.starttime, self.day) + self.starttime.strftime(
                "%H%M%S"
            )
        return (
            f"{self.network}.{self.station}.{self.location}.{self.channel}."
            f"{self.year}.{self.julday:03d}.{final}.sac"
        )

    def sac_path(self, root: str | Path, *, merged: bool = False) -> Path:
        return (
            Path(root)
            / self.network
            / self.station
            / str(self.year)
            / self.sac_filename(merged=merged)
        )

    def matches_sac_path(self, path: str | Path, root: str | Path) -> bool:
        candidate = Path(path)
        return candidate == self.sac_path(root) or candidate == self.sac_path(
            root, merged=True
        )


def stream_day_identity(stream: Iterable[Any]) -> tuple[str, str, int, int]:
    """Return the single station-day represented by a stream.

    All traces must start on the same UTC day and belong to the same network and
    station.  MiniSEED channel and location diversity is allowed.
    """
    traces = list(stream)
    if not traces:
        raise ValueError("waveform stream is empty")
    identities = [
        WaveformIdentity.from_trace(trace)
        for trace in traces
        if getattr(trace.stats, "npts", None) != 0
    ]
    if not identities:
        raise ValueError("waveform stream contains no samples")
    keys = {(item.network, item.station, item.year, item.julday) for item in identities}
    if len(keys) != 1:
        raise ValueError("waveform stream contains multiple stations or start days")
    return keys.pop()


def mseed_path(root: str | Path, stream: Iterable[Any]) -> Path:
    """Build a canonical daily MiniSEED path from stream headers."""
    network, station, year, julday = stream_day_identity(stream)
    filename = f"{network}.{station}.{year}.{julday:03d}.mseed"
    return Path(root) / network / station / str(year) / filename


def channel_mseed_path(
    root: str | Path,
    network: str,
    station: str,
    location: str,
    channel: str,
    starttime: Any,
    endtime: Any,
    *,
    archive_day: date | None = None,
    start_token: str | None = None,
) -> Path:
    """Build a self-describing path for one NSLC chunk.

    The filename retains the complete NSLC and archive day so it remains
    meaningful outside its archive directory. The julian-day field is the archive
    day the chunk predominantly covers; a partial-day chunk adds its first-sample
    clock as a stable slot identifier. start_token overrides that token (for
    example with a short HHMMSS); the precise sub-second token is used when it is
    omitted. When archive_day is omitted it is derived from the coverage window.
    Actual data coverage remains authoritative in the MiniSEED headers and is
    never claimed by the filename.
    """
    location = location or "--"
    day = archive_day or _window_coverage_day(starttime, endtime)
    julday = int(day.timetuple().tm_yday)
    prefix = f"{network}.{station}.{location}.{channel}.{day.year}.{julday:03d}"
    if _is_full_utc_day(starttime, endtime):
        filename = f"{prefix}.mseed"
    else:
        start = _time_token(starttime, day) if start_token is None else start_token
        filename = f"{prefix}.{start}.mseed"
    return Path(root) / network / station / str(day.year) / filename


def _is_full_utc_day(starttime: Any, endtime: Any) -> bool:
    start_day = date(int(starttime.year), 1, 1) + timedelta(
        days=int(starttime.julday) - 1
    )
    end_day = date(int(endtime.year), 1, 1) + timedelta(days=int(endtime.julday) - 1)
    return (
        starttime.strftime("%H%M%S") == "000000"
        and endtime.strftime("%H%M%S") == "000000"
        and _fractional_nanoseconds(starttime) == 0
        and _fractional_nanoseconds(endtime) == 0
        and end_day == start_day + timedelta(days=1)
    )


def _time_token(value: Any, archive_day: date | None = None) -> str:
    token = value.strftime("%H%M%S")
    if archive_day is not None:
        token = start_day_token(value, archive_day) + token
    nanoseconds = _fractional_nanoseconds(value)
    if nanoseconds:
        token += f".{nanoseconds:09d}".rstrip("0")
    return token


def _fractional_nanoseconds(value: Any) -> int:
    if hasattr(value, "ns"):
        return int(value.ns) % 1_000_000_000
    return int(getattr(value, "microsecond", 0)) * 1_000


def matches_mseed_path(
    path: str | Path, root: str | Path, stream: Iterable[Any]
) -> bool:
    """Validate standard daily or exact-channel MiniSEED paths."""
    traces = list(stream)
    network, station, year, julday = stream_day_identity(traces)
    candidate = Path(path)
    if candidate == mseed_path(root, traces):
        return True
    identities = [
        WaveformIdentity.from_trace(trace)
        for trace in traces
        if getattr(trace.stats, "npts", None) != 0
    ]
    stream_keys = {
        (item.network, item.station, item.location, item.channel) for item in identities
    }
    if len(stream_keys) != 1:
        return False
    _, _, location, channel = stream_keys.pop()
    directory = Path(root) / network / station / str(year)
    chunk_prefix = (
        f"{network}.{station}.{location or '--'}.{channel}.{year}.{julday:03d}"
    )
    partial_pattern = (
        rf"{re.escape(chunk_prefix)}\."
        rf"(?:\d{{6}}|\d{{3}}T\d{{6}})(?:\.\d{{1,9}})?\.mseed"
    )
    return (
        candidate.parent == directory
        and candidate.suffix.lower() == ".mseed"
        and (
            candidate.name == f"{chunk_prefix}.mseed"
            or re.fullmatch(partial_pattern, candidate.name) is not None
        )
    )
