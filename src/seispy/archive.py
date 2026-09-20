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


@dataclass(frozen=True)
class WaveformIdentity:
    """Header-derived identity for one waveform trace."""

    network: str
    station: str
    location: str
    channel: str
    starttime: Any

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
        )

    @property
    def year(self) -> int:
        return int(self.starttime.year)

    @property
    def julday(self) -> int:
        return int(self.starttime.julday)

    @property
    def day(self) -> date:
        return date(self.year, 1, 1) + timedelta(days=self.julday - 1)

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
        final = "merged" if merged else self.starttime.strftime("%H%M%S")
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
) -> Path:
    """Build a self-describing path for one NSLC download chunk.

    The filename retains the complete NSLC and UTC day so it remains meaningful
    outside its archive directory. A partial-day chunk adds its requested start
    as a stable slot identifier. Actual data coverage remains authoritative in
    the MiniSEED headers and is never claimed by the filename.
    """
    location = location or "--"
    julday = int(starttime.julday)
    prefix = f"{network}.{station}.{location}.{channel}.{starttime.year}.{julday:03d}"
    if _is_full_utc_day(starttime, endtime):
        filename = f"{prefix}.mseed"
    else:
        start = _time_token(starttime)
        filename = f"{prefix}.{start}.mseed"
    return Path(root) / network / station / str(starttime.year) / filename


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


def _time_token(value: Any) -> str:
    token = value.strftime("%H%M%S")
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
    partial_pattern = rf"{re.escape(chunk_prefix)}\.\d{{6}}(?:\.\d{{1,9}})?\.mseed"
    return (
        candidate.parent == directory
        and candidate.suffix.lower() == ".mseed"
        and (
            candidate.name == f"{chunk_prefix}.mseed"
            or re.fullmatch(partial_pattern, candidate.name) is not None
        )
    )
