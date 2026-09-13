"""Canonical waveform identity and archive paths.

Waveform headers are authoritative.  Directory names and filenames are derived
indexes and must never be used to infer a trace's identity.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterable

_QUALITY_HEADER_PREFIX = "SQ:"


def _trace_quality(trace: Any) -> str:
    stats = trace.stats
    try:
        return str(stats.mseed.dataquality)
    except (AttributeError, KeyError):
        pass
    for key in ("kuser0", "kuser1", "kuser2"):
        try:
            stored = str(stats.sac[key]).strip()
        except (AttributeError, KeyError):
            continue
        if stored.startswith(_QUALITY_HEADER_PREFIX):
            return stored.removeprefix(_QUALITY_HEADER_PREFIX)
    return "D"


def preserve_sac_quality(trace: Any) -> None:
    """Persist MiniSEED quality in a SAC character header before writing."""
    quality = _trace_quality(trace)
    try:
        sac = trace.stats.sac
    except (AttributeError, KeyError):
        trace.stats.sac = {}
        sac = trace.stats.sac
    value = f"{_QUALITY_HEADER_PREFIX}{quality}"
    for key in ("kuser0", "kuser1", "kuser2"):
        stored = str(sac.get(key, "")).strip()
        if not stored or stored.startswith(_QUALITY_HEADER_PREFIX):
            sac[key] = value
            return
    raise ValueError("no free SAC kuser header is available for data quality")


@dataclass(frozen=True)
class WaveformIdentity:
    """Header-derived identity for one waveform trace."""

    network: str
    station: str
    location: str
    channel: str
    quality: str
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
            _trace_quality(trace),
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
    def day_key(self) -> tuple[str, str, str, str, str, int, int]:
        return (
            self.network,
            self.station,
            self.location,
            self.channel,
            self.quality,
            self.year,
            self.julday,
        )

    def sac_filename(self, *, merged: bool = False) -> str:
        final = "merged" if merged else self.starttime.strftime("%H%M%S")
        return (
            f"{self.network}.{self.station}.{self.location}.{self.channel}."
            f"{self.quality}.{self.year}.{self.julday:03d}.{final}.sac"
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
    identities = [WaveformIdentity.from_trace(trace) for trace in stream]
    if not identities:
        raise ValueError("waveform stream is empty")
    keys = {(item.network, item.station, item.year, item.julday) for item in identities}
    if len(keys) != 1:
        raise ValueError("waveform stream contains multiple stations or start days")
    return keys.pop()


def mseed_path(root: str | Path, stream: Iterable[Any]) -> Path:
    """Build a canonical daily MiniSEED path from stream headers."""
    network, station, year, julday = stream_day_identity(stream)
    filename = f"{network}.{station}.{year}.{julday:03d}.mseed"
    return Path(root) / network / station / str(year) / filename


def matches_mseed_path(
    path: str | Path, root: str | Path, stream: Iterable[Any]
) -> bool:
    """Validate standard daily or MassDownloader channel-chunk paths."""
    traces = list(stream)
    network, station, year, julday = stream_day_identity(traces)
    candidate = Path(path)
    if candidate == mseed_path(root, traces):
        return True
    identities = [WaveformIdentity.from_trace(trace) for trace in traces]
    stream_keys = {
        (item.network, item.station, item.location, item.channel) for item in identities
    }
    if len(stream_keys) != 1:
        return False
    _, _, location, channel = stream_keys.pop()
    directory = Path(root) / network / station / str(year)
    prefix = f"{network}.{station}.{location}.{channel}.{year}.{julday:03d}."
    return (
        candidate.parent == directory
        and candidate.name.startswith(prefix)
        and candidate.suffix.lower() == ".mseed"
    )
