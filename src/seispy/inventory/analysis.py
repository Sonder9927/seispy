"""Read-only, network-agnostic analysis of ObsPy inventories."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from obspy import UTCDateTime, read_inventory
from obspy.core.inventory import Inventory

InventorySource = str | Path | Inventory
SuitabilityStatus = Literal["ready", "conditional", "unsafe"]
IssueSeverity = Literal["info", "warning", "error"]
IssueScope = Literal["download", "response", "both"]


@dataclass(frozen=True)
class InventoryIssue:
    """One machine-readable finding from an inventory audit."""

    code: str
    severity: IssueSeverity
    scope: IssueScope
    message: str
    recommendation: str | None = None
    network: str | None = None
    station: str | None = None
    location: str | None = None
    channel: str | None = None
    starttime: UTCDateTime | None = None
    endtime: UTCDateTime | None = None


@dataclass(frozen=True)
class InventorySuitability:
    """Suitability of an inventory for one consuming operation."""

    status: SuitabilityStatus
    issues: tuple[InventoryIssue, ...]
    requires_waveform_check: bool = False

    @property
    def is_safe(self) -> bool:
        """Whether no blocking metadata error was found."""
        return self.status != "unsafe"

    def require_safe(self, purpose: str) -> None:
        """Raise a compact error for blocking findings."""
        errors = [item for item in self.issues if item.severity == "error"]
        if errors:
            detail = "; ".join(f"{item.code}: {item.message}" for item in errors[:5])
            extra = "" if len(errors) <= 5 else f"; and {len(errors) - 5} more"
            raise ValueError(f"inventory is unsafe for {purpose}: {detail}{extra}")


@dataclass(frozen=True)
class InventoryAnalysis:
    """Normalized inventory facts, statistics, and use-specific advice."""

    summary: dict[str, int]
    channel_epochs: pd.DataFrame
    stations: pd.DataFrame
    channel_codes: pd.DataFrame
    sample_rates: pd.DataFrame
    location_changes: pd.DataFrame
    location_overlaps: pd.DataFrame
    component_sets: pd.DataFrame
    download_suitability: InventorySuitability
    response_suitability: InventorySuitability


_EPOCH_COLUMNS = [
    "network",
    "station",
    "location",
    "channel",
    "latitude",
    "longitude",
    "elevation",
    "depth",
    "sample_rate",
    "starttime",
    "endtime",
    "has_response",
    "has_sensitivity",
]


def analyze_inventory(
    source: InventorySource,
    *,
    starttime: Any | None = None,
    endtime: Any | None = None,
) -> InventoryAnalysis:
    """Analyze StationXML metadata within the half-open interval ``[start, end)``.

    The function accepts a StationXML path or an ObsPy ``Inventory`` and never
    mutates it.  It is independent of any data centre or network convention.
    """
    inventory = _load_inventory(source)
    start = _optional_time(starttime)
    end = _optional_time(endtime)
    if start is not None and end is not None and start >= end:
        raise ValueError("starttime must be earlier than endtime")
    epochs = _epoch_table(inventory, start, end)
    changes, overlaps = _location_tables(epochs)
    issues = (*_audit_epochs(epochs), *_location_issues(changes, overlaps))
    download = _suitability(issues, "download", requires_waveform_check=False)
    response = _suitability(issues, "response", requires_waveform_check=True)
    stations = _counts(epochs, ["network", "station"], "channel_epochs")
    channel_codes = _counts(epochs, ["channel"], "channel_epochs")
    sample_rates = _counts(epochs, ["sample_rate"], "channel_epochs")
    components = _component_sets(epochs)
    summary = {
        "networks": int(epochs["network"].nunique()) if not epochs.empty else 0,
        "stations": int(epochs[["network", "station"]].drop_duplicates().shape[0]),
        "station_epochs": _station_epoch_count(inventory, start, end),
        "unique_nslc": int(
            epochs[["network", "station", "location", "channel"]]
            .drop_duplicates()
            .shape[0]
        ),
        "channel_epochs": len(epochs),
    }
    return InventoryAnalysis(
        summary,
        epochs,
        stations,
        channel_codes,
        sample_rates,
        changes,
        overlaps,
        components,
        download,
        response,
    )


def _load_inventory(source: InventorySource) -> Inventory:
    if isinstance(source, Inventory):
        return source
    if not isinstance(source, (str, Path)):
        raise TypeError("source must be a StationXML path or ObsPy Inventory")
    path = Path(source)
    if not path.is_file():
        raise FileNotFoundError(path)
    try:
        return read_inventory(str(path), format="STATIONXML")
    except Exception as exc:
        raise ValueError(f"cannot parse StationXML {path}: {exc}") from exc


def _optional_time(value: Any | None) -> UTCDateTime | None:
    if value is None:
        return None
    try:
        return UTCDateTime(value)
    except Exception as exc:
        raise ValueError(f"invalid inventory time: {value!r}") from exc


def _overlaps(start, end, window_start, window_end) -> bool:
    return not (
        (window_end is not None and start is not None and start >= window_end)
        or (window_start is not None and end is not None and end <= window_start)
    )


def _epoch_table(inventory, window_start, window_end) -> pd.DataFrame:
    rows = []
    for network in inventory:
        for station in network:
            for channel in station:
                if not _overlaps(
                    channel.start_date, channel.end_date, window_start, window_end
                ):
                    continue
                response = channel.response
                rows.append(
                    {
                        "network": network.code,
                        "station": station.code,
                        "location": channel.location_code or "",
                        "channel": channel.code,
                        "latitude": channel.latitude,
                        "longitude": channel.longitude,
                        "elevation": channel.elevation,
                        "depth": channel.depth,
                        "sample_rate": channel.sample_rate,
                        "starttime": channel.start_date,
                        "endtime": channel.end_date,
                        "has_response": response is not None,
                        "has_sensitivity": bool(
                            response is not None
                            and response.instrument_sensitivity is not None
                        ),
                    }
                )
    table = pd.DataFrame(rows, columns=_EPOCH_COLUMNS)
    if table.empty:
        return table
    table["_sort_start"] = table.starttime.map(_start_number)
    return table.sort_values(
        ["network", "station", "location", "channel", "_sort_start"],
        ignore_index=True,
    ).drop(columns="_sort_start")


def _counts(table, keys, name):
    if table.empty:
        return pd.DataFrame(columns=[*keys, name])
    return (
        table.groupby(keys, dropna=False)
        .size()
        .rename(name)
        .reset_index()
        .sort_values(keys, ignore_index=True)
    )


def _station_epoch_count(inventory, start, end):
    return sum(
        _overlaps(station.start_date, station.end_date, start, end)
        for network in inventory
        for station in network
    )


def _end_number(value):
    return float("inf") if value is None or pd.isna(value) else value.timestamp


def _start_number(value):
    return float("-inf") if value is None or pd.isna(value) else value.timestamp


def _epochs_overlap(left, right):
    return _start_number(right.starttime) < _end_number(left.endtime)


def _audit_epochs(table: pd.DataFrame) -> tuple[InventoryIssue, ...]:
    issues: list[InventoryIssue] = []
    if table.empty:
        return (
            InventoryIssue(
                "NO_CHANNEL_IN_TIME_WINDOW",
                "error",
                "both",
                "no channel epoch intersects the requested time window",
                "Select another time range or inventory.",
            ),
        )
    keys = ["network", "station", "location", "channel"]
    for key, group in table.groupby(keys, dropna=False):
        ordered = sorted(
            group.itertuples(), key=lambda row: _start_number(row.starttime)
        )
        for left, right in zip(ordered, ordered[1:], strict=False):
            if _epochs_overlap(left, right):
                rate_conflict = left.sample_rate != right.sample_rate
                issues.append(
                    InventoryIssue(
                        "CONFLICTING_SAMPLE_RATES"
                        if rate_conflict
                        else "OVERLAPPING_CHANNEL_EPOCHS",
                        "error",
                        "both",
                        f"overlapping epochs for {'.'.join(key)}"
                        + (" have different sample rates" if rate_conflict else ""),
                        "Correct or further select the StationXML before processing.",
                        *key,
                        right.starttime,
                        left.endtime,
                    )
                )
        for row in ordered:
            if (
                row.starttime is not None
                and row.endtime is not None
                and row.starttime >= row.endtime
            ):
                issues.append(
                    InventoryIssue(
                        "INVALID_EPOCH",
                        "error",
                        "both",
                        f"channel epoch start is not earlier than end for {'.'.join(key)}",
                        "Correct the channel epoch.",
                        *key,
                        row.starttime,
                        row.endtime,
                    )
                )
            if (
                row.sample_rate is None
                or pd.isna(row.sample_rate)
                or row.sample_rate <= 0
            ):
                issues.append(
                    InventoryIssue(
                        "INVALID_SAMPLE_RATE",
                        "error",
                        "both",
                        f"invalid sample rate for {'.'.join(key)}",
                        "Add a positive sample rate.",
                        *key,
                        row.starttime,
                        row.endtime,
                    )
                )
            if not row.has_response:
                issues.append(
                    InventoryIssue(
                        "MISSING_RESPONSE",
                        "error",
                        "response",
                        f"response is missing for {'.'.join(key)}",
                        "Use response-level StationXML before removing response.",
                        *key,
                        row.starttime,
                        row.endtime,
                    )
                )
            elif not row.has_sensitivity:
                issues.append(
                    InventoryIssue(
                        "MISSING_SENSITIVITY",
                        "warning",
                        "response",
                        f"instrument sensitivity is missing for {'.'.join(key)}",
                        "Verify that the response stages can still be evaluated.",
                        *key,
                        row.starttime,
                        row.endtime,
                    )
                )
    return tuple(issues)


def _suitability(issues, purpose, requires_waveform_check):
    relevant = tuple(item for item in issues if item.scope in {purpose, "both"})
    if any(item.severity == "error" for item in relevant):
        status = "unsafe"
    elif requires_waveform_check:
        status = "conditional"
    else:
        status = "ready"
    return InventorySuitability(status, relevant, requires_waveform_check)


def _location_tables(table):
    columns = ["network", "station", "channel", "locations", "overlap"]
    rows = []
    if not table.empty:
        for key, group in table.groupby(["network", "station", "channel"]):
            locations = tuple(sorted(group.location.unique()))
            if len(locations) < 2:
                continue
            records = list(group.itertuples())
            overlap = any(
                left.location != right.location
                and _start_number(left.starttime) < _end_number(right.endtime)
                and _start_number(right.starttime) < _end_number(left.endtime)
                for index, left in enumerate(records)
                for right in records[index + 1 :]
            )
            rows.append((*key, locations, overlap))
    frame = pd.DataFrame(rows, columns=columns)
    return frame[~frame.overlap].reset_index(drop=True), frame[
        frame.overlap
    ].reset_index(drop=True)


def _location_issues(changes, overlaps):
    issues = [
        InventoryIssue(
            "MULTIPLE_SEQUENTIAL_LOCATIONS",
            "info",
            "both",
            f"{row.network}.{row.station}.{row.channel} uses sequential locations {row.locations}",
            "Use each exact location only during its StationXML validity interval.",
            row.network,
            row.station,
            channel=row.channel,
        )
        for row in changes.itertuples()
    ]
    issues.extend(
        InventoryIssue(
            "SIMULTANEOUS_LOCATIONS",
            "warning",
            "both",
            f"{row.network}.{row.station}.{row.channel} has simultaneous locations {row.locations}",
            "Treat them as distinct data streams and keep location in filenames.",
            row.network,
            row.station,
            channel=row.channel,
        )
        for row in overlaps.itertuples()
    )
    return tuple(issues)


def _component_sets(table):
    columns = [
        "network",
        "station",
        "location",
        "band_instrument",
        "sample_rate",
        "starttime",
        "endtime",
        "components",
        "component_count",
    ]
    rows = []
    if not table.empty:
        work = table.copy()
        work["band_instrument"] = work.channel.str[:2]
        work["component"] = work.channel.str[-1:]
        for key, group in work.groupby(
            ["network", "station", "location", "band_instrument", "sample_rate"],
            dropna=False,
        ):
            boundaries = sorted(
                {
                    value.timestamp
                    for value in (*group.starttime, *group.endtime)
                    if value is not None and not pd.isna(value)
                }
            )
            segment_starts = [None, *[UTCDateTime(value) for value in boundaries]]
            segment_ends = [*[UTCDateTime(value) for value in boundaries], None]
            for segment_start, segment_end in zip(
                segment_starts, segment_ends, strict=True
            ):
                active = group[
                    [
                        _overlaps(
                            row.starttime, row.endtime, segment_start, segment_end
                        )
                        for row in group.itertuples()
                    ]
                ]
                if active.empty:
                    continue
                components = "".join(sorted(active.component.unique()))
                rows.append(
                    (*key, segment_start, segment_end, components, len(components))
                )
    return pd.DataFrame(rows, columns=columns).sort_values(
        columns[:5], ignore_index=True
    )
