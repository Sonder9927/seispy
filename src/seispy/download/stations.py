"""Download and normalize station metadata from FDSN providers."""

import csv
import warnings
from pathlib import Path
from typing import Any

from obspy.clients.fdsn import Client
from obspy.core.inventory import Inventory

EARTHSCOPE_URL = "https://service.earthscope.org"


class ResponseConflictError(ValueError):
    """Response metadata cannot be normalized without choosing arbitrarily."""


class ResponseConflictWarning(UserWarning):
    """Raw response metadata was saved because normalization was ambiguous."""


def download_inventory(
    output_file: str | Path | None = None,
    *,
    client: str = EARTHSCOPE_URL,
    username: str | None = None,
    password: str | None = None,
    level: str = "response",
    strict_response_conflicts: bool = False,
    **query: Any,
) -> Inventory:
    """Download station metadata from an FDSN service.

    Args:
        output_file: Optional destination for a StationXML copy. A station
            summary with a ``.csv`` suffix is also written.
        client: ObsPy FDSN client name or service URL.
        username: Username for restricted services.
        password: Password for restricted services.
        level: FDSN metadata detail level.
        strict_response_conflicts: Raise after saving the raw StationXML when
            response conflicts cannot be resolved safely. By default a warning
            is emitted and the raw inventory is returned and saved.
        **query: Additional filters accepted by ``Client.get_stations``.

    Returns:
        The downloaded ObsPy inventory.

    Raises:
        ValueError: If only one credential is supplied.

    Examples:
        ```python
        inventory = download_inventory(
            "data/metadata/stations.xml",
            network="NZ", station="WEL", channel="BH?",
        )
        len(inventory.networks) >= 0
        # => True
        ```
    """
    fdsn = _client(client, username, password)
    downloaded = fdsn.get_stations(level=level, **query)
    inventory = downloaded
    conflict = None
    if level.lower() == "response":
        try:
            inventory = _normalize_response_epochs(downloaded)
        except ResponseConflictError as exc:
            conflict = exc
    if output_file is not None:
        path = Path(output_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        inventory.write(str(path), format="STATIONXML")
        _write_station_csv(inventory, path.with_suffix(".csv"))
    if conflict is not None:
        if strict_response_conflicts:
            raise conflict
        warnings.warn(
            f"{conflict}; raw StationXML was preserved without normalization",
            ResponseConflictWarning,
            stacklevel=2,
        )
    return inventory


def _normalize_response_epochs(inventory: Inventory) -> Inventory:
    """Resolve deterministic channel-epoch duplicates on an inventory copy."""
    normalized = inventory.copy()
    networks = {}
    for network in normalized.networks:
        existing = networks.get(network.code)
        if existing is None:
            networks[network.code] = network
            continue
        existing.stations.extend(network.stations)
        existing.start_date = _earliest_start(existing.start_date, network.start_date)
        existing.end_date = _latest_end(existing.end_date, network.end_date)
    normalized.networks = list(networks.values())

    for network in normalized:
        stations = {}
        for station in network.stations:
            existing = stations.get(station.code)
            if existing is None:
                stations[station.code] = station
                continue
            existing.channels.extend(station.channels)
            existing.start_date = _earliest_start(
                existing.start_date, station.start_date
            )
            existing.end_date = _latest_end(existing.end_date, station.end_date)
        network.stations = list(stations.values())
        for station in network:
            groups = {}
            for channel in station.channels:
                key = (channel.location_code or "", channel.code)
                groups.setdefault(key, []).append(channel)
            channels = []
            for epochs in groups.values():
                ordered = sorted(
                    epochs,
                    key=lambda item: (
                        float("-inf")
                        if item.start_date is None
                        else item.start_date.timestamp
                    ),
                )
                resolved = []
                for epoch in ordered:
                    if not resolved:
                        resolved.append(epoch)
                        continue
                    previous = resolved[-1]
                    same_start = previous.start_date == epoch.start_date
                    overlaps = (
                        previous.end_date is None
                        or epoch.start_date is None
                        or epoch.start_date <= previous.end_date
                    )
                    equivalent = _responses_equivalent(previous, epoch)
                    if same_start and not equivalent:
                        seed_id = (
                            f"{network.code}.{station.code}."
                            f"{epoch.location_code}.{epoch.code}"
                        )
                        raise ResponseConflictError(
                            f"conflicting responses share the same start time for "
                            f"{seed_id} at {epoch.start_date}"
                        )
                    if overlaps and equivalent:
                        previous.end_date = _latest_end(
                            previous.end_date, epoch.end_date
                        )
                        continue
                    if overlaps:
                        if epoch.start_date is None:
                            raise ResponseConflictError(
                                "cannot resolve overlapping open response epochs"
                            )
                        previous.end_date = epoch.start_date - 1e-6
                    resolved.append(epoch)
                channels.extend(resolved)
            station.channels = sorted(
                channels,
                key=lambda item: (
                    item.location_code or "",
                    item.code,
                    float("-inf")
                    if item.start_date is None
                    else item.start_date.timestamp,
                ),
            )
    return normalized


def _responses_equivalent(left, right) -> bool:
    return left.response == right.response and left.sample_rate == right.sample_rate


def _latest_end(left, right):
    if left is None or right is None:
        return None
    return max(left, right)


def _earliest_start(left, right):
    if left is None or right is None:
        return None
    return min(left, right)


def _write_station_csv(inventory: Inventory, path: str | Path) -> Path:
    """Write one searchable summary row for each StationXML station epoch."""
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fields = (
        "network",
        "station",
        "station_name",
        "latitude",
        "longitude",
        "elevation_m",
        "start_date",
        "end_date",
        "locations",
        "channels",
    )
    with destination.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for network in inventory:
            for station in network:
                channels = sorted({item.code for item in station.channels})
                locations = sorted(
                    {item.location_code or "--" for item in station.channels}
                )
                writer.writerow(
                    {
                        "network": network.code,
                        "station": station.code,
                        "station_name": getattr(station.site, "name", "") or "",
                        "latitude": station.latitude,
                        "longitude": station.longitude,
                        "elevation_m": station.elevation,
                        "start_date": _format_time(station.start_date),
                        "end_date": _format_time(station.end_date),
                        "locations": ",".join(locations),
                        "channels": ",".join(channels),
                    }
                )
    return destination


def _format_time(value: Any) -> str:
    return "" if value is None else value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _client(base_url, username=None, password=None):
    if (username is None) != (password is None):
        raise ValueError("username and password must be provided together")
    kwargs = {"user": username, "password": password} if username else {}
    return Client(base_url, **kwargs)
