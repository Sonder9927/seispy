import csv
from pathlib import Path
from typing import Any

from obspy.clients.fdsn import Client
from obspy.core.inventory import Inventory

EARTHSCOPE_URL = "https://service.earthscope.org"


def download_inventory(
    output_file: str | Path | None = None,
    *,
    client: str = EARTHSCOPE_URL,
    username: str | None = None,
    password: str | None = None,
    level: str = "response",
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
        **query: Additional filters accepted by ``Client.get_stations``.

    Returns:
        The downloaded ObsPy inventory.

    Raises:
        ValueError: If only one credential is supplied.

    Examples:
        >>> inventory = download_inventory(
        ...     "stations.xml", network="NZ", station="WEL", channel="BH?"
        ... )
        >>> len(inventory.networks) >= 0
        True
    """
    fdsn = _client(client, username, password)
    inventory = fdsn.get_stations(level=level, **query)
    if output_file is not None:
        path = Path(output_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        inventory.write(str(path), format="STATIONXML")
        _write_station_csv(inventory, path.with_suffix(".csv"))
    return inventory


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
