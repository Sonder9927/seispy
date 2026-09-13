"""Station inventory selection, combination, and writing operations."""

from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import obspy
from obspy import UTCDateTime
from obspy.core.inventory import Inventory
from seispy.workflow import commit_output, temporary_output_path

InventorySource = str | Path | Inventory


def combine_inventories(
    sources: Iterable[InventorySource],
    output_file: str | Path | None = None,
    *,
    channel_starttime: Any = None,
    overwrite: bool = False,
) -> Inventory:
    """Combine inventory files or objects without mutating inputs.

    Args:
        sources: StationXML paths or ObsPy inventories.
        output_file: Optional StationXML destination.
        channel_starttime: Optional start date assigned to every channel.
        overwrite: Whether an existing output file may be replaced.

    Returns:
        A new combined ObsPy inventory.

    Raises:
        ValueError: If no inventory is supplied or the result is empty.

    Examples:
        ```python
        combined = combine_inventories(["broadband.xml", "short-period.xml"])
        len(combined.networks) >= 0
        # => True
        ```
    """
    inventories = [_read_inventory(source) for source in sources]
    if not inventories:
        raise ValueError("at least one inventory is required")
    combined = Inventory(networks=[], source="SeisPy")
    for inventory in inventories:
        combined += inventory.copy()
    if not len(combined):
        raise ValueError("the combined inventory contains no networks")
    if channel_starttime is not None:
        combined = shift_channel_starttime(combined, channel_starttime)
    if output_file is not None:
        write_inventory(combined, output_file, overwrite=overwrite)
    return combined


def select_inventory(
    source: InventorySource,
    *,
    networks: Sequence[str] | None = None,
    stations: Sequence[str] | None = None,
    locations: Sequence[str] | None = None,
    channels: Sequence[str] | None = None,
    output_file: str | Path | None = None,
    overwrite: bool = False,
) -> Inventory:
    """Select exact metadata codes from every network in an inventory.

    Args:
        source: StationXML path or ObsPy inventory.
        networks: Network codes to retain, or all networks when omitted.
        stations: Station codes to retain, or all stations when omitted.
        locations: Location codes to retain, or all locations when omitted.
        channels: Channel codes to retain, or all channels when omitted.
        output_file: Optional StationXML destination.
        overwrite: Whether an existing output file may be replaced.

    Returns:
        A new inventory containing the selected channels.

    Examples:
        ```python
        selected = select_inventory(
            "stations.xml", stations=["WEL"], channels=["BHZ"]
        )
        ```
    """
    inventory = _read_inventory(source)
    network_codes = set(networks) if networks is not None else None
    station_codes = set(stations) if stations is not None else None
    location_codes = set(locations) if locations is not None else None
    channel_codes = set(channels) if channels is not None else None
    selected = Inventory(networks=[], source=inventory.source)
    for network in inventory:
        if network_codes is not None and network.code not in network_codes:
            continue
        new_network = network.copy()
        new_network.stations = []
        for station in network:
            if station_codes is not None and station.code not in station_codes:
                continue
            new_station = station.copy()
            new_station.channels = [
                channel.copy()
                for channel in station.channels
                if (location_codes is None or channel.location_code in location_codes)
                and (channel_codes is None or channel.code in channel_codes)
            ]
            if new_station.channels:
                new_network.stations.append(new_station)
        if new_network.stations:
            selected.networks.append(new_network)
    if output_file is not None:
        write_inventory(selected, output_file, overwrite=overwrite)
    return selected


def shift_channel_starttime(source: InventorySource, starttime: Any) -> Inventory:
    """Set every channel start date on a copy of an inventory.

    Args:
        source: StationXML path or ObsPy inventory.
        starttime: Value accepted by ``UTCDateTime``, including a tuple.

    Returns:
        A modified copy; the source inventory is not changed.

    Examples:
        ```python
        shifted = shift_channel_starttime("stations.xml", "2020-01-01")
        ```
    """
    inventory = _read_inventory(source).copy()
    value = _to_utc(starttime)
    for network in inventory:
        for station in network:
            for channel in station:
                channel.start_date = value
    return inventory


def write_inventory(
    inventory: Inventory,
    output_file: str | Path,
    *,
    overwrite: bool = False,
    format: str = "STATIONXML",
) -> Path:
    """Write an inventory through a temporary file before committing it.

    Args:
        inventory: ObsPy inventory to serialize.
        output_file: Destination file path.
        overwrite: Whether an existing destination may be replaced.
        format: ObsPy inventory output format.

    Returns:
        The destination path.

    Examples:
        ```python
        path = write_inventory(inventory, "stations.xml", overwrite=True)
        ```
    """
    destination = Path(output_file)
    temporary = temporary_output_path(destination)
    try:
        inventory.write(str(temporary), format=format)
        commit_output(temporary, destination, overwrite=overwrite)
    except Exception:
        temporary.unlink(missing_ok=True)
        raise
    return destination


def _read_inventory(source: InventorySource) -> Inventory:
    return source if isinstance(source, Inventory) else obspy.read_inventory(source)


def _to_utc(value) -> UTCDateTime:
    if isinstance(value, UTCDateTime):
        return value
    if isinstance(value, (tuple, list)):
        return UTCDateTime(*value)
    return UTCDateTime(value)
