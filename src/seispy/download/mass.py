"""Experimental ObsPy MassDownloader integration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

from obspy import UTCDateTime, read_inventory
from obspy.clients.fdsn.mass_downloader import (
    GlobalDomain,
    MassDownloader,
    Restrictions,
)
from obspy.core.inventory import Inventory


@dataclass(frozen=True)
class MassDownloadResult:
    """Files present after an experimental mass-download run."""

    output_dir: Path
    stationxml_dir: Path
    mseed_files: tuple[Path, ...]
    stationxml_files: tuple[Path, ...]


def download_waveforms_mass(
    output_dir: str | Path,
    starttime: Any,
    endtime: Any,
    *,
    domain: Any | None = None,
    providers: str | Iterable[Any] | None = None,
    network: str | None = None,
    station: str | None = None,
    location: str | None = None,
    channel: str | None = None,
    inventory: str | Path | Inventory | None = None,
    chunklength_in_sec: float | None = 86_400,
    reject_channels_with_gaps: bool = False,
    minimum_length: float = 0.0,
    sanitize: bool = True,
    minimum_interstation_distance_in_m: float = 0.0,
    stationxml_dir: str | Path | None = None,
    download_chunk_size_in_mb: float = 20,
    threads_per_client: int = 3,
    print_report: bool = True,
) -> MassDownloadResult:
    """Download continuous MiniSEED with ObsPy's ``MassDownloader``.

    This experimental interface stores channel chunks under the flattened
    ``network/station/year`` archive. Unlike :func:`download_waveforms`, one
    MiniSEED file is produced per location, channel, and requested time chunk.

    Args:
        output_dir: Root directory for downloaded MiniSEED files.
        starttime: Inclusive download start accepted by ``UTCDateTime``.
        endtime: Exclusive download end accepted by ``UTCDateTime``.
        domain: ObsPy mass-downloader domain; defaults to ``GlobalDomain``.
        providers: Provider name, iterable of names, or configured FDSN clients.
        network: Optional FDSN network selector.
        station: Optional FDSN station selector.
        location: Optional FDSN location selector.
        channel: Optional FDSN channel selector.
        inventory: Optional StationXML path or ObsPy inventory limiting eligible
            stations. MassDownloader still acquires the response metadata it
            needs into ``stationxml_dir``.
        chunklength_in_sec: Requested file duration; defaults to one UTC day.
        reject_channels_with_gaps: Reject chunks containing gaps or overlaps.
        minimum_length: Required fraction of the requested chunk, from 0 to 1.
        sanitize: Require matching StationXML for downloaded waveform channels.
        minimum_interstation_distance_in_m: Minimum station separation.
        stationxml_dir: StationXML root; defaults to ``output_dir/stationxml``.
        download_chunk_size_in_mb: Approximate bulk request size per thread.
        threads_per_client: Download threads used for each provider.
        print_report: Ask ObsPy to print its final acquisition report.

    Returns:
        Output roots and all MiniSEED and StationXML files present afterward.

    Raises:
        ValueError: If times, sizes, thread count, or completeness are invalid.

    Examples:
        ```python
        result = download_waveforms_mass(
            "waveforms", "2025-01-01", "2025-01-03",
            providers="GEONET", network="NZ", station="WEL", channel="BH?",
        )
        print(len(result.mseed_files))
        ```
    """
    start, end = UTCDateTime(starttime), UTCDateTime(endtime)
    if start >= end:
        raise ValueError("starttime must be earlier than endtime")
    if chunklength_in_sec is not None and chunklength_in_sec <= 0:
        raise ValueError("chunklength_in_sec must be positive or None")
    if not 0 <= minimum_length <= 1:
        raise ValueError("minimum_length must be between 0 and 1")
    if download_chunk_size_in_mb <= 0:
        raise ValueError("download_chunk_size_in_mb must be positive")
    if threads_per_client < 1:
        raise ValueError("threads_per_client must be at least 1")
    if minimum_interstation_distance_in_m < 0:
        raise ValueError("minimum_interstation_distance_in_m cannot be negative")
    if domain is None and network is None and station is None and inventory is None:
        raise ValueError(
            "provide a domain, network, station, or inventory to avoid an "
            "unrestricted global download"
        )

    output = Path(output_dir).expanduser().resolve()
    stationxml = (
        Path(stationxml_dir).expanduser().resolve()
        if stationxml_dir is not None
        else output / "stationxml"
    )
    output.mkdir(parents=True, exist_ok=True)
    stationxml.mkdir(parents=True, exist_ok=True)

    restrictions = Restrictions(
        starttime=start,
        endtime=end,
        chunklength_in_sec=chunklength_in_sec,
        network=network,
        station=station,
        location=location,
        channel=channel,
        limit_stations_to_inventory=_load_inventory(inventory),
        reject_channels_with_gaps=reject_channels_with_gaps,
        minimum_length=minimum_length,
        sanitize=sanitize,
        minimum_interstation_distance_in_m=minimum_interstation_distance_in_m,
    )
    downloader = MassDownloader(providers=_normalize_providers(providers))
    downloader.download(
        domain if domain is not None else GlobalDomain(),
        restrictions,
        mseed_storage=_mseed_storage(output),
        stationxml_storage=str(stationxml / "{network}.{station}.xml"),
        download_chunk_size_in_mb=download_chunk_size_in_mb,
        threads_per_client=threads_per_client,
        print_report=print_report,
    )
    return MassDownloadResult(
        output,
        stationxml,
        tuple(sorted(output.rglob("*.mseed"))),
        tuple(sorted(stationxml.rglob("*.xml"))),
    )


def _normalize_providers(providers):
    if providers is None:
        return None
    if isinstance(providers, str):
        return [providers]
    return list(providers)


def _load_inventory(source):
    if source is None or isinstance(source, Inventory):
        return source
    return read_inventory(str(source), format="STATIONXML")


def _mseed_storage(root: Path):
    def storage(network, station, location, channel, starttime, endtime):
        year = int(starttime.year)
        julday = int(starttime.julday)
        start = starttime.strftime("%H%M%S")
        end = endtime.strftime("%Y%jT%H%M%S")
        filename = (
            f"{network}.{station}.{location}.{channel}.{year}.{julday:03d}."
            f"{start}-{end}.mseed"
        )
        return str(root / network / station / str(year) / filename)

    return storage
