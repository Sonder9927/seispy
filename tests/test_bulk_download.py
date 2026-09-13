"""Bulk waveform-download contracts."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from obspy import UTCDateTime
from obspy.core.inventory import Inventory, Network, Site, Station

from seispy.download import bulk


def _inventory():
    station = Station(
        code="WEL",
        latitude=-41,
        longitude=174,
        elevation=10,
        site=Site(name="Wellington"),
    )
    return Inventory([Network(code="NZ", stations=[station])], source="test")


def test_mass_download_builds_daily_flat_storage_and_restrictions(tmp_path):
    downloader = Mock()

    def download(domain, restrictions, **kwargs):
        filename = kwargs["mseed_storage"](
            "NZ",
            "WEL",
            "10",
            "BHZ",
            UTCDateTime("2025-01-01"),
            UTCDateTime("2025-01-02"),
        )
        path = Path(filename)
        path.parent.mkdir(parents=True)
        path.write_bytes(b"mseed")
        stationxml = Path(
            kwargs["stationxml_storage"].format(network="NZ", station="WEL")
        )
        stationxml.write_text("xml")

    downloader.download.side_effect = download
    domain = object()
    with patch.object(bulk, "MassDownloader", return_value=downloader) as factory:
        result = bulk.mass_download_waveforms(
            tmp_path / "waveforms",
            "2025-01-01",
            "2025-01-03",
            domain=domain,
            providers="GEONET",
            network="NZ",
            station="WEL",
            location="10",
            channel="BHZ",
            threads_per_client=2,
        )

    factory.assert_called_once_with(providers=["GEONET"])
    args, kwargs = downloader.download.call_args
    restrictions = args[1]
    assert args[0] is domain
    assert restrictions.network == "NZ"
    assert restrictions.station == "WEL"
    assert restrictions.chunklength == 86_400
    assert kwargs["threads_per_client"] == 2
    assert result.mseed_files == (
        result.output_dir
        / "NZ"
        / "WEL"
        / "2025"
        / "NZ.WEL.10.BHZ.2025.001.000000-2025002T000000.mseed",
    )
    assert len(result.stationxml_files) == 1
    assert result.status == "completed"
    assert result.report_path.is_file()
    assert result.log_path.is_file()


def test_mass_download_defaults_to_global_domain(tmp_path):
    downloader = Mock()
    with (
        patch.object(bulk, "MassDownloader", return_value=downloader),
        patch.object(bulk, "GlobalDomain", return_value="global") as global_domain,
    ):
        bulk.mass_download_waveforms(tmp_path, "2025-01-01", "2025-01-02", network="NZ")

    global_domain.assert_called_once_with()
    assert downloader.download.call_args.args[0] == "global"


def test_mass_download_uses_supplied_inventory_as_station_limit(tmp_path):
    downloader = Mock()
    inventory = _inventory()
    with patch.object(bulk, "MassDownloader", return_value=downloader):
        bulk.mass_download_waveforms(
            tmp_path,
            "2025-01-01",
            "2025-01-02",
            inventory=inventory,
        )

    restrictions = downloader.download.call_args.args[1]
    assert restrictions.limit_stations_to_inventory == {("NZ", "WEL")}


def test_mass_download_reads_stationxml_inventory(tmp_path):
    downloader = Mock()
    inventory = _inventory()
    stationxml = tmp_path / "input.xml"
    with (
        patch.object(bulk, "MassDownloader", return_value=downloader),
        patch.object(bulk, "read_inventory", return_value=inventory) as read,
    ):
        bulk.mass_download_waveforms(
            tmp_path / "output",
            "2025-01-01",
            "2025-01-02",
            inventory=stationxml,
        )

    read.assert_called_once_with(str(stationxml), format="STATIONXML")
    restrictions = downloader.download.call_args.args[1]
    assert restrictions.limit_stations_to_inventory == {("NZ", "WEL")}


def test_mass_download_validates_options_before_creating_downloader(tmp_path):
    cases = [
        ({"minimum_length": 1.1}, "minimum_length"),
        ({"chunklength_in_sec": 0}, "chunklength_in_sec"),
        ({"threads_per_client": 0}, "threads_per_client"),
        ({}, "unrestricted global download"),
    ]
    for kwargs, message in cases:
        with (
            patch.object(bulk, "MassDownloader") as downloader,
            pytest.raises(ValueError, match=message),
        ):
            bulk.mass_download_waveforms(tmp_path, "2025-01-01", "2025-01-02", **kwargs)
        downloader.assert_not_called()
