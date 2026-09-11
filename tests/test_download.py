from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
from obspy import UTCDateTime
from obspy import Stream as ObsPyStream
from obspy import Trace
from obspy.clients.fdsn.header import FDSNForbiddenException
from obspy.core.inventory import Channel, Inventory, Network, Site, Station
from obspy.core.inventory.response import InstrumentSensitivity, Response
import pytest

from seispy.download import events, inventory, waveform


def test_earthscope_client_receives_restricted_credentials():
    with patch.object(inventory, "Client") as client:
        inventory._client(inventory.EARTHSCOPE_URL, "user", "password")
    client.assert_called_once_with(
        "https://service.earthscope.org", user="user", password="password"
    )


def test_inventory_download_has_no_logging_and_writes_stationxml(tmp_path):
    inv = Mock()
    client = Mock()
    client.get_stations.return_value = inv
    with (
        patch.object(inventory, "_client", return_value=client),
        patch.object(inventory, "_normalize_response_epochs", return_value=inv),
        patch.object(inventory, "_write_station_csv") as write_csv,
    ):
        result = inventory.download_inventory(
            tmp_path / "inventory.xml", network="NZ", station="AAA"
        )
    assert result is inv
    inv.write.assert_called_once_with(
        str(tmp_path / "inventory.xml"), format="STATIONXML"
    )
    write_csv.assert_called_once_with(inv, tmp_path / "inventory.csv")


def test_inventory_conflict_preserves_raw_stationxml_by_default(tmp_path):
    raw = Mock()
    client = Mock()
    client.get_stations.return_value = raw
    conflict = inventory.ResponseConflictError("conflicting responses")
    with (
        patch.object(inventory, "_client", return_value=client),
        patch.object(inventory, "_normalize_response_epochs", side_effect=conflict),
        patch.object(inventory, "_write_station_csv") as write_csv,
        pytest.warns(inventory.ResponseConflictWarning, match="raw StationXML"),
    ):
        result = inventory.download_inventory(tmp_path / "inventory.xml")

    assert result is raw
    raw.write.assert_called_once_with(
        str(tmp_path / "inventory.xml"), format="STATIONXML"
    )
    write_csv.assert_called_once_with(raw, tmp_path / "inventory.csv")


def test_strict_inventory_conflict_raises_after_raw_stationxml_is_saved(tmp_path):
    raw = Mock()
    client = Mock()
    client.get_stations.return_value = raw
    conflict = inventory.ResponseConflictError("conflicting responses")
    with (
        patch.object(inventory, "_client", return_value=client),
        patch.object(inventory, "_normalize_response_epochs", side_effect=conflict),
        patch.object(inventory, "_write_station_csv") as write_csv,
        pytest.raises(inventory.ResponseConflictError, match="conflicting responses"),
    ):
        inventory.download_inventory(
            tmp_path / "inventory.xml", strict_response_conflicts=True
        )

    raw.write.assert_called_once_with(
        str(tmp_path / "inventory.xml"), format="STATIONXML"
    )
    write_csv.assert_called_once_with(raw, tmp_path / "inventory.csv")


def test_non_response_inventory_level_still_writes_only_stationxml(tmp_path):
    inv = Mock()
    client = Mock()
    client.get_stations.return_value = inv
    with (
        patch.object(inventory, "_client", return_value=client),
        patch.object(inventory, "_write_station_csv"),
    ):
        inventory.download_inventory(tmp_path / "stations.xml", level="station")

    inv.write.assert_called_once_with(
        str(tmp_path / "stations.xml"), format="STATIONXML"
    )


def test_station_csv_contains_coordinates_and_channel_summary(tmp_path):
    class Container(list):
        pass

    channel = SimpleNamespace(code="BHZ", location_code="")
    station = SimpleNamespace(
        code="AAA",
        site=SimpleNamespace(name="Alpha Station"),
        latitude=-41.1,
        longitude=174.8,
        elevation=123.0,
        start_date=UTCDateTime("2020-01-01"),
        end_date=None,
        channels=[channel],
    )
    network = Container([station])
    network.code = "NZ"
    inventory._write_station_csv([network], tmp_path / "station.csv")

    contents = (tmp_path / "station.csv").read_text()
    assert "network,station,station_name,latitude,longitude,elevation_m" in contents
    assert "NZ,AAA,Alpha Station,-41.1,174.8,123.0" in contents
    assert "--,BHZ" in contents


def _response(value):
    return Response(
        instrument_sensitivity=InstrumentSensitivity(
            value=value,
            frequency=1.0,
            input_units="M",
            output_units="COUNTS",
        )
    )


def _channel(start, end, response):
    return Channel(
        code="BHZ",
        location_code="",
        latitude=0,
        longitude=0,
        elevation=0,
        depth=0,
        azimuth=0,
        dip=-90,
        sample_rate=100,
        start_date=UTCDateTime(start),
        end_date=None if end is None else UTCDateTime(end),
        response=response,
    )


def _inventory_with_channels(channels):
    station = Station(
        code="AAA",
        latitude=0,
        longitude=0,
        elevation=0,
        site=Site(name="Alpha"),
        channels=channels,
    )
    return Inventory([Network(code="NZ", stations=[station])], source="test")


def test_inventory_normalization_merges_equivalent_overlapping_epochs():
    response = _response(1.0)
    original = _inventory_with_channels(
        [
            _channel("2024-01-01", "2024-08-01", response),
            _channel("2024-06-01", "2025-01-01", response),
        ]
    )

    normalized = inventory._normalize_response_epochs(original)

    assert len(original[0][0].channels) == 2
    assert len(normalized[0][0].channels) == 1
    assert normalized[0][0][0].end_date == UTCDateTime("2025-01-01")


def test_inventory_normalization_clips_old_conflicting_epoch():
    normalized = inventory._normalize_response_epochs(
        _inventory_with_channels(
            [
                _channel("2024-01-01", None, _response(1.0)),
                _channel("2024-06-01", None, _response(2.0)),
            ]
        )
    )

    assert normalized[0][0][0].end_date == UTCDateTime("2024-06-01") - 1e-6
    assert len(normalized[0][0].channels) == 2


def test_inventory_normalization_rejects_ambiguous_same_start():
    source = _inventory_with_channels(
        [
            _channel("2024-01-01", None, _response(1.0)),
            _channel("2024-01-01", None, _response(2.0)),
        ]
    )

    with pytest.raises(inventory.ResponseConflictError, match="conflicting responses"):
        inventory._normalize_response_epochs(source)


def test_inventory_normalization_collapses_duplicate_network_and_station_nodes():
    response = _response(1.0)
    first = _inventory_with_channels([_channel("2024-01-01", None, response)])
    duplicated = first + first.copy()

    normalized = inventory._normalize_response_epochs(duplicated)

    assert len(normalized.networks) == 1
    assert len(normalized[0].stations) == 1
    assert len(normalized[0][0].channels) == 1


def test_earthquake_events_are_returned_as_dataframe(tmp_path):
    origin = SimpleNamespace(
        time=UTCDateTime("2026-01-01"),
        longitude=1,
        latitude=2,
        depth=3000,
    )
    magnitude = SimpleNamespace(mag=4.5, magnitude_type="Mw")
    event = SimpleNamespace(
        preferred_origin=lambda: origin,
        preferred_magnitude=lambda: magnitude,
        origins=[origin],
        magnitudes=[magnitude],
    )
    client = Mock()
    client.get_events.return_value = [event]
    with patch.object(events, "Client", return_value=client):
        frame = events.download_earthquake_events(
            "2026-01-01", "2026-01-02", tmp_path / "events.csv"
        )
    assert len(frame) == 1
    assert frame.loc[0, "depth"] == 3
    assert (tmp_path / "events.csv").exists()


class _Stream(list):
    def get_gaps(self):
        return []

    def write(self, filename, format):
        assert format == "MSEED"
        ObsPyStream([item.as_obspy() for item in self]).write(filename, format=format)

    def merge(self, **kwargs):
        return self


class _SacTrace:
    def __init__(self, channel):
        self.stats = SimpleNamespace(
            network="NZ",
            station="AAA",
            location="10",
            channel=channel,
            starttime=UTCDateTime("2026-01-01"),
            mseed=SimpleNamespace(dataquality="D"),
        )

    def write(self, filename, format):
        assert format == "SAC"
        self.as_obspy().write(str(filename), format=format)

    def as_obspy(self):
        trace = Trace(data=np.arange(10, dtype=np.float32))
        trace.stats.network = self.stats.network
        trace.stats.station = self.stats.station
        trace.stats.location = self.stats.location
        trace.stats.channel = self.stats.channel
        trace.stats.starttime = self.stats.starttime
        return trace


def test_waveform_worker_reuses_authenticated_client_and_writes_mseed(tmp_path):
    client = Mock()
    client.get_waveforms.return_value = _Stream([_SacTrace("BHZ")])
    with patch.object(waveform, "_client", return_value=client) as factory:
        result = waveform._download_station(
            inventory.EARTHSCOPE_URL,
            "user",
            "password",
            tmp_path,
            "NZ",
            "AAA",
            "*",
            "BH?",
            [UTCDateTime("2026-01-01")],
            UTCDateTime("2026-01-02"),
            False,
            1,
        )
    factory.assert_called_once_with(inventory.EARTHSCOPE_URL, "user", "password")
    assert result.downloaded == 1
    assert result.files_written == 1
    assert (
        tmp_path / "NZ" / "AAA" / "2026" / "NZ.AAA.2026.001.mseed"
    ).is_file()


def test_waveform_worker_can_write_one_sac_per_channel(tmp_path):
    client = Mock()
    client.get_waveforms.return_value = _Stream([_SacTrace("BHZ"), _SacTrace("BHN")])
    with patch.object(waveform, "_client", return_value=client):
        result = waveform._download_station(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            tmp_path,
            "NZ",
            "AAA",
            "*",
            "BH?",
            [UTCDateTime("2026-01-01")],
            UTCDateTime("2026-01-02"),
            False,
            1,
            "sac",
        )
    assert result.downloaded == 1
    assert result.files_written == 2
    assert (
        tmp_path
        / "NZ"
        / "AAA"
        / "2026"
        / "NZ.AAA.10.BHZ.D.2026.001.000000.sac"
    ).is_file()


def test_download_rejects_trace_header_that_disagrees_with_request(tmp_path):
    day = UTCDateTime("2026-01-01")
    trace = _SacTrace("BHZ")
    trace.stats.station = "WRONG"

    with pytest.raises(ValueError, match="does not match request"):
        waveform._write_waveforms(
            _Stream([trace]),
            tmp_path,
            "NZ",
            "AAA",
            day,
            "sac",
            False,
        )


def test_invalid_waveform_format_is_rejected(tmp_path):
    with patch.object(waveform, "_client"):
        try:
            waveform.download_waveforms(
                tmp_path, "NZ", "2026-01-01", "2026-01-02", output_format="wav"
            )
        except ValueError as exc:
            assert "output_format" in str(exc)
        else:
            raise AssertionError("ValueError was not raised")


def test_existing_mseed_is_skipped_without_network_request(tmp_path):
    day = UTCDateTime("2026-01-01")
    destination = tmp_path / "NZ" / "AAA" / "2026" / "NZ.AAA.2026.001.mseed"
    destination.parent.mkdir(parents=True)
    _Stream([_SacTrace("BHZ")]).write(destination, "MSEED")

    with patch.object(waveform, "_fetch_waveforms") as fetch:
        result = waveform._download_day(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            tmp_path,
            "NZ",
            "AAA",
            "*",
            "BH?",
            day,
            day + 86400,
            False,
            1,
            "mseed",
            2,
            0,
        )

    fetch.assert_not_called()
    assert result.skipped == 1
    assert result.total == 1


def test_waveform_request_retries_transient_failure():
    client = Mock()
    client.get_waveforms.side_effect = [TimeoutError("temporary"), _Stream([object()])]

    with (
        patch.object(waveform, "_thread_client", return_value=client),
        patch.object(waveform.time, "sleep") as sleep,
        patch.object(waveform.random, "uniform", return_value=0),
    ):
        result = waveform._fetch_waveforms(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            "NZ",
            "AAA",
            "*",
            "BH?",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            2,
            0.5,
        )

    assert len(result) == 1
    assert client.get_waveforms.call_count == 2
    sleep.assert_called_once_with(0.5)


def test_waveform_request_does_not_retry_permanent_fdsn_failure():
    client = Mock()
    client.get_waveforms.side_effect = FDSNForbiddenException("forbidden")

    with (
        patch.object(waveform, "_thread_client", return_value=client),
        patch.object(waveform.time, "sleep") as sleep,
    ):
        try:
            waveform._fetch_waveforms(
                inventory.EARTHSCOPE_URL,
                None,
                None,
                "NZ",
                "AAA",
                "*",
                "BH?",
                UTCDateTime("2026-01-01"),
                UTCDateTime("2026-01-02"),
                2,
                0.5,
            )
        except FDSNForbiddenException:
            pass
        else:
            raise AssertionError("FDSNForbiddenException was not raised")

    client.get_waveforms.assert_called_once()
    sleep.assert_not_called()


def test_existing_sac_files_allow_network_free_resume_without_metadata_files(tmp_path):
    day = UTCDateTime("2026-01-01")
    stream = _Stream([_SacTrace("BHZ"), _SacTrace("BHN")])
    written, skipped = waveform._write_waveforms(
        stream, tmp_path, "NZ", "AAA", day, "sac", False, "*", "BH?"
    )

    assert written == 2
    assert not skipped
    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BH?")
    assert not waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "HH?")
    assert not list(tmp_path.rglob("*.json"))


def test_sac_check_requires_each_explicit_channel(tmp_path):
    day = UTCDateTime("2026-01-01")
    waveform._write_waveforms(
        _Stream([_SacTrace("BHZ")]),
        tmp_path,
        "NZ",
        "AAA",
        day,
        "sac",
        False,
        "*",
        "BHZ",
    )

    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BHZ")
    assert not waveform._day_is_complete(
        tmp_path, "NZ", "AAA", day, "sac", "*", "BHZ,BHN"
    )


def test_incomplete_sac_day_downloads_only_missing_files(tmp_path):
    day = UTCDateTime("2026-01-01")
    existing = waveform._sac_destination(_SacTrace("BHZ"), tmp_path)
    existing.parent.mkdir(parents=True)
    _SacTrace("BHZ").write(existing, "SAC")

    written, skipped = waveform._write_waveforms(
        _Stream([_SacTrace("BHZ"), _SacTrace("BHN")]),
        tmp_path,
        "NZ",
        "AAA",
        day,
        "sac",
        False,
        "*",
        "BH?",
    )

    assert written == 1
    assert not skipped
    assert existing.is_file()
    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BH?")


def test_download_waveforms_aggregates_station_day_tasks(tmp_path):
    def completed(*args, **kwargs):
        return waveform._Counts(total=1, downloaded=1, files_written=1)

    with (
        patch.object(waveform, "_client"),
        patch.object(waveform, "_download_day", side_effect=completed) as worker,
    ):
        summary = waveform.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-03",
            station=["AAA"],
            max_workers=2,
        )

    assert worker.call_count == 2
    assert summary.total == 2
    assert summary.downloaded == 2
    assert summary.files_written == 2
    assert summary.ok
    assert not replace(summary, no_data=1).ok


def test_waveform_inventory_manifest_replaces_remote_station_lookup(tmp_path):
    manifest = Mock()
    guided_tasks = (("AAA", UTCDateTime("2026-01-01")),)

    with (
        patch.object(waveform, "_client"),
        patch.object(waveform, "read_inventory", return_value=manifest) as read,
        patch.object(waveform, "_station_codes") as station_codes,
        patch.object(waveform, "_inventory_tasks", return_value=guided_tasks),
        patch.object(
            waveform,
            "_download_day",
            return_value=waveform._Counts(total=1, downloaded=1, files_written=1),
        ),
    ):
        summary = waveform.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-03",
            inventory=tmp_path / "stations.xml",
            max_workers=1,
        )

    read.assert_called_once_with(str(tmp_path / "stations.xml"), format="STATIONXML")
    station_codes.assert_not_called()
    assert summary.total == 1
