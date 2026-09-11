from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from obspy import UTCDateTime
from obspy.core.inventory import Channel, Inventory, Network, Site, Station
from obspy.core.inventory.response import InstrumentSensitivity, Response

from seispy.download import events, inventory


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
