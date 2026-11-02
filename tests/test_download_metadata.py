from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from obspy import UTCDateTime
from obspy.core.inventory import Channel, Inventory, Network, Site, Station
from obspy.core.inventory.response import InstrumentSensitivity, Response

from seispy.download import catalog, stations


def test_earthscope_client_receives_restricted_credentials():
    with patch.object(stations, "Client") as client:
        stations._client(stations.EARTHSCOPE_URL, "user", "password")
    client.assert_called_once_with(
        "https://service.earthscope.org", user="user", password="password"
    )


@pytest.mark.parametrize(
    "level,conflict,strict",
    [
        ("response", False, False),
        ("response", True, False),
        ("response", True, True),
        ("station", False, False),
        ("channel", False, False),
    ],
)
def test_stationxml_exports(tmp_path, level, conflict, strict):
    from contextlib import nullcontext

    active = _channel("2023-01-01", None, _response(1.0))
    expired = _channel("1970-01-01", "1980-01-01", _response(1.0))
    inventory = _inventory_with_channels([active, expired])
    station = inventory[0][0]
    station.site.name = "Alpha Station"
    station.latitude, station.longitude, station.elevation = -41.1, 174.8, 123.0
    historical = station.copy()
    historical.code = "001A"
    historical.channels = [expired.copy()]
    inventory[0].stations.append(historical)
    if conflict:
        station.channels.append(_channel("2023-01-01", None, _response(2.0)))
    client = Mock()
    client.get_stations.return_value = inventory
    output = tmp_path / "stations.xml"
    context = (
        pytest.raises(stations.ResponseConflictError, match="conflicting responses")
        if strict
        else pytest.warns(stations.ResponseConflictWarning, match="raw StationXML")
        if conflict
        else nullcontext()
    )
    with patch.object(stations, "_client", return_value=client), context:
        result = stations.download_inventory(
            output,
            level=level,
            strict_response_conflicts=strict,
            starttime="2010-01-01",
            endtime="2026-01-01",
        )
        assert result is not None
    # Both artifacts must already exist even when strict conflict handling raises.
    from obspy import read_inventory

    saved = read_inventory(str(output))
    assert len(saved[0][0].channels) == (
        3
        if level == "station" and conflict
        else 2
        if conflict or level == "station"
        else 1
    )
    csv_text = output.with_suffix(".csv").read_text()
    assert "network,station,station_name,latitude,longitude,elevation_m" in csv_text
    assert "NZ,AAA,Alpha Station,-41.1,174.8,123.0" in csv_text
    assert "channel_count" in csv_text
    assert ("001A" in csv_text) == (level == "station")
    assert len(inventory[0].stations) == 2
    assert len(inventory[0][0].channels) == (3 if conflict else 2)


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


@pytest.mark.parametrize(
    "scenario",
    [
        "inventory_normalization_merges_equivalent_overlapping_epochs",
        "inventory_normalization_clips_old_conflicting_epoch",
        "inventory_normalization_rejects_ambiguous_same_start",
        "inventory_normalization_collapses_duplicate_network_and_station_nodes",
    ],
)
def test_inventory_epochs(scenario):
    if scenario == "inventory_normalization_merges_equivalent_overlapping_epochs":
        response = _response(1.0)
        original = _inventory_with_channels(
            [
                _channel("2024-01-01", "2024-08-01", response),
                _channel("2024-06-01", "2025-01-01", response),
            ]
        )

        normalized = stations._normalize_response_epochs(original)

        assert len(original[0][0].channels) == 2
        assert len(normalized[0][0].channels) == 1
        assert normalized[0][0][0].end_date == UTCDateTime("2025-01-01")
    elif scenario == "inventory_normalization_clips_old_conflicting_epoch":
        normalized = stations._normalize_response_epochs(
            _inventory_with_channels(
                [
                    _channel("2024-01-01", None, _response(1.0)),
                    _channel("2024-06-01", None, _response(2.0)),
                ]
            )
        )

        assert normalized[0][0][0].end_date == UTCDateTime("2024-06-01") - 1e-6
        assert len(normalized[0][0].channels) == 2
    elif scenario == "inventory_normalization_rejects_ambiguous_same_start":
        source = _inventory_with_channels(
            [
                _channel("2024-01-01", None, _response(1.0)),
                _channel("2024-01-01", None, _response(2.0)),
            ]
        )

        with pytest.raises(
            stations.ResponseConflictError, match="conflicting responses"
        ):
            stations._normalize_response_epochs(source)
    elif (
        scenario
        == "inventory_normalization_collapses_duplicate_network_and_station_nodes"
    ):
        response = _response(1.0)
        first = _inventory_with_channels([_channel("2024-01-01", None, response)])
        duplicated = first + first.copy()

        normalized = stations._normalize_response_epochs(duplicated)

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
    with patch.object(catalog, "Client", return_value=client):
        frame = catalog.download_earthquake_events(
            "2026-01-01", "2026-01-02", tmp_path / "catalog.csv"
        )
    assert len(frame) == 1
    assert frame.loc[0, "depth"] == 3
    assert (tmp_path / "catalog.csv").exists()
