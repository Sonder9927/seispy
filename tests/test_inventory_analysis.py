from pathlib import Path

import pytest
from obspy import UTCDateTime
from obspy.core.inventory import Channel, Inventory, Network, Site, Station
from obspy.core.inventory.response import InstrumentSensitivity, Response

from seispy.inventory import analyze_inventory


def _channel(
    code="HHZ", *, location="10", rate=100.0, start=None, end=None, response=True
):
    metadata = None
    if response:
        metadata = Response(
            instrument_sensitivity=InstrumentSensitivity(1.0, 1.0, "M/S", "COUNTS")
        )
    return Channel(
        code=code,
        location_code=location,
        latitude=-40.0,
        longitude=175.0,
        elevation=10.0,
        depth=0.0,
        sample_rate=rate,
        start_date=UTCDateTime(start) if start else None,
        end_date=UTCDateTime(end) if end else None,
        response=metadata,
    )


def _inventory(*channels):
    station = Station(
        code="AAA",
        latitude=-40.0,
        longitude=175.0,
        elevation=10.0,
        site=Site(name="AAA"),
        channels=list(channels),
    )
    return Inventory([Network(code="NZ", stations=[station])], source="test")


def test_analyze_inventory_reports_counts_components_and_readiness():
    inventory = _inventory(_channel("HHZ"), _channel("HHN"), _channel("HHE"))

    report = analyze_inventory(inventory)

    assert report.summary == {
        "networks": 1,
        "stations": 1,
        "station_epochs": 1,
        "unique_nslc": 3,
        "channel_epochs": 3,
    }
    assert report.download_suitability.status == "ready"
    assert report.response_suitability.status == "conditional"
    assert report.response_suitability.requires_waveform_check
    assert report.component_sets.iloc[0].components == "ENZ"


def test_missing_response_only_blocks_response_removal():
    report = analyze_inventory(_inventory(_channel(response=False)))

    assert report.download_suitability.status == "ready"
    assert report.response_suitability.status == "unsafe"
    assert report.response_suitability.issues[0].code == "MISSING_RESPONSE"
    with pytest.raises(ValueError, match="MISSING_RESPONSE"):
        report.response_suitability.require_safe("response removal")


def test_location_sequence_is_not_misreported_as_simultaneous():
    report = analyze_inventory(
        _inventory(
            _channel(location="10", start="2020-01-01", end="2021-01-01"),
            _channel(location="20", start="2021-01-01", end=None),
        )
    )

    assert len(report.location_changes) == 1
    assert report.location_overlaps.empty
    assert report.download_suitability.status == "ready"


def test_overlapping_nslc_epochs_are_unsafe_for_both_consumers():
    report = analyze_inventory(
        _inventory(
            _channel(start="2020-01-01", end="2022-01-01"),
            _channel(start="2021-01-01", end=None),
        )
    )

    assert report.download_suitability.status == "unsafe"
    assert report.response_suitability.status == "unsafe"
    assert any(
        issue.code == "OVERLAPPING_CHANNEL_EPOCHS"
        for issue in report.download_suitability.issues
    )


def test_analysis_accepts_stationxml_path_and_half_open_window(tmp_path: Path):
    inventory = _inventory(
        _channel(start="2020-01-01", end="2021-01-01"),
        _channel(location="20", start="2021-01-01", end=None),
    )
    path = tmp_path / "stations.xml"
    inventory.write(path, format="STATIONXML")

    report = analyze_inventory(path, starttime="2021-01-01", endtime="2022-01-01")

    assert len(report.channel_epochs) == 1
    assert report.channel_epochs.iloc[0].location == "20"


def test_invalid_source_and_window_have_clear_errors(tmp_path):
    with pytest.raises(FileNotFoundError):
        analyze_inventory(tmp_path / "missing.xml")
    with pytest.raises(ValueError, match="earlier"):
        analyze_inventory(
            _inventory(_channel()), starttime="2022-01-01", endtime="2022-01-01"
        )
