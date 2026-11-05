import pytest
from pathlib import Path
from unittest.mock import patch

import numpy as np
from obspy import Trace, UTCDateTime

from seispy.archive import WaveformIdentity
from seispy.event.archive_index import WaveformArchiveIndex
from seispy.event.cutting import cut_event_station


def _archived_sac(
    root: Path,
    *,
    location="10",
    channel="BHZ",
    starttime="2025-01-01T23:59:50",
    npts=21,
    network="NZ",
    station="WEL",
):
    trace = Trace(data=np.arange(npts, dtype=np.float32))
    trace.stats.network = network
    trace.stats.station = station
    trace.stats.location = location
    trace.stats.channel = channel
    trace.stats.starttime = UTCDateTime(starttime)
    trace.stats.sampling_rate = 1
    destination = WaveformIdentity.from_trace(trace).sac_path(root)
    destination.parent.mkdir(parents=True, exist_ok=True)
    trace.write(str(destination), format="SAC")
    return destination


def _event():
    start = UTCDateTime("2025-01-02T00:00:00")
    return {
        "start": start,
        "end": start + 5,
        "latitude": 1,
        "longitude": 2,
        "depth": 3,
        "mag": 4,
    }


@pytest.mark.parametrize(
    "scenario",
    [
        "index_finds_previous_day_file_by_actual_time_overlap",
        "index_returns_cross_day_file_only_once",
        "index_uses_headers_in_an_arbitrary_directory_tree",
        "index_separates_same_station_code_in_different_networks",
        "repeated_queries_do_not_reread_headers",
    ],
)
def test_index_lookup(tmp_path, scenario):
    archive_root = tmp_path / "archive"
    if scenario == "index_finds_previous_day_file_by_actual_time_overlap":
        path = _archived_sac(archive_root)
        index = WaveformArchiveIndex.build(archive_root / "NZ")

        records = index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])

        assert [record.path for record in records] == [path]
    elif scenario == "index_returns_cross_day_file_only_once":
        path = _archived_sac(archive_root)
        index = WaveformArchiveIndex.build(archive_root / "NZ")

        records = index.overlapping(
            "NZ",
            "WEL",
            UTCDateTime("2025-01-01T23:59:55"),
            UTCDateTime("2025-01-02T00:00:05"),
        )

        assert [record.path for record in records] == [path]
    elif scenario == "index_uses_headers_in_an_arbitrary_directory_tree":
        original = _archived_sac(archive_root)
        arbitrary = archive_root / "unrelated" / "names" / "waveform.bin"
        arbitrary.parent.mkdir(parents=True)
        original.rename(arbitrary)

        index = WaveformArchiveIndex.build(archive_root, pattern="*")

        records = index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])
        assert [record.path for record in records] == [arbitrary]
    elif scenario == "index_separates_same_station_code_in_different_networks":
        nz = _archived_sac(archive_root, network="NZ")
        iu = _archived_sac(archive_root, network="IU")

        index = WaveformArchiveIndex.build(archive_root)

        assert [
            item.path
            for item in index.overlapping(
                "NZ", "WEL", _event()["start"], _event()["end"]
            )
        ] == [nz]
        assert [
            item.path
            for item in index.overlapping(
                "IU", "WEL", _event()["start"], _event()["end"]
            )
        ] == [iu]
    elif scenario == "repeated_queries_do_not_reread_headers":
        _archived_sac(archive_root)
        from seispy.event import archive_index as index_module

        with patch.object(
            index_module.obspy, "read", wraps=index_module.obspy.read
        ) as read:
            index = WaveformArchiveIndex.build(archive_root / "NZ")
            index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])
            index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])

        assert read.call_count == 1


@pytest.mark.parametrize("change", ["missing", "truncated", "samples", "window"])
def test_resume_validates_existing_and_fills_missing(tmp_path, change):
    root = tmp_path / "archive"
    _archived_sac(root)
    _archived_sac(root, channel="BHN")
    index = WaveformArchiveIndex.build(root)
    station = {"network": "NZ", "station": "WEL"}
    output = tmp_path / "events"
    cut_event_station(_event(), station, output, archive_index=index)
    paths = sorted(output.rglob("*.sac"))
    original = paths[1].read_bytes()
    event = _event()
    if change == "missing":
        paths[0].unlink()
    elif change == "truncated":
        paths[0].write_bytes(paths[0].read_bytes()[:640])
    elif change == "samples":
        import obspy

        trace = obspy.read(str(paths[0]))[0]
        trace.data[0] += 1
        trace.write(str(paths[0]), format="SAC")
    else:
        event["end"] += 1
    preserved = paths[0].read_bytes() if paths[0].exists() else None
    result = cut_event_station(event, station, output, archive_index=index)
    assert paths[1].read_bytes() == original
    if change == "missing":
        assert result.succeeded == result.outputs == result.existing_outputs == 1
        assert result.failed == result.skipped == result.existing_skipped == 0
        assert paths[0].exists()
    else:
        assert result.failed == 1
        assert result.outputs == result.skipped == result.existing_skipped == 0
        assert result.samples[0].status == "output_conflict"
        assert paths[0].read_bytes() == preserved
