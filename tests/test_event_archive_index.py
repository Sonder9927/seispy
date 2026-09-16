from pathlib import Path
from unittest.mock import patch

import numpy as np
from obspy import Trace, UTCDateTime

from seispy.archive import WaveformIdentity
from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.cutting import cut_event_station, cut_event_waveforms


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


def test_index_finds_previous_day_file_by_actual_time_overlap(tmp_path):
    archive_root = tmp_path / "archive"
    path = _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")

    records = index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])

    assert [record.path for record in records] == [path]


def test_index_returns_cross_day_file_only_once(tmp_path):
    archive_root = tmp_path / "archive"
    path = _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")

    records = index.overlapping(
        "NZ",
        "WEL",
        UTCDateTime("2025-01-01T23:59:55"),
        UTCDateTime("2025-01-02T00:00:05"),
    )

    assert [record.path for record in records] == [path]


def test_index_uses_headers_in_an_arbitrary_directory_tree(tmp_path):
    archive_root = tmp_path / "archive"
    original = _archived_sac(archive_root)
    arbitrary = archive_root / "unrelated" / "names" / "waveform.bin"
    arbitrary.parent.mkdir(parents=True)
    original.rename(arbitrary)

    index = WaveformArchiveIndex.build(archive_root, pattern="*")

    records = index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])
    assert [record.path for record in records] == [arbitrary]


def test_index_separates_same_station_code_in_different_networks(tmp_path):
    archive_root = tmp_path / "archive"
    nz = _archived_sac(archive_root, network="NZ")
    iu = _archived_sac(archive_root, network="IU")

    index = WaveformArchiveIndex.build(archive_root)

    assert [
        item.path
        for item in index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])
    ] == [nz]
    assert [
        item.path
        for item in index.overlapping("IU", "WEL", _event()["start"], _event()["end"])
    ] == [iu]


def test_repeated_queries_do_not_reread_headers(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root)
    from seispy.event import archive_index as index_module

    with patch.object(
        index_module.obspy, "read", wraps=index_module.obspy.read
    ) as read:
        index = WaveformArchiveIndex.build(archive_root / "NZ")
        index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])
        index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])

    assert read.call_count == 1


def test_waveform_reader_reuses_full_file_without_sharing_mutations(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")
    record = index.overlapping("NZ", "WEL", _event()["start"], _event()["end"])[0]
    reader = WaveformReader(max_files=1)
    from seispy.event import archive_index as index_module

    with patch.object(
        index_module.obspy, "read", wraps=index_module.obspy.read
    ) as read:
        first = reader.read(record)
        first[0].data[:] = 0
        second = reader.read(record)

    assert read.call_count == 1
    assert np.any(second[0].data)


def test_cut_keeps_same_channel_different_locations_separate(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root, location="10")
    _archived_sac(archive_root, location="20")
    index = WaveformArchiveIndex.build(archive_root / "NZ")

    result = cut_event_station(
        _event(),
        {"network": "NZ", "station": "WEL"},
        tmp_path / "events",
        archive_index=index,
    )

    outputs = sorted((tmp_path / "events").rglob("*.sac"))
    assert result.succeeded == 1
    assert result.outputs == 2
    assert any(".10.BHZ.sac" in path.name for path in outputs)
    assert any(".20.BHZ.sac" in path.name for path in outputs)


def test_cut_preserves_gaps_as_named_segments(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root, location="", starttime="2025-01-02T00:00:00", npts=2)
    _archived_sac(archive_root, location="", starttime="2025-01-02T00:00:04", npts=2)
    index = WaveformArchiveIndex.build(archive_root)

    result = cut_event_station(
        _event(),
        {"network": "NZ", "station": "WEL"},
        tmp_path / "events",
        archive_index=index,
    )

    outputs = sorted((tmp_path / "events").rglob("*.sac"))
    assert result.outputs == 2
    assert all("NZ.WEL.--.BHZ.T" in path.name for path in outputs)
    assert [path.parent.parts[-2:] for path in outputs] == [
        ("NZ", "WEL"),
        ("NZ", "WEL"),
    ]


def test_public_cutting_workflow_uses_arbitrary_source_layout(tmp_path):
    archive_root = tmp_path / "source"
    original = _archived_sac(archive_root)
    arbitrary = archive_root / "flat" / "input.sac"
    arbitrary.parent.mkdir(parents=True)
    original.rename(arbitrary)
    second = _archived_sac(archive_root, station="ABC")
    second.rename(archive_root / "flat" / "input-2.sac")
    catalog = tmp_path / "events.csv"
    catalog.write_text(
        "time,latitude,longitude,depth,mag\n2025-01-02T00:00:00.123Z,1,2,3,4\n",
        encoding="utf-8",
    )
    output = tmp_path / "events"

    summary = cut_event_waveforms(
        archive_root,
        catalog,
        output_dir=output,
        time_window=5,
        max_workers=2,
        save_report=False,
        save_log=False,
    )

    assert summary.succeeded == 2
    assert summary.outputs_written == 2
    assert (output / "20250102T000000123" / "NZ" / "WEL").is_dir()
    assert (output / "20250102T000000123" / "NZ" / "ABC").is_dir()


def test_existing_event_output_is_not_overwritten(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")
    args = (
        _event(),
        {"network": "NZ", "station": "WEL"},
        tmp_path / "events",
    )
    first = cut_event_station(*args, archive_index=index)
    output = next((tmp_path / "events").rglob("*.sac"))
    original = output.read_bytes()

    second = cut_event_station(*args, archive_index=index)

    assert first.outputs == 1
    assert second.failed == 1
    assert second.outputs == 0
    assert output.read_bytes() == original
