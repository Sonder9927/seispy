from pathlib import Path
from unittest.mock import patch

import numpy as np
from obspy import Trace, UTCDateTime

from seispy.archive import WaveformIdentity
from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.cutting import cut_event_station


def _archived_sac(
    root: Path,
    *,
    location="10",
    channel="BHZ",
    starttime="2025-01-01T23:59:50",
    npts=21,
):
    trace = Trace(data=np.arange(npts, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = "WEL"
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

    records = index.overlapping("WEL", _event()["start"], _event()["end"])

    assert [record.path for record in records] == [path]


def test_index_returns_cross_day_file_only_once(tmp_path):
    archive_root = tmp_path / "archive"
    path = _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")

    records = index.overlapping(
        "WEL",
        UTCDateTime("2025-01-01T23:59:55"),
        UTCDateTime("2025-01-02T00:00:05"),
    )

    assert [record.path for record in records] == [path]


def test_repeated_queries_do_not_reread_headers(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root)
    from seispy.event import archive_index as index_module

    with patch.object(
        index_module.obspy, "read", wraps=index_module.obspy.read
    ) as read:
        index = WaveformArchiveIndex.build(archive_root / "NZ")
        index.overlapping("WEL", _event()["start"], _event()["end"])
        index.overlapping("WEL", _event()["start"], _event()["end"])

    assert read.call_count == 1


def test_waveform_reader_reuses_full_file_without_sharing_mutations(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")
    record = index.overlapping("WEL", _event()["start"], _event()["end"])[0]
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
        {"station": "WEL"},
        archive_root / "NZ",
        tmp_path / "events",
        archive_index=index,
    )

    outputs = sorted((tmp_path / "events").rglob("*.sac"))
    assert result.succeeded == 1
    assert result.outputs == 2
    assert any(".10.BHZ.sac" in path.name for path in outputs)
    assert any(".20.BHZ.sac" in path.name for path in outputs)


def test_existing_event_output_is_not_overwritten(tmp_path):
    archive_root = tmp_path / "archive"
    _archived_sac(archive_root)
    index = WaveformArchiveIndex.build(archive_root / "NZ")
    args = (
        _event(),
        {"station": "WEL"},
        archive_root / "NZ",
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
