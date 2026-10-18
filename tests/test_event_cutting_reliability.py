"""Written SAC correctness and bounded-read contracts for event cutting."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from obspy import Trace, UTCDateTime, read
from obspy.io.sac.util import get_sac_reftime

from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.cutting import cut_event_station, cut_event_waveforms
from seispy.event.catalog import write_event_catalog

BASE = UTCDateTime("2024-01-02")
STATION = {"network": "XX", "station": "TEST"}


def source_trace(root, name="input", *, offset=-10, npts=3001, rate=100, bias=0):
    root.mkdir(parents=True, exist_ok=True)
    trace = Trace(
        np.arange(npts, dtype=np.float32) + bias,
        header={
            **STATION,
            "channel": "BHZ",
            "location": "",
            "sampling_rate": rate,
            "starttime": BASE + offset,
        },
    )
    trace.stats.sac = {
        "o": -10.0,
        "a": 3.0,
        "t0": 4.0,
        "f": 5.0,
        "iztype": 9,
        "stla": 30.0,
        "stlo": 110.0,
        "cmpaz": 0.0,
        "cmpinc": 0.0,
    }
    path = root / f"{name}.sac"
    trace.write(str(path), format="SAC")
    return path


def event(offset=0.123456, duration=5):
    return {
        "start": BASE + offset,
        "end": BASE + offset + duration,
        "latitude": 35.0,
        "longitude": 105.0,
        "depth": 12.0,
        "mag": 6.0,
    }


@pytest.mark.parametrize("offset", [0.0, 0.123, 0.123456, 2.789])
def test_written_origin_and_picks_keep_absolute_times(tmp_path, offset):
    root = tmp_path / "source"
    path = source_trace(root)
    original = read(path)[0]
    original_bytes = path.read_bytes()
    index = WaveformArchiveIndex.build(root)
    target = event(offset)
    result = cut_event_station(
        target,
        STATION,
        tmp_path / "out",
        archive_index=index,
        waveform_reader=WaveformReader(),
    )
    assert result.succeeded == 1
    actual = read(next((tmp_path / "out").rglob("*.sac")))[0]
    reference = get_sac_reftime(actual.stats.sac)
    assert abs(reference + float(actual.stats.sac.o) - target["start"]) < 1e-6
    start = UTCDateTime(ns=target["start"].ns // 1_000_000_000 * 1_000_000_000)
    expected = original.copy().trim(start, start + 5)
    assert actual.stats.starttime == expected.stats.starttime
    assert actual.stats.endtime == expected.stats.endtime
    np.testing.assert_array_equal(actual.data, expected.data)
    for name in ("a", "t0", "f"):
        before = get_sac_reftime(original.stats.sac) + float(original.stats.sac[name])
        after = reference + float(actual.stats.sac[name])
        assert abs(before - after) < 2e-6
    assert path.read_bytes() == original_bytes


def test_segments_share_event_reference_and_report_union_coverage(tmp_path):
    root = tmp_path / "source"
    source_trace(root, "first", offset=0, npts=2, rate=1)
    source_trace(root, "second", offset=4, npts=2, rate=1)
    target = event(0)
    result = cut_event_station(
        target,
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
    )
    traces = [read(path)[0] for path in sorted((tmp_path / "out").rglob("*.sac"))]
    assert [float(tr.stats.sac.b) for tr in traces] == [0, 4]
    assert all(get_sac_reftime(tr.stats.sac) == BASE for tr in traces)
    assert result.partial_channels == 1
    assert result.gapped_channels == 1
    assert result.coverage_samples[0].coverage_fraction == pytest.approx(4 / 6)


def test_cached_window_copies_only_window_and_never_shares_samples(tmp_path):
    path = source_trace(tmp_path, offset=0, npts=100_001)
    record = WaveformArchiveIndex.build(tmp_path).records_for_station("XX", "TEST")[0]
    reader = WaveformReader(max_bytes=500_000)
    with patch("seispy.event.archive_index.obspy.read", wraps=read) as load:
        first = reader.read(record, starttime=BASE + 10, endtime=BASE + 11)
        first[0].data[:] = -1
        second = reader.read(record, starttime=BASE + 10, endtime=BASE + 11)
    assert load.call_count == 1
    assert second[0].data.nbytes == 101 * 4
    np.testing.assert_array_equal(
        second[0].data, np.arange(1000, 1101, dtype=np.float32)
    )
    assert reader.cached_bytes <= 500_000


def test_oversized_record_is_not_retained(tmp_path):
    source_trace(tmp_path, npts=1001)
    record = WaveformArchiveIndex.build(tmp_path).records_for_station("XX", "TEST")[0]
    reader = WaveformReader(max_bytes=1000)
    reader.read(record, starttime=BASE, endtime=BASE + 1)
    assert reader.cached_bytes == 0


def test_catalog_export_preserves_microseconds(tmp_path):
    frame = pd.DataFrame([{"time": "2024-01-02T00:00:00.123456Z"}])
    output = tmp_path / "events.cat"
    write_event_catalog(frame, output)
    assert output.read_text().strip() == "2024/01/02,00:00:00.123456"


def test_public_summary_distinguishes_existing_output(tmp_path):
    source = tmp_path / "source"
    source_trace(source)
    csv = tmp_path / "events.csv"
    pd.DataFrame(
        [{"time": str(BASE), "latitude": 35, "longitude": 105, "depth": 12, "mag": 6}]
    ).to_csv(csv, index=False)
    kwargs = dict(
        output_dir=tmp_path / "out",
        time_window=5,
        max_workers=1,
        save_report=False,
        save_log=False,
    )
    first = cut_event_waveforms(source, csv, **kwargs)
    second = cut_event_waveforms(source, csv, **kwargs)
    assert first.outputs_written == 1
    assert second.existing_outputs == 1
    assert second.issue_samples[0].status == "output_exists"


@pytest.mark.parametrize("rate", [1, 40, 62.5, 100])
@pytest.mark.parametrize("origin", [0.125, 0.15, 0.123456])
def test_window_copy_matches_full_read_merge_and_trim(tmp_path, rate, origin):
    from obspy import Stream
    from seispy.waveform.integrity import merge_contiguous_segments

    root = tmp_path / "source"
    paths = [
        source_trace(root, "a", offset=-2, npts=int(3 * rate), rate=rate),
        source_trace(
            root,
            "b",
            offset=-2 + int(3 * rate) / rate,
            npts=int(3 * rate),
            rate=rate,
            bias=500,
        ),
    ]
    target = event(origin, duration=2)
    expected = merge_contiguous_segments(Stream([read(p)[0] for p in paths]))
    for trace in expected:
        start = UTCDateTime(ns=target["start"].ns // 1_000_000_000 * 1_000_000_000)
        trace.trim(start, start + 2)
    result = cut_event_station(
        target,
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
        waveform_reader=WaveformReader(),
    )
    assert result.succeeded == 1
    assert result.partial_channels == 0
    actual = sorted(
        (read(p)[0] for p in (tmp_path / "out").rglob("*.sac")),
        key=lambda t: t.stats.starttime,
    )
    assert len(actual) == len(expected)
    for a, b in zip(actual, expected, strict=True):
        assert a.stats.starttime == b.stats.starttime
        np.testing.assert_array_equal(a.data, b.data)


def test_conflict_outside_window_still_preserves_overlapping_segments(tmp_path):
    root = tmp_path / "source"
    first = source_trace(root, "a", offset=0, npts=1001, rate=100)
    second = source_trace(root, "b", offset=1, npts=1001, rate=100, bias=100)
    trace = read(second)[0]
    trace.data[0] = -999  # Conflict before the requested [3, 5] second window.
    trace.write(str(second), format="SAC")
    target = event(3, 2)
    result = cut_event_station(
        target,
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
        waveform_reader=WaveformReader(),
    )
    assert result.outputs == 2
    assert result.overlapping_channels == 1
    assert result.partial_channels == 0
    assert result.coverage_samples[0].coverage_fraction == 1
    assert result.coverage_samples[0].covered_seconds == pytest.approx(2.01)


def test_lru_byte_budget_evicts_old_records(tmp_path):
    from seispy.event import archive_index as module

    source_trace(tmp_path, "a", npts=1001)
    source_trace(tmp_path, "b", offset=30, npts=1001)
    a, b = WaveformArchiveIndex.build(tmp_path).records_for_station("XX", "TEST")
    reader = WaveformReader(max_bytes=5000)
    with patch.object(module.obspy, "read", wraps=read) as load:
        reader.read(a)
        reader.read(b)
        assert reader.cached_bytes == 4004
        reader.read(a)
    assert load.call_count == 3


def test_station_yields_incremental_counts_and_bounds_samples(tmp_path):
    from seispy.event.cutting import _station_results

    # No data is deliberate: the actual event loop must emit progress before
    # exhausting a station and retain only the configured issue sample count.
    events = [event(i * 10) for i in range(40)]
    results = _station_results([(STATION, ())], events, tmp_path, 3, 1)
    first = next(results)
    assert 0 < first.tasks < len(events)
    rest = list(results)
    assert sum(x.tasks for x in [first, *rest]) == 40
    assert sum(len(x.samples) for x in [first, *rest]) == 3


@pytest.mark.parametrize("workers", [1, 2])
def test_progress_counts_do_not_duplicate_or_drop_tasks(tmp_path, workers):
    from seispy.event.cutting import _station_results

    events = [event(i * 10) for i in range(40)]
    tasks = [({"network": "XX", "station": f"S{i}"}, ()) for i in range(3)]
    results = list(_station_results(tasks, events, tmp_path, 2, workers))
    assert sum(x.tasks for x in results) == 120
    assert sum(x.no_data for x in results) == 120
    assert sum(len(x.samples) for x in results) == 6


def test_distinct_microsecond_events_do_not_collide(tmp_path):
    root = tmp_path / "source"
    source_trace(root)
    index = WaveformArchiveIndex.build(root)
    for offset in (0.123001, 0.123999):
        result = cut_event_station(
            event(offset), STATION, tmp_path / "out", archive_index=index
        )
        assert result.succeeded == 1
    assert len(list((tmp_path / "out").rglob("*.sac"))) == 2


def test_parallel_progress_can_be_closed_early(tmp_path):
    from seispy.event.cutting import _station_results

    events = [event(i * 10) for i in range(200)]
    tasks = [({"network": "XX", "station": f"S{i}"}, ()) for i in range(5)]
    iterator = _station_results(tasks, events, tmp_path, 2, 2)
    assert next(iterator).tasks > 0
    iterator.close()


def test_parallel_worker_failure_is_propagated(tmp_path):
    from seispy.event.cutting import _station_results

    events = [event(i * 10) for i in range(20)] + [{}]
    tasks = [({"network": "XX", "station": f"S{i}"}, ()) for i in range(3)]
    with pytest.raises(RuntimeError, match="KeyError"):
        list(_station_results(tasks, events, tmp_path, 2, 2))


def test_event_distance_headers_are_recalculated(tmp_path):
    from obspy.geodetics import gps2dist_azimuth

    root = tmp_path / "source"
    path = source_trace(root)
    original = read(path)[0]
    original.stats.sac.update(dict(dist=1.0, az=2.0, baz=3.0, gcarc=4.0))
    original.write(str(path), format="SAC")
    target = event(0)
    result = cut_event_station(
        target,
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
    )
    assert result.succeeded == 1
    output = next((tmp_path / "out").rglob("*.sac"))
    raw = read(output)[0].stats.sac
    expected = gps2dist_azimuth(35.0, 105.0, 30.0, 110.0)
    assert raw.dist == pytest.approx(expected[0] / 1000, rel=1e-6)
    assert raw.az == pytest.approx(expected[1], rel=1e-6)
    assert raw.baz == pytest.approx(expected[2], rel=1e-6)


def test_empty_half_sample_edge_does_not_discard_other_segments(tmp_path):
    root = tmp_path / "source"
    source_trace(root, "edge", offset=-0.5, npts=2, rate=1)
    source_trace(root, "valid", offset=3, npts=4, rate=1, bias=10)
    result = cut_event_station(
        event(1.5, 4),
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
    )
    assert result.succeeded == 1
    assert result.outputs == 1
    actual = read(next((tmp_path / "out").rglob("*.sac")))[0]
    assert actual.stats.starttime == BASE + 3
    assert actual.data[0] == 10


@pytest.mark.parametrize("fragmented", [False, True])
def test_output_names_use_event_second_and_short_segment_suffix(tmp_path, fragmented):
    root = tmp_path / "source"
    if fragmented:
        source_trace(root, "first", offset=0, npts=2, rate=1)
        source_trace(root, "second", offset=4, npts=2, rate=1)
    else:
        source_trace(root, offset=0, npts=6, rate=1)
    result = cut_event_station(
        event(0.123456),
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
    )
    assert result.succeeded == 1
    names = sorted(p.name for p in (tmp_path / "out").rglob("*.sac"))
    prefix = "XX.TEST.--.BHZ.20240102000000"
    expected = (
        [f"{prefix}.s01.sac", f"{prefix}.s02.sac"] if fragmented else [f"{prefix}.sac"]
    )
    assert names == expected
