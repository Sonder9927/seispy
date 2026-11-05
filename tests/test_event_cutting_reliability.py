"""Written SAC correctness and bounded-read contracts for event cutting."""

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from obspy import Trace, UTCDateTime, read
from obspy.io.sac.util import get_sac_reftime

from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.cutting import cut_event_station, cut_event_waveforms

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


@pytest.mark.parametrize("offset", [0.0, 0.123456])
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
    assert abs(reference + float(actual.stats.sac.o) - target["start"]) < 1e-06
    start = UTCDateTime(ns=target["start"].ns // 1000000000 * 1000000000)
    expected = original.copy().trim(start, start + 5)
    assert actual.stats.starttime == expected.stats.starttime
    assert actual.stats.endtime == expected.stats.endtime
    np.testing.assert_array_equal(actual.data, expected.data)
    for name in ("a", "t0", "f"):
        before = get_sac_reftime(original.stats.sac) + float(original.stats.sac[name])
        after = reference + float(actual.stats.sac[name])
        assert abs(before - after) < 2e-06
    assert path.read_bytes() == original_bytes


@pytest.mark.parametrize(
    "scenario",
    [
        "segments_share_event_reference_and_report_union_coverage",
        "conflict_outside_window_still_preserves_overlapping_segments",
        "empty_half_sample_edge_does_not_discard_other_segments",
    ],
)
def test_event_segments(tmp_path, scenario):
    root = tmp_path / "source"
    if scenario == "segments_share_event_reference_and_report_union_coverage":
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
    elif scenario == "conflict_outside_window_still_preserves_overlapping_segments":
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
    elif scenario == "empty_half_sample_edge_does_not_discard_other_segments":
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


@pytest.mark.parametrize(
    "scenario",
    [
        "cached_window_copies_only_window_and_never_shares_samples",
        "oversized_record_is_not_retained",
        "lru_byte_budget_evicts_old_records",
    ],
)
def test_bounded_waveform_cache(tmp_path, scenario):
    if scenario == "cached_window_copies_only_window_and_never_shares_samples":
        path = source_trace(tmp_path, offset=0, npts=100_001)
        record = WaveformArchiveIndex.build(tmp_path).records_for_station("XX", "TEST")[
            0
        ]
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
    elif scenario == "oversized_record_is_not_retained":
        source_trace(tmp_path, npts=1001)
        record = WaveformArchiveIndex.build(tmp_path).records_for_station("XX", "TEST")[
            0
        ]
        reader = WaveformReader(max_bytes=1000)
        reader.read(record, starttime=BASE, endtime=BASE + 1)
        assert reader.cached_bytes == 0
    elif scenario == "lru_byte_budget_evicts_old_records":
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


@pytest.mark.parametrize(
    "scenario",
    [
        "public_summary_distinguishes_existing_output",
        "public_cutter_rejects_same_second_events_before_writing",
    ],
)
def test_event_output_collisions(tmp_path, scenario):
    if scenario == "public_summary_distinguishes_existing_output":
        source = tmp_path / "source"
        source_trace(source)
        csv = tmp_path / "events.csv"
        pd.DataFrame(
            [
                {
                    "time": str(BASE),
                    "latitude": 35,
                    "longitude": 105,
                    "depth": 12,
                    "mag": 6,
                }
            ]
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
        assert second.skipped == second.existing_skipped == 1
        assert second.failed == 0
        assert not second.issue_samples
    elif scenario == "public_cutter_rejects_same_second_events_before_writing":
        root = tmp_path / "source"
        source_trace(root)
        catalog = tmp_path / "events.csv"
        catalog.write_text(
            "time,latitude,longitude,depth,mag\n2024-01-02T00:00:00.123001Z,1,2,3,4\n2024-01-02T00:00:00.123999Z,1,2,3,4\n"
        )
        output = tmp_path / "out"
        with pytest.raises(ValueError, match="same UTC second"):
            cut_event_waveforms(root, catalog, output_dir=output, max_workers=1)
        assert not output.exists()


@pytest.mark.parametrize(
    "rate,origin", [(1, 0.125), (40, 0.15), (62.5, 0.125), (100, 0.123456)]
)
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
        start = UTCDateTime(ns=target["start"].ns // 1000000000 * 1000000000)
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


@pytest.mark.parametrize(
    "scenario",
    [
        "station_yields_incremental_counts_and_bounds_samples",
        "parallel_progress_can_be_closed_early",
        "parallel_worker_failure_is_propagated",
    ],
)
def test_incremental_progress(tmp_path, scenario):
    from seispy.event.cutting import _station_results

    if scenario == "station_yields_incremental_counts_and_bounds_samples":
        events = [event(i * 10) for i in range(40)]
        results = _station_results([(STATION, ())], events, tmp_path, 3, 1)
        first = next(results)
        assert 0 < first.tasks < len(events)
        rest = list(results)
        assert sum(x.tasks for x in [first, *rest]) == 40
        assert sum(len(x.samples) for x in [first, *rest]) == 0
    elif scenario == "parallel_progress_can_be_closed_early":
        events = [event(i * 10) for i in range(200)]
        tasks = [({"network": "XX", "station": f"S{i}"}, ()) for i in range(5)]
        iterator = _station_results(tasks, events, tmp_path, 2, 2)
        assert next(iterator).tasks > 0
        iterator.close()
    elif scenario == "parallel_worker_failure_is_propagated":
        events = [event(i * 10) for i in range(20)] + [{}]
        tasks = [({"network": "XX", "station": f"S{i}"}, ()) for i in range(3)]
        with pytest.raises(RuntimeError, match="KeyError"):
            list(_station_results(tasks, events, tmp_path, 2, 2))


@pytest.mark.parametrize("workers", [1, 2])
def test_progress_counts_do_not_duplicate_or_drop_tasks(tmp_path, workers):
    from seispy.event.cutting import _station_results

    events = [event(i * 10) for i in range(40)]
    tasks = [({"network": "XX", "station": f"S{i}"}, ()) for i in range(3)]
    results = list(_station_results(tasks, events, tmp_path, 2, workers))
    assert sum(x.tasks for x in results) == 120
    assert sum(x.no_data for x in results) == sum(x.skipped for x in results) == 120
    assert sum(x.failed for x in results) == 0
    assert sum(len(x.samples) for x in results) == 0


def test_event_distance_headers_are_recalculated(tmp_path):
    from seispy._sac_geometry import sac_geometry

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
    expected = sac_geometry(35.0, 105.0, 30.0, 110.0)
    for key in ("dist", "az", "baz", "gcarc"):
        assert raw[key] == pytest.approx(expected[key], rel=1e-6)


def test_cut_filter_does_not_add_durations_across_gaps(tmp_path):
    from seispy.waveform import TraceFilter

    root = tmp_path / "source"
    source_trace(root, name="first", offset=0, npts=3000, rate=1)
    source_trace(root, name="second", offset=4000, npts=3000, rate=1)
    result = cut_event_station(
        event(0, 10800),
        STATION,
        tmp_path / "out",
        archive_index=WaveformArchiveIndex.build(root),
        trace_filter=TraceFilter(min_duration_seconds=5400),
    )
    assert result.traces_filtered == 2 and result.skipped == 1
    assert result.failed == 0 and result.outputs == 0


@pytest.mark.parametrize(
    "minimum,policy_minimum,kept,workers",
    [
        (None, None, True, 1),
        (2, None, True, 2),
        (3, None, True, 1),
        (4, None, False, 2),
        (2, 4, False, 1),
        (4, 2, False, 2),
    ],
)
def test_public_minimum_cut_window(tmp_path, minimum, policy_minimum, kept, workers):
    from seispy.waveform.integrity import TraceFilter

    policy = (
        TraceFilter(min_duration_seconds=policy_minimum, min_samples=0)
        if policy_minimum is not None
        else None
    )
    source = tmp_path / "source"
    source_trace(source, offset=0, npts=3, rate=1)
    catalog = tmp_path / "events.csv"
    pd.DataFrame(
        [dict(time=str(BASE), latitude=35, longitude=105, depth=12, mag=6)]
    ).to_csv(catalog, index=False)
    result = cut_event_waveforms(
        source,
        catalog,
        output_dir=tmp_path / "out",
        time_window=5,
        min_time_window=minimum,
        trace_filter=policy,
        max_workers=workers,
        save_report=False,
        save_log=False,
    )
    assert result.failed == 0
    assert result.succeeded == result.outputs_written == int(kept)
    assert result.skipped == result.traces_filtered == int(not kept)


@pytest.mark.parametrize("minimum", [0, 6, float("nan")])
def test_invalid_minimum_cut_window(tmp_path, minimum):
    with pytest.raises(ValueError, match="min_time_window"):
        cut_event_waveforms(
            tmp_path, tmp_path / "events.csv", time_window=5, min_time_window=minimum
        )


def test_rerun_compares_samples_after_first_megabyte(tmp_path):
    source_trace(tmp_path / "source", offset=0, rate=100, npts=300_001)
    index = WaveformArchiveIndex.build(tmp_path / "source")
    args = (event(0, 3000), STATION, tmp_path / "out")
    first = cut_event_station(*args, archive_index=index)
    assert first.outputs == 1
    repeated = cut_event_station(*args, archive_index=index)
    assert repeated.existing_skipped == 1
    output = next((tmp_path / "out").rglob("*.sac"))
    with output.open("r+b") as handle:
        handle.seek(1024 * 1024 + 100)
        original = handle.read(1)
        handle.seek(-1, 1)
        handle.write(bytes([original[0] ^ 1]))
    result = cut_event_station(*args, archive_index=index)
    assert result.failed == 1
    assert result.outputs == result.skipped == 0
    assert result.samples[0].status == "output_conflict"


@pytest.mark.parametrize("sample_limit", [0, 1])
def test_channel_write_failure_preserves_outputs_and_continues_channels(
    tmp_path, sample_limit
):
    from seispy.event import cutting as module

    root = tmp_path / "source"
    for name, offset in (("a", 0), ("b", 5), ("c", 10)):
        source_trace(root, name=name, offset=offset, rate=1, npts=3)
    other = source_trace(root, name="d", offset=0, rate=1, npts=4)
    trace = read(other)[0]
    trace.stats.channel = "BHN"
    trace.write(str(other), format="SAC")
    original_writer = module._write_event_sac

    def failing_writer(trace, destination):
        if trace.stats.channel == "BHZ" and trace.stats.starttime == BASE + 5:
            raise OSError("simulated write failure")
        original_writer(trace, destination)

    with patch.object(module, "_write_event_sac", side_effect=failing_writer):
        result = cut_event_station(
            event(0, 20),
            STATION,
            tmp_path / "out",
            archive_index=WaveformArchiveIndex.build(root),
            max_error_samples=sample_limit,
        )
    assert result.failed == 1
    assert result.succeeded == result.skipped == 0
    assert result.outputs == result.channels == 2
    assert len(result.samples) == min(1, sample_limit)
    if result.samples:
        assert result.samples[0].status == "output_failed"
    paths = list((tmp_path / "out").rglob("*.sac"))
    assert len(paths) == 2
    assert {read(path)[0].stats.channel for path in paths} == {"BHZ", "BHN"}
    assert not any((path.name.startswith(".") for path in paths))
