from pathlib import Path
import json
import struct

import numpy as np
import pytest
from hypothesis import given, strategies as st
from obspy import Inventory, Stream, Trace, UTCDateTime, read

from seispy import waveform
from seispy.archive import WaveformIdentity
from seispy.waveform import archiving
from seispy.waveform.integrity import TraceFilter


def _raw_mseed(root, *, channel="HHZ"):
    trace = _trace_at(
        "2026-01-01",
        data=np.arange(100, dtype=np.int32),
        channel=channel,
        sampling_rate=10,
    )
    path = root / "NZ" / "AAA" / "2026" / f"NZ.AAA.10.{channel}.2026.001.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([trace]).write(path, format="MSEED")
    return path


def _constant_raw_mseed(root, *, value=1):
    trace = _trace_at(
        "2015-01-01",
        data=np.full(100, value, dtype=np.int32),
        network="YH",
        station="LOBS1",
        location="",
    )
    path = root / "YH" / "LOBS1" / "2015" / "YH.LOBS1.--.HHZ.2015.001.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([trace]).write(path, format="MSEED")
    return path


def _append_empty_next_day_record(path):
    contents = path.read_bytes()
    record_length = 4096
    record = bytearray(contents[-record_length:])
    struct.pack_into(">HHBBBBH", record, 20, 2026, 2, 0, 0, 0, 0, 0)
    struct.pack_into(">H", record, 30, 0)
    first_blockette = struct.unpack_from(">H", record, 46)[0]
    record[first_blockette + 4] = 0
    path.write_bytes(contents + record)
    return bytes(record)


def _multiday_raw_mseed(root):
    first = _trace_at(
        "2026-01-01T01:00:00", data=np.arange(100, dtype=np.int32), sampling_rate=10
    )
    second = first.copy()
    second.stats.starttime = UTCDateTime("2026-01-02T02:00:00")
    path = root / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([first, second]).write(path, format="MSEED")
    return path


def _unsorted_sac(root):
    trace = _trace_at(
        "2026-01-01", data=np.arange(100, dtype=np.float32), sampling_rate=10
    )
    path = root / "incoming" / "arbitrary-name.sac"
    path.parent.mkdir(parents=True, exist_ok=True)
    trace.write(str(path), format="SAC")
    return path


def test_mseed_archive_preserves_valid_source_bytes(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _raw_mseed(source)
    original = raw.read_bytes()

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    archived = output / "NZ" / "AAA" / "2026" / raw.name.removesuffix(".raw")
    assert archived.read_bytes() == original
    assert raw.is_file()
    assert summary.succeeded == summary.files_written == 1
    assert summary.failed == 0

    repeated = waveform.archive_waveforms(source, output, max_workers=1)
    assert repeated.skipped == 1
    assert repeated.succeeded == repeated.failed == repeated.files_written == 0
    assert archived.read_bytes() == original


def test_mseed_archive_ignores_empty_next_day_trace_and_preserves_bytes(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _raw_mseed(source)
    _append_empty_next_day_record(raw)
    original = raw.read_bytes()

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    archived = output / "NZ" / "AAA" / "2026" / raw.name.removesuffix(".raw")
    assert archived.read_bytes() == original
    assert summary.succeeded == summary.files_written == 1
    assert summary.failed == 0
    assert summary.traces_total == 2
    assert summary.traces_written == 1
    assert summary.traces_ignored_empty == 1


def test_mseed_archive_splits_valid_days_and_succeeds(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    _multiday_raw_mseed(source)

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    outputs = sorted(output.rglob("*.mseed"))
    assert len(outputs) == 2
    assert {read(path)[0].stats.starttime.julday for path in outputs} == {1, 2}
    assert summary.succeeded == summary.reshaped == 1
    assert summary.recovered == 0
    assert summary.failed == 0
    assert summary.files_written == summary.traces_written == 2


def test_worker_fails_when_mseed_contains_only_an_empty_trace(tmp_path):
    source = tmp_path / "raw"
    raw = _raw_mseed(source)
    empty_record = _append_empty_next_day_record(raw)
    raw.write_bytes(empty_record)

    result = archiving._archive_one(
        str(raw), str(source), str(tmp_path / "archive"), "mseed", False, False
    )

    assert result.failed == 1
    assert result.succeeded == result.files_written == 0
    assert result.traces_total == result.traces_ignored_empty == 1
    assert "contains no samples" in result.issues[0].error


def test_worker_succeeds_when_only_one_trace_group_can_be_written(
    tmp_path, monkeypatch
):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _multiday_raw_mseed(source)
    original_write = archiving._write_mseed_group
    calls = 0

    def fail_second_group(traces, destination, overwrite):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("simulated destination failure")
        return original_write(traces, destination, overwrite)

    monkeypatch.setattr(archiving, "_write_mseed_group", fail_second_group)

    result = archiving._archive_one(
        str(raw), str(source), str(output), "mseed", False, False
    )

    assert result.succeeded == result.reshaped == 1
    assert result.recovered == 0
    assert result.failed == 0
    assert result.files_written == result.traces_written == 1
    assert result.traces_failed == 1
    assert "simulated destination failure" in result.issues[0].error


@pytest.mark.parametrize("output_format", ["mseed", "sac"])
@pytest.mark.parametrize("failed_day", [1, 2])
@pytest.mark.parametrize("stage", ["merge", "filter", "path"])
def test_archive_isolates_group_errors(
    tmp_path, monkeypatch, output_format, failed_day, stage
):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _multiday_raw_mseed(source)
    if stage == "merge":
        original = archiving._merge_group

        def fail_selected(group):
            if group[0].stats.starttime.julday == failed_day:
                raise ValueError("simulated group failure")
            return original(group)

        monkeypatch.setattr(archiving, "_merge_group", fail_selected)
    elif stage == "filter":
        original = archiving.split_traces_by_filter

        def fail_selected(traces, policy):
            if traces[0].stats.starttime.julday == failed_day:
                raise ValueError("simulated group failure")
            return original(traces, policy)

        monkeypatch.setattr(archiving, "split_traces_by_filter", fail_selected)
    else:
        original = (
            WaveformIdentity.sac_path
            if output_format == "sac"
            else archiving._recovered_mseed_path
        )

        def fail_sac(identity, root):
            if identity.julday == failed_day:
                raise ValueError("simulated group failure")
            return original(identity, root)

        def fail_mseed(root, traces, **kwargs):
            if traces[0].stats.starttime.julday == failed_day:
                raise ValueError("simulated group failure")
            return original(root, traces, **kwargs)

        if output_format == "sac":
            monkeypatch.setattr(WaveformIdentity, "sac_path", fail_sac)
        else:
            # Ensure both days use generated paths rather than the raw filename.
            renamed = raw.with_name("arbitrary.mseed.raw")
            raw.rename(renamed)
            raw = renamed
            monkeypatch.setattr(archiving, "_recovered_mseed_path", fail_mseed)

    result = archiving._archive_one(
        str(raw),
        str(source),
        str(output),
        output_format,
        False,
        False,
        trace_filter=TraceFilter(min_duration_seconds=0, min_samples=2),
    )

    assert result.succeeded == 1
    assert result.failed == result.skipped == 0
    assert result.files_written == result.traces_written == 1
    assert result.traces_total == 2
    assert result.traces_failed == result.files_with_errors == 1
    assert "simulated group failure" in result.issues[0].error
    outputs = list(output.rglob(f"*.{output_format}"))
    assert len(outputs) == 1
    assert read(outputs[0])[0].stats.starttime.julday == 3 - failed_day


@pytest.mark.parametrize("output_format", ["mseed", "sac"])
def test_archive_existing_samples_must_match(tmp_path, output_format):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _raw_mseed(source)

    def archive(overwrite=False):
        return archiving._archive_one(
            str(raw), str(source), str(output), output_format, overwrite, False
        )

    assert archive().files_written == 1
    assert archive().traces_existing == 1
    destination = next(output.rglob(f"*.{output_format}"))
    previous = destination.read_bytes()
    changed = read(raw)
    changed[0].data += 5000
    changed.write(raw, format="MSEED")

    conflict = archive()
    assert conflict.failed == conflict.traces_failed == 1
    assert conflict.files_with_errors == 1
    assert conflict.issues[0].reason_code == "existing_content_conflict"
    assert conflict.traces_existing == conflict.files_written == 0
    assert "source" in conflict.issues[0].error
    assert destination.read_bytes() == previous
    assert archive(overwrite=True).files_written == 1
    np.testing.assert_array_equal(read(destination)[0].data, changed[0].data)
    assert archive().traces_existing == 1


def test_sac_validation_compares_samples_at_storage_precision(tmp_path):
    trace = _trace_at("2026-01-01", npts=100)
    trace.data = np.linspace(0.1, 1.1, 100, dtype=np.float64)
    path = tmp_path / "rounded.sac"
    trace.write(str(path), format="SAC")

    archiving._validate_sac_output(path, trace)
    trace.data[50] += 0.01
    with pytest.raises(ValueError, match="samples differ"):
        archiving._validate_sac_output(path, trace)


@pytest.mark.parametrize("sample_limit", [0, 1])
def test_archive_report_counts_all_events_when_samples_are_capped(
    tmp_path, sample_limit
):
    source = tmp_path / "raw"
    _raw_mseed(source, channel="HHZ")
    _raw_mseed(source, channel="HHN")
    (source / "broken.mseed.raw").write_bytes(b"not a waveform")

    summary = waveform.archive_waveforms(
        source,
        tmp_path / "archive",
        max_workers=1,
        inventory=Inventory([], source="test"),
        max_error_samples=sample_limit,
    )

    assert summary.succeeded == 2
    assert summary.failed == summary.files_with_errors == 1
    assert summary.files_with_warnings == 2
    assert summary.error_counts == {"source_read_failed": 1}
    assert summary.warning_counts == {"inventory_mismatch": 2}
    assert len(summary.issue_samples) == sample_limit
    report = json.loads(summary.report_path.read_text())
    assert report["error_counts"] == summary.error_counts
    assert report["warning_counts"] == summary.warning_counts
    assert "errored" not in report
    assert len(report["issue_samples"]) == sample_limit
    log = summary.log_path.read_text()
    assert "reason=source_read_failed" in log
    assert log.count("reason=inventory_mismatch") == 2


def test_partial_archive_logs_errors_and_warnings_separately(tmp_path, monkeypatch):
    from unittest.mock import Mock

    source = tmp_path / "raw"
    raw = _multiday_raw_mseed(source)
    original = archiving._merge_group

    def fail_second(group):
        if group[0].stats.starttime.julday == 2:
            raise ValueError("bad group")
        return original(group)

    monkeypatch.setattr(archiving, "_merge_group", fail_second)
    monkeypatch.setattr(archiving, "_WORKER_INVENTORY", Inventory([], source="test"))
    result = archiving._archive_one(
        str(raw), str(source), str(tmp_path / "archive"), "sac", False, False
    )

    assert result.succeeded == 1
    assert result.failed == 0
    assert result.files_with_errors == result.files_with_warnings == 1
    assert [issue.reason_code for issue in result.issues] == [
        "group_processing_failed",
        "inventory_mismatch",
        "inventory_mismatch",
    ]
    run = Mock()
    archiving._log_issue(run, result)
    assert run.error.call_count == 1
    assert run.warning.call_count == 2


def test_worker_failure_is_an_error_event():
    result = archiving._failed_result(Path("source"), RuntimeError("worker stopped"))
    assert result.failed == result.files_with_errors == 1
    assert result.files_with_warnings == 0
    assert result.issues[0].reason_code == "worker_failed"


def test_archive_requires_separate_source_and_output_trees(tmp_path):
    source = tmp_path / "raw"
    _raw_mseed(source)

    with np.testing.assert_raises_regex(ValueError, "separate directory trees"):
        waveform.archive_waveforms(source, source / "archive", max_workers=1)


def test_archive_organizes_existing_sac_from_headers(tmp_path):
    source = tmp_path / "unsorted"
    original = _unsorted_sac(source)
    output = tmp_path / "archive"

    summary = waveform.archive_waveforms(
        source,
        output,
        output_format="sac",
        pattern="*.sac",
        max_workers=1,
    )

    archived = output / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.000000.sac"
    assert archived.is_file()
    assert read(archived, format="SAC")[0].id == "NZ.AAA.10.HHZ"
    assert original.is_file()
    assert summary.succeeded == summary.files_written == 1
    assert summary.failed == 0


def test_sac_source_cannot_be_archived_as_mseed(tmp_path):
    source = tmp_path / "unsorted"
    original = _unsorted_sac(source)

    summary = waveform.archive_waveforms(
        source,
        tmp_path / "archive",
        pattern="*.sac",
        max_workers=1,
    )

    assert summary.failed == 1
    assert "output_format='sac'" in summary.issue_samples[0].error
    assert original.is_file()


def test_invalid_source_is_reported(tmp_path):
    source = tmp_path / "raw"
    raw = source / "NZ" / "AAA" / "2026" / "NZ.AAA.2026.001.mseed.raw"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"not miniSEED")

    summary = waveform.archive_waveforms(
        source,
        tmp_path / "archive",
        max_workers=1,
        discard_corrupt_records=False,
    )

    assert summary.failed == 1


def test_worker_salvages_valid_trace_when_source_path_disagrees(tmp_path):
    raw = _raw_mseed(tmp_path / "raw")
    mismatched = raw.with_name("NZ.WRONG.10.HHZ.2026.001.mseed.raw")
    raw.rename(mismatched)
    result = archiving._archive_one(
        str(mismatched),
        str(tmp_path / "raw"),
        str(tmp_path / "archive"),
        "mseed",
        False,
        False,
    )

    corrected = (
        tmp_path
        / "archive"
        / "NZ"
        / "AAA"
        / "2026"
        / "NZ.AAA.10.HHZ.2026.001.000000.mseed"
    )
    assert corrected.is_file()
    assert result.succeeded == 1
    assert result.reshaped == 0
    assert result.recovered == 0
    assert result.failed == 0


def test_constant_raw_response_is_not_archived(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _constant_raw_mseed(source)

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    assert summary.failed == 0
    assert summary.skipped == 1
    assert summary.succeeded == summary.files_written == 0
    assert summary.traces_filtered == 1
    assert not list(output.rglob("*.mseed"))
    assert raw.is_file()


def test_worker_keeps_usable_traces_when_a_sibling_is_constant(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    first = _trace_at(
        "2026-01-01T01:00:00", data=np.full(100, 7, dtype=np.int32), sampling_rate=10
    )
    second = first.copy()
    second.data = np.arange(100, dtype=np.int32)
    second.stats.starttime = UTCDateTime("2026-01-02T02:00:00")
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([first, second]).write(path, format="MSEED")

    result = archiving._archive_one(
        str(path), str(source), str(output), "mseed", False, False
    )

    assert result.succeeded == result.reshaped == 1
    assert result.recovered == 0
    assert result.failed == 0
    assert result.files_with_errors == 0
    assert result.traces_total == 2
    assert result.traces_written == 1
    assert result.traces_filtered == 1
    assert result.traces_failed == 0


def test_classify_traces_rejects_non_finite_samples():
    trace = _trace_at(
        "2026-01-01",
        data=np.array([1.0, np.nan, 2.0], dtype=np.float64),
        sampling_rate=10,
    )

    valid, empty, unusable = archiving._classify_traces(Stream([trace]))

    assert valid == []
    assert empty == []
    assert len(unusable) == 1
    assert "NaN or infinite" in unusable[0][1]


def test_inventory_mismatch_archives_with_a_warning(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    _raw_mseed(source)

    summary = waveform.archive_waveforms(
        source, output, inventory=Inventory(), max_workers=1
    )

    assert summary.succeeded == 1
    assert summary.files_with_errors == 0
    assert summary.files_with_warnings == 1
    assert summary.has_issues
    assert "no inventory epoch matches" in summary.issue_samples[0].error
    assert list(output.rglob("*.mseed"))


@pytest.mark.parametrize("output_format", ["mseed", "sac"])
def test_archive_trace_filter_runs_after_lossless_merge(
    tmp_path, monkeypatch, output_format
):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed.raw"
    path.parent.mkdir(parents=True)

    first = _trace_at("2026-01-01T00:00:00", npts=600)
    second = _trace_at("2026-01-01T00:00:06", npts=800, seed=600)
    # ObsPy can merge contiguous records while reading MiniSEED. Keep the
    # decoded traces separate so this exercises the archive's merge/filter order.
    read_full_mseed = archiving._read_full_mseed

    def read_source_unmerged(candidate):
        if Path(candidate) == path:
            return Stream([first.copy(), second.copy()])
        return read_full_mseed(candidate)

    monkeypatch.setattr(archiving, "_read_full_mseed", read_source_unmerged)

    summary = archiving._archive_one(
        str(path),
        str(source),
        str(output),
        output_format,
        False,
        True,
        trace_filter=TraceFilter(min_duration_seconds=10, min_samples=1000),
    )

    assert summary.succeeded == 1
    assert summary.failed == 0
    assert summary.traces_total == 2
    assert summary.traces_filtered == 0
    assert summary.traces_written == 2

    archived = list(output.rglob(f"*.{output_format}"))
    assert len(archived) == 1
    merged = read(archived[0], format=output_format.upper())
    assert len(merged) == 1
    assert merged[0].stats.npts == 1400
    np.testing.assert_array_equal(merged[0].data, np.arange(1400))


@pytest.mark.parametrize("output_format", ["mseed", "sac"])
@pytest.mark.parametrize("filter_outer", [True, False])
def test_archive_conflicting_overlap_counts_only_preserved_trace(
    tmp_path, output_format, filter_outer
):
    source = tmp_path / "raw"
    source.mkdir()
    output = tmp_path / "archive"
    path = source / "overlap.mseed.raw"
    outer = _trace_at("2026-01-01", npts=2000)
    inner = _trace_at("2026-01-01T00:00:02", npts=1200, seed=5000)
    rejected, expected = (outer, inner) if filter_outer else (inner, outer)
    rejected.data = np.tile(np.array([0, 1], dtype=np.int32), rejected.stats.npts // 2)
    Stream([outer, inner]).write(path, format="MSEED")

    result = archiving._archive_one(
        str(path),
        str(source),
        str(output),
        output_format,
        False,
        True,
        trace_filter=TraceFilter(max_flatline_amplitude=2),
    )

    assert result.succeeded == 1
    assert result.skipped == result.failed == result.files_with_errors == 0
    assert result.traces_total == 2
    assert result.traces_written == result.traces_filtered == 1
    assert result.traces_failed == 0
    assert result.files_written == 1
    archived = list(output.rglob(f"*.{output_format}"))
    assert len(archived) == 1
    actual = read(archived[0])[0]
    assert actual.stats.starttime == expected.stats.starttime
    np.testing.assert_array_equal(actual.data, expected.data)


@st.composite
def _contained_trace_pairs(draw):
    rate = draw(st.sampled_from([10, 20, 50, 100, 200]))
    npts = draw(st.integers(min_value=4, max_value=2000))
    offset = draw(st.integers(min_value=0, max_value=npts - 2))
    length = draw(st.integers(min_value=2, max_value=npts - offset))
    seed = draw(st.integers(min_value=-10000, max_value=10000))
    start = UTCDateTime("2026-01-01") + draw(st.integers(0, 86399))
    outer = _trace_at(start, npts=npts, seed=seed, sampling_rate=rate)
    inner = _trace_at(
        start + offset / rate, npts=length, seed=seed + offset, sampling_rate=rate
    )
    return outer, inner


@given(pair=_contained_trace_pairs())
def test_merge_group_tracks_identical_overlap(pair):
    outer, inner = pair
    expected = outer.data.copy()

    segments, membership = archiving._merge_group([outer, inner])

    assert len(segments) == 1
    assert membership == [0, 0]
    np.testing.assert_array_equal(segments[0].data, expected)


@pytest.mark.parametrize("mismatch", ["samples", "rate", "grid", "coverage", "channel"])
@given(pair=_contained_trace_pairs())
def test_covering_segment_rejects_incompatible_trace(mismatch, pair):
    segment, trace = pair
    if mismatch == "samples":
        trace.data[0] += 1
    elif mismatch == "rate":
        trace.stats.sampling_rate *= 2
    elif mismatch == "grid":
        trace.stats.starttime += trace.stats.delta / 2
    elif mismatch == "coverage":
        trace.stats.starttime = segment.stats.endtime + segment.stats.delta
    else:
        trace.stats.channel = "HHN"

    with pytest.raises(ValueError, match="no merged segment preserves trace"):
        archiving._covering_segment([segment], trace)


def test_archive_trace_filter_skips_filtered_traces(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed.raw"
    path.parent.mkdir(parents=True)
    Stream(
        [
            _trace_at("2026-01-01T00:00:00", npts=100),
            _trace_at("2026-01-01T01:00:00", npts=100_000),
        ]
    ).write(path, format="MSEED")

    summary = waveform.archive_waveforms(
        source,
        output,
        max_workers=1,
        trace_filter=TraceFilter(min_duration_seconds=60),
    )

    assert summary.traces_total == 2
    assert summary.traces_filtered == 1
    assert summary.traces_failed == 0
    assert summary.succeeded == 1


def test_classify_traces_treats_single_sample_trace_as_empty():
    trace = _trace_at("2026-01-01", data=np.array([5], dtype=np.int32))

    valid, empty, unusable = archiving._classify_traces(Stream([trace]))

    assert valid == []
    assert len(empty) == 1
    assert unusable == []


def _midnight_straddling_raw(root):
    pre_roll = _trace_at("2026-01-01T23:59:57", data=np.arange(1000, dtype=np.int32))
    same_day = pre_roll.copy()
    same_day.stats.starttime = UTCDateTime("2026-01-02T12:00:00")
    path = root / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([pre_roll, same_day]).write(path, format="MSEED")
    return path


def _contiguous_raw(root):
    first = _trace_at("2026-01-02T00:00:00", data=np.arange(1000, dtype=np.int32))
    second = first.copy()
    second.stats.starttime = first.stats.endtime + first.stats.delta
    path = root / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([first, second]).write(path, format="MSEED")
    return path


@pytest.mark.parametrize("output_format", ["mseed", "sac"])
def test_archive_splits_unmergeable_traces_into_one_file_each(tmp_path, output_format):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    _midnight_straddling_raw(source)

    summary = waveform.archive_waveforms(
        source, output, output_format=output_format, max_workers=1
    )

    outputs = sorted(path.name for path in output.rglob(f"*.{output_format}"))
    assert outputs == [
        f"NZ.AAA.10.HHZ.2026.002.001T235957.{output_format}",
        f"NZ.AAA.10.HHZ.2026.002.120000.{output_format}",
    ]
    assert summary.succeeded == 1
    assert summary.files_written == 2
    assert summary.traces_failed == 0


@pytest.mark.parametrize(
    "output_format, filename",
    [
        ("mseed", "NZ.AAA.10.HHZ.2026.002.mseed"),
        ("sac", "NZ.AAA.10.HHZ.2026.002.000000.sac"),
    ],
)
def test_archive_merges_contiguous_traces_into_one_file(
    tmp_path, output_format, filename
):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _contiguous_raw(source)

    summary = waveform.archive_waveforms(
        source, output, output_format=output_format, max_workers=1
    )

    outputs = sorted(output.rglob(f"*.{output_format}"))
    assert [path.name for path in outputs] == [filename]
    merged = read(outputs[0])
    assert len(merged) == 1
    assert merged[0].stats.npts == 2000
    assert merged[0].id == "NZ.AAA.10.HHZ"
    assert raw.is_file()
    assert summary.files_written == 1
    assert summary.traces_failed == 0


def _trace_at(
    starttime,
    *,
    npts=1000,
    seed=0,
    sampling_rate=100,
    network="NZ",
    station="AAA",
    location="10",
    channel="HHZ",
    data=None,
):
    return Trace(
        data=np.arange(npts, dtype=np.int32) + seed if data is None else data,
        header=dict(
            network=network,
            station=station,
            location=location,
            channel=channel,
            starttime=UTCDateTime(starttime),
            sampling_rate=sampling_rate,
        ),
    )


def test_mseed_archive_uses_short_segment_tokens(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream(
        [
            _trace_at("2026-01-02T01:00:00.100000"),
            _trace_at("2026-01-02T02:00:00.200000"),
        ]
    ).write(path, format="MSEED")

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    outputs = sorted(path.name for path in output.rglob("*.mseed"))
    assert outputs == [
        "NZ.AAA.10.HHZ.2026.002.010000.mseed",
        "NZ.AAA.10.HHZ.2026.002.020000.mseed",
    ]
    assert summary.files_written == 2
    assert summary.reshaped == 1


def test_mseed_archive_disambiguates_same_second_segments(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream(
        [
            _trace_at("2026-01-02T01:00:00.100000"),
            _trace_at("2026-01-02T01:00:00.900000"),
        ]
    ).write(path, format="MSEED")

    waveform.archive_waveforms(source, output, max_workers=1)

    assert sorted(path.name for path in output.rglob("*.mseed")) == [
        "NZ.AAA.10.HHZ.2026.002.010000.9.mseed",
        "NZ.AAA.10.HHZ.2026.002.010000.mseed",
    ]


def test_mseed_archive_accepts_an_already_archived_source(tmp_path):
    source = tmp_path / "old"
    raw = _raw_mseed(source)
    archived = raw.with_name(raw.name.removesuffix(".raw"))
    raw.rename(archived)
    output = tmp_path / "new"

    summary = waveform.archive_waveforms(
        source, output, pattern="*.mseed", max_workers=1
    )

    migrated = output / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed"
    assert migrated.read_bytes() == archived.read_bytes()
    assert summary.succeeded == 1
    assert summary.failed == 0


def test_mseed_archive_migrates_a_misnamed_archived_file(tmp_path):
    source = tmp_path / "old"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed"
    path.parent.mkdir(parents=True)
    Stream([_trace_at("2026-01-01T23:59:57.100000")]).write(path, format="MSEED")
    output = tmp_path / "new"

    summary = waveform.archive_waveforms(
        source, output, pattern="*.mseed", max_workers=1
    )

    outputs = sorted(p.name for p in output.rglob("*.mseed"))
    assert outputs == ["NZ.AAA.10.HHZ.2026.002.001T235957.mseed"]
    assert summary.succeeded == 1
    assert summary.reshaped == 0
    assert summary.files_with_errors == 0
    assert source.joinpath("NZ/AAA/2026/NZ.AAA.10.HHZ.2026.001.mseed").is_file()


@pytest.mark.parametrize(
    "output_format, source_name",
    [
        ("mseed", "NZ.AAA.10.HHZ.2026.001.mseed"),
        ("sac", "NZ.AAA.10.HHZ.2026.001.235957.sac"),
    ],
)
def test_existing_shorter_archive_is_not_silently_kept(
    tmp_path, output_format, source_name
):
    output = tmp_path / "archive"
    for directory, npts in [("old", 1000), ("new", 5000)]:
        source = tmp_path / directory
        path = source / "NZ" / "AAA" / "2026" / source_name
        path.parent.mkdir(parents=True)
        _trace_at("2026-01-01T23:59:57.1", npts=npts).write(
            str(path), format=output_format.upper()
        )
        summary = waveform.archive_waveforms(
            source,
            output,
            output_format=output_format,
            pattern=f"*.{output_format}",
            max_workers=1,
        )
        if directory == "old":
            assert summary.files_written == 1

    archived = (
        output
        / "NZ"
        / "AAA"
        / "2026"
        / f"NZ.AAA.10.HHZ.2026.002.001T235957.{output_format}"
    )
    assert read(archived)[0].stats.npts == 1000
    assert summary.failed == 1
    assert summary.has_issues
    if output_format == "mseed":
        assert "overwrite=True" in summary.issue_samples[0].error


def test_conflicting_same_start_traces_are_reported(tmp_path):
    source = tmp_path / "raw"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed.raw"
    path.parent.mkdir(parents=True)
    Stream(
        [
            _trace_at("2026-01-01T00:00:00", seed=0),
            _trace_at("2026-01-01T00:00:00", seed=5),
        ]
    ).write(path, format="MSEED")

    summary = waveform.archive_waveforms(source, tmp_path / "archive", max_workers=1)

    assert summary.has_issues
    assert summary.files_with_errors == 1
    assert summary.issue_samples


def test_mseed_archive_keeps_post_roll_trace_in_its_day(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    trace = _trace_at("2026-01-02T23:59:00", data=np.arange(10000, dtype=np.int32))
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([trace]).write(path, format="MSEED")

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    outputs = sorted(output.rglob("*.mseed"))
    assert [path.name for path in outputs] == ["NZ.AAA.10.HHZ.2026.002.mseed"]
    assert summary.succeeded == 1
    assert summary.files_written == 1


def _sac_chunk(starttime, npts):
    trace = _trace_at(
        starttime, data=np.arange(npts, dtype=np.float32), station="HLRZ", channel="EHE"
    )
    return trace


def test_merge_waveforms_by_day_separates_midnight_chunks(tmp_path):
    source = tmp_path / "sac"
    source.mkdir()
    aligned = _sac_chunk("2014-05-07T00:30:00", 1000)
    straddling = _sac_chunk("2014-05-07T23:59:59.7", 100_000)
    for trace in (aligned, straddling):
        destination = WaveformIdentity.from_trace(trace).sac_path(source)
        destination.parent.mkdir(parents=True, exist_ok=True)
        trace.write(str(destination), format="SAC")

    output = tmp_path / "merged"
    waveform.merge_waveforms_by_day(source, output, pattern="*.sac")

    assert sorted(path.name for path in output.rglob("*.merged.sac")) == [
        "NZ.HLRZ.10.EHE.2014.127.merged.sac",
        "NZ.HLRZ.10.EHE.2014.128.merged.sac",
    ]
    assert not (output / "merge-errors.txt").exists()
