from pathlib import Path
import struct

import numpy as np
from obspy import Stream, Trace, UTCDateTime, read

from seispy import waveform
from seispy.archive import WaveformIdentity
from seispy.waveform import archiving


def _raw_mseed(root, *, channel="HHZ"):
    trace = Trace(data=np.arange(100, dtype=np.int32))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = channel
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = 10
    path = root / "NZ" / "AAA" / "2026" / f"NZ.AAA.10.{channel}.2026.001.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([trace]).write(path, format="MSEED")
    return path


def _constant_raw_mseed(root, *, value=1):
    trace = Trace(data=np.full(100, value, dtype=np.int32))
    trace.stats.network = "YH"
    trace.stats.station = "LOBS1"
    trace.stats.location = ""
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2015-01-01")
    trace.stats.sampling_rate = 100
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
    first = Trace(data=np.arange(100, dtype=np.int32))
    first.stats.network = "NZ"
    first.stats.station = "AAA"
    first.stats.location = "10"
    first.stats.channel = "HHZ"
    first.stats.starttime = UTCDateTime("2026-01-01T01:00:00")
    first.stats.sampling_rate = 10
    second = first.copy()
    second.stats.starttime = UTCDateTime("2026-01-02T02:00:00")
    path = root / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([first, second]).write(path, format="MSEED")
    return path


def _unsorted_sac(root):
    trace = Trace(data=np.arange(100, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = 10
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
    assert "contains no samples" in result.issue.error


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
    assert "simulated destination failure" in result.issue.error


def test_archive_requires_separate_source_and_output_trees(tmp_path):
    source = tmp_path / "raw"
    _raw_mseed(source)

    with np.testing.assert_raises_regex(ValueError, "separate directory trees"):
        waveform.archive_waveforms(source, source / "archive", max_workers=1)


def test_sac_archive_writes_one_valid_file_per_trace(tmp_path):
    source = tmp_path / "raw"
    raw = _raw_mseed(source)
    output = tmp_path / "sac"

    summary = waveform.archive_waveforms(
        source, output, output_format="sac", max_workers=1
    )

    files = list(output.rglob("*.sac"))
    assert len(files) == 1
    assert read(files[0], format="SAC")[0].id == "NZ.AAA.10.HHZ"
    assert raw.exists()
    assert summary.files_written == 1


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


def test_existing_valid_archive_is_skipped(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _raw_mseed(source)

    first = waveform.archive_waveforms(source, output, max_workers=1)
    second = waveform.archive_waveforms(source, output, max_workers=1)

    assert first.succeeded == 1
    assert second.skipped == 1
    assert second.succeeded == 0


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

    assert summary.failed == 1
    assert summary.succeeded == summary.files_written == 0
    assert not list(output.rglob("*.mseed"))
    assert "trace is constant" in summary.issue_samples[0].error
    assert raw.is_file()


def test_worker_keeps_usable_traces_when_a_sibling_is_constant(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    first = Trace(data=np.full(100, 7, dtype=np.int32))
    first.stats.network = "NZ"
    first.stats.station = "AAA"
    first.stats.location = "10"
    first.stats.channel = "HHZ"
    first.stats.starttime = UTCDateTime("2026-01-01T01:00:00")
    first.stats.sampling_rate = 10
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
    assert result.traces_total == 2
    assert result.traces_written == 1
    assert result.traces_failed == 1
    assert "trace is constant" in result.issue.error


def test_classify_traces_rejects_non_finite_samples():
    trace = Trace(data=np.array([1.0, np.nan, 2.0], dtype=np.float64))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = 10

    valid, empty, errors = archiving._classify_traces(Stream([trace]))

    assert valid == []
    assert empty == []
    assert "NaN or infinite" in errors[0]


def _midnight_straddling_raw(root):
    pre_roll = Trace(data=np.arange(1000, dtype=np.int32))
    pre_roll.stats.network = "NZ"
    pre_roll.stats.station = "AAA"
    pre_roll.stats.location = "10"
    pre_roll.stats.channel = "HHZ"
    pre_roll.stats.starttime = UTCDateTime("2026-01-01T23:59:57")
    pre_roll.stats.sampling_rate = 100
    same_day = pre_roll.copy()
    same_day.stats.starttime = UTCDateTime("2026-01-02T12:00:00")
    path = root / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([pre_roll, same_day]).write(path, format="MSEED")
    return path


def _contiguous_raw(root):
    first = Trace(data=np.arange(1000, dtype=np.int32))
    first.stats.network = "NZ"
    first.stats.station = "AAA"
    first.stats.location = "10"
    first.stats.channel = "HHZ"
    first.stats.starttime = UTCDateTime("2026-01-02T00:00:00")
    first.stats.sampling_rate = 100
    second = first.copy()
    second.stats.starttime = first.stats.endtime + first.stats.delta
    path = root / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([first, second]).write(path, format="MSEED")
    return path


def test_mseed_archive_splits_unmergeable_traces_into_one_file_each(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    _midnight_straddling_raw(source)

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    outputs = sorted(path.name for path in output.rglob("*.mseed"))
    assert outputs == [
        "NZ.AAA.10.HHZ.2026.002.001T235957.mseed",
        "NZ.AAA.10.HHZ.2026.002.120000.mseed",
    ]
    assert summary.succeeded == 1
    assert summary.files_written == 2
    assert summary.traces_failed == 0


def test_mseed_archive_merges_contiguous_traces_into_one_file(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    _contiguous_raw(source)

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    outputs = sorted(output.rglob("*.mseed"))
    assert [path.name for path in outputs] == ["NZ.AAA.10.HHZ.2026.002.mseed"]
    merged = read(outputs[0])
    assert len(merged) == 1
    assert merged[0].stats.npts == 2000
    assert summary.files_written == 1
    assert summary.traces_failed == 0


def test_sac_archive_merges_contiguous_traces_into_one_file(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "sac"
    _contiguous_raw(source)

    summary = waveform.archive_waveforms(
        source, output, output_format="sac", max_workers=1
    )

    outputs = sorted(path.name for path in output.rglob("*.sac"))
    assert outputs == ["NZ.AAA.10.HHZ.2026.002.000000.sac"]
    assert summary.files_written == 1
    assert summary.traces_failed == 0


def test_sac_archive_splits_unmergeable_traces_into_one_file_each(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "sac"
    _midnight_straddling_raw(source)

    summary = waveform.archive_waveforms(
        source, output, output_format="sac", max_workers=1
    )

    outputs = sorted(path.name for path in output.rglob("*.sac"))
    assert outputs == [
        "NZ.AAA.10.HHZ.2026.002.001T235957.sac",
        "NZ.AAA.10.HHZ.2026.002.120000.sac",
    ]
    assert summary.files_written == 2
    assert summary.traces_failed == 0


def _trace_at(starttime, *, npts=1000, seed=0):
    trace = Trace(data=(np.arange(npts, dtype=np.int32) + seed))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime(starttime)
    trace.stats.sampling_rate = 100
    return trace


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
    assert summary.errored == 0
    assert source.joinpath("NZ/AAA/2026/NZ.AAA.10.HHZ.2026.001.mseed").is_file()


def test_existing_shorter_mseed_archive_is_not_silently_kept(tmp_path):
    source = tmp_path / "old"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed"
    path.parent.mkdir(parents=True)
    Stream([_trace_at("2026-01-01T23:59:57.1")]).write(path, format="MSEED")
    output = tmp_path / "archive"
    waveform.archive_waveforms(source, output, pattern="*.mseed", max_workers=1)

    longer = tmp_path / "new" / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.mseed"
    longer.parent.mkdir(parents=True)
    Stream([_trace_at("2026-01-01T23:59:57.1", npts=5000)]).write(
        longer, format="MSEED"
    )
    summary = waveform.archive_waveforms(
        tmp_path / "new", output, pattern="*.mseed", max_workers=1
    )

    archived = (
        output / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.001T235957.mseed"
    )
    assert read(archived)[0].stats.npts == 1000
    assert summary.failed == 1
    assert summary.has_issues
    assert "overwrite=True" in summary.issue_samples[0].error


def test_existing_shorter_sac_archive_is_not_silently_kept(tmp_path):
    source = tmp_path / "old"
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.235957.sac"
    path.parent.mkdir(parents=True)
    _trace_at("2026-01-01T23:59:57.1").write(str(path), format="SAC")
    output = tmp_path / "archive"
    waveform.archive_waveforms(
        source, output, output_format="sac", pattern="*.sac", max_workers=1
    )

    longer = (
        tmp_path / "new" / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.001.235957.sac"
    )
    longer.parent.mkdir(parents=True)
    _trace_at("2026-01-01T23:59:57.1", npts=5000).write(str(longer), format="SAC")
    summary = waveform.archive_waveforms(
        tmp_path / "new", output, output_format="sac", pattern="*.sac", max_workers=1
    )

    archived = output / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.001T235957.sac"
    assert read(archived, format="SAC")[0].stats.npts == 1000
    assert summary.failed == 1
    assert summary.has_issues


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
    assert summary.errored == 1
    assert summary.issue_samples


def test_mseed_archive_keeps_post_roll_trace_in_its_day(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    trace = Trace(data=np.arange(10000, dtype=np.int32))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2026-01-02T23:59:00")
    trace.stats.sampling_rate = 100
    path = source / "NZ" / "AAA" / "2026" / "NZ.AAA.10.HHZ.2026.002.mseed.raw"
    path.parent.mkdir(parents=True, exist_ok=True)
    Stream([trace]).write(path, format="MSEED")

    summary = waveform.archive_waveforms(source, output, max_workers=1)

    outputs = sorted(output.rglob("*.mseed"))
    assert [path.name for path in outputs] == ["NZ.AAA.10.HHZ.2026.002.mseed"]
    assert summary.succeeded == 1
    assert summary.files_written == 1


def _sac_chunk(starttime, npts):
    trace = Trace(data=np.arange(npts, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = "HLRZ"
    trace.stats.location = "10"
    trace.stats.channel = "EHE"
    trace.stats.starttime = UTCDateTime(starttime)
    trace.stats.sampling_rate = 100
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
