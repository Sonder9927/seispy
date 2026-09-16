from pathlib import Path

import numpy as np
from obspy import Stream, Trace, UTCDateTime, read

from seispy import waveform
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


def test_worker_rejects_a_path_that_disagrees_with_headers(tmp_path):
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

    assert result.failed == 1
    assert "do not match intended archive path" in result.issue.error
