import inspect
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


def test_archive_defaults_are_safe():
    parameters = inspect.signature(waveform.archive_waveforms).parameters

    assert parameters["max_workers"].default == 5
    assert parameters["remove_original"].default is False


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


def test_remove_original_happens_after_successful_archive(tmp_path):
    source = tmp_path / "raw"
    raw = _raw_mseed(source)

    summary = waveform.archive_waveforms(
        source, tmp_path / "archive", max_workers=1, remove_original=True
    )

    assert not raw.exists()
    assert summary.originals_removed == 1


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


def test_invalid_source_is_reported_and_retained(tmp_path):
    source = tmp_path / "raw"
    raw = source / "NZ" / "AAA" / "2026" / "NZ.AAA.2026.001.mseed.raw"
    raw.parent.mkdir(parents=True)
    raw.write_bytes(b"not miniSEED")

    summary = waveform.archive_waveforms(
        source,
        tmp_path / "archive",
        max_workers=1,
        remove_original=True,
        discard_corrupt_records=False,
    )

    assert summary.failed == 1
    assert summary.originals_retained == 1
    assert raw.exists()


def test_existing_valid_archive_is_skipped(tmp_path):
    source = tmp_path / "raw"
    output = tmp_path / "archive"
    raw = _raw_mseed(source)

    first = waveform.archive_waveforms(source, output, max_workers=1)
    second = waveform.archive_waveforms(source, output, max_workers=1)

    assert first.succeeded == 1
    assert second.skipped == 1
    assert second.succeeded == 0


def test_worker_result_does_not_delete_failed_source(tmp_path):
    raw = _raw_mseed(tmp_path / "raw")
    mismatched = raw.with_name("NZ.WRONG.10.HHZ.2026.001.mseed.raw")
    raw.rename(mismatched)
    result = archiving._archive_one(
        str(mismatched),
        str(tmp_path / "raw"),
        str(tmp_path / "archive"),
        "mseed",
        True,
        False,
        False,
    )

    assert result.failed == 1
    assert mismatched.exists()
