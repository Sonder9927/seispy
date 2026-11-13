"""Existing event windows are sliced on-grid with a half-open end."""

from pathlib import Path
from unittest.mock import patch
import csv

import numpy as np
import obspy
import pytest
from obspy.io.sac import SACTrace

from seispy.event import trim_event_waveforms


def put(root, *, rate=1, npts=10801, begin=0, origin=0, channel="HHZ"):
    path = root / "20240101000000" / f"NZ.STA.10.{channel}.20240101000000.sac"
    path.parent.mkdir(parents=True, exist_ok=True)
    sac = SACTrace(
        data=np.arange(npts, dtype=np.float32),
        delta=1 / rate,
        b=begin,
        o=origin,
        a=20.5,
        t0=40.25,
        nzyear=2024,
        nzjday=1,
        nzhour=0,
        nzmin=0,
        nzsec=0,
        nzmsec=0,
        knetwk="NZ",
        kstnm="STA",
        khole="10",
        kcmpnm=channel,
        evdp=35,
        gcarc=45,
        cmpaz=123,
        cmpinc=90,
    )
    sac.write(str(path))
    return path


def output_for(result, path):
    return result.output_dir / path.parent.name / path.name


@pytest.mark.parametrize("rate", [1, 25, 100])
def test_90min_half_open_counts_and_original_unchanged(tmp_path, rate):
    source = tmp_path / "input"
    path = put(source, rate=rate, npts=10800 * rate + 1)
    before = path.read_bytes()
    result = trim_event_waveforms(source, tmp_path / "out")
    assert result.succeeded == 1 and not result.has_issues
    assert path.read_bytes() == before
    sac = SACTrace.read(str(output_for(result, path)))
    assert sac.npts == 5400 * rate
    np.testing.assert_array_equal(sac.data, np.arange(5400 * rate, dtype=np.float32))
    assert sac.b == 0
    assert sac.depmin == 0 and sac.depmax == 5400 * rate - 1


@pytest.mark.parametrize("rate", [1, 25])
def test_precise_origin_picks_and_non_aligned_window(tmp_path, rate):
    source = tmp_path / "input"
    path = put(source, rate=rate, npts=10800 * rate + 1, origin=0.7)
    original = SACTrace.read(str(path))
    result = trim_event_waveforms(source, tmp_path / "out")
    trimmed = SACTrace.read(str(output_for(result, path)))
    first = 1 if rate == 1 else 18
    assert trimmed.npts == 5400 * rate
    np.testing.assert_array_equal(
        trimmed.data, original.data[first : first + 5400 * rate]
    )
    assert trimmed.reftime == original.reftime
    assert trimmed.o == original.o
    assert trimmed.a == original.a and trimmed.t0 == original.t0
    assert trimmed.evdp == original.evdp and trimmed.gcarc == original.gcarc
    assert trimmed.cmpaz == original.cmpaz and trimmed.cmpinc == original.cmpinc
    assert trimmed.b == np.float32(first / rate)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"begin": 30, "npts": 10801},
        {"npts": 5399},
    ],
)
def test_insufficient_coverage_skips_without_padding(tmp_path, kwargs):
    source = tmp_path / "input"
    put(source, **kwargs)
    result = trim_event_waveforms(source, tmp_path / "out")
    assert result.skipped_by_reason["incomplete_coverage"] == 1
    assert not result.has_issues
    assert not list(result.output_dir.rglob("*.sac"))


def test_exact_90min_input_last_point_is_not_required(tmp_path):
    source = tmp_path / "input"
    path = put(source, npts=5400)
    result = trim_event_waveforms(source, tmp_path / "out")
    assert result.succeeded == 1
    assert SACTrace.read(str(output_for(result, path))).npts == 5400


def test_negative_offsets_and_empty_grid_window(tmp_path):
    source = tmp_path / "input"
    path = put(source, origin=10)
    result = trim_event_waveforms(source, tmp_path / "out", time_window=(-2, 3))
    trimmed = SACTrace.read(str(output_for(result, path)))
    np.testing.assert_array_equal(trimmed.data, np.arange(8, 13, dtype=np.float32))
    assert trimmed.b == 8 and trimmed.o == 10
    result = trim_event_waveforms(source, tmp_path / "empty", time_window=(0.2, 0.3))
    assert result.skipped_by_reason["empty_window"] == 1


def test_existing_output_and_overwrite(tmp_path):
    source = tmp_path / "input"
    path = put(source)
    first = trim_event_waveforms(source, tmp_path / "out")
    before = output_for(first, path).read_bytes()
    result = trim_event_waveforms(source, tmp_path / "out", time_window=(0, 60))
    assert result.failed == 1
    assert output_for(first, path).read_bytes() == before
    result = trim_event_waveforms(
        source, tmp_path / "out", time_window=(0, 60), overwrite=True
    )
    assert result.succeeded == 1
    assert SACTrace.read(str(output_for(result, path))).npts == 60


def test_missing_origin_and_csv(tmp_path):
    source = tmp_path / "input"
    path = put(source)
    sac = SACTrace.read(str(path))
    sac.o = None
    sac.write(str(path))
    report = tmp_path / "errors.csv"
    result = trim_event_waveforms(source, tmp_path / "out", errors_csv=report)
    assert result.failed == 1
    assert "header: o" in result.issue_samples[0].error
    with report.open() as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == ["source", "error"] and len(rows) == 2


def test_pattern_and_atomic_failure(tmp_path):
    source = tmp_path / "input"
    path = put(source)
    put(source, channel="HHN")
    before = path.read_bytes()
    with patch(
        "seispy.event.trimming.commit_output", side_effect=OSError("disk error")
    ):
        result = trim_event_waveforms(source, tmp_path / "out", pattern="*.HHZ.*.sac")
    assert result.total == result.failed == 1
    assert path.read_bytes() == before
    assert not list(result.output_dir.rglob("*.sac"))


@pytest.mark.parametrize(
    "window", [(0, 0), (1, 0), (0, float("nan")), (0, float("inf")), (1,), None, "90"]
)
def test_invalid_window(tmp_path, window):
    with pytest.raises(ValueError, match="time_window"):
        trim_event_waveforms(tmp_path, tmp_path.parent / "out", time_window=window)


def test_source_output_and_report_separation(tmp_path):
    source = tmp_path / "input"
    source.mkdir()
    with pytest.raises(ValueError, match="separate"):
        trim_event_waveforms(source, source / "out")
    with pytest.raises(ValueError, match="outside src"):
        trim_event_waveforms(source, tmp_path / "out", errors_csv=source / "errors.csv")


def test_unrelated_reference_rejected(tmp_path):
    with pytest.raises(ValueError, match="reference"):
        trim_event_waveforms(tmp_path, tmp_path.parent / "out", reference="starttime")


def test_coverage_skip_reads_headers_only(tmp_path):
    source = tmp_path / "input"
    put(source, begin=60)
    with patch("seispy.event.trimming.obspy.read", wraps=obspy.read) as read:
        result = trim_event_waveforms(source, tmp_path / "out")
    assert result.skipped == 1
    assert read.call_count == 1
    assert read.call_args.kwargs["headonly"] is True


def test_symlink_output_cannot_replace_input(tmp_path):
    source = tmp_path / "input"
    path = put(source)
    original = path.read_bytes()
    output = tmp_path / "out"
    output.mkdir()
    (output / path.parent.name).symlink_to(path.parent, target_is_directory=True)
    result = trim_event_waveforms(source, output, overwrite=True)
    assert result.failed == 1
    assert path.read_bytes() == original


def test_non_aligned_edge_keeps_sample_time(tmp_path):
    source = tmp_path / "input"
    path = put(source, begin=-0.003126, origin=0.06)
    result = trim_event_waveforms(source, tmp_path / "out")
    trimmed = SACTrace.read(str(output_for(result, path)))
    original = SACTrace.read(str(path))
    assert trimmed.npts == 5400
    assert trimmed.b == np.float32(original.b + 1)
    assert trimmed.o == original.o
    np.testing.assert_array_equal(trimmed.data, original.data[1:5401])
