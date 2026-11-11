"""Selection preserves files and separates rejection from metadata failures."""

import csv

import numpy as np
import obspy
import pytest

from seispy.event import select_event_waveforms
from seispy.waveform import TraceFilter


def sac(root, name="XX.STA..BHZ.event.sac", **headers):
    path = root / "20240101000000" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    trace = obspy.Trace(np.arange(100, dtype=np.float32))
    trace.stats.starttime = obspy.UTCDateTime("2024-01-01") + 10
    trace.stats.sampling_rate = 1
    trace.stats.sac = {"b": 10, "o": 0, **headers}
    trace.write(str(path), format="SAC")
    return path


def test_copy_dedup_pattern_and_origin(tmp_path):
    source = tmp_path / "input"
    path = sac(source, gcarc=40)
    sac(source, "XX.STA..BHN.event.sac", gcarc=40)
    result = select_event_waveforms(
        [source, path],
        tmp_path / "out",
        pattern="*.BHZ.*.sac",
        starttime="2024-01-01",
        endtime="2024-01-01T00:00:01",
        distance_deg=(40, 40),
    )
    assert (result.total, result.succeeded, result.failed) == (1, 1, 0)
    assert (
        result.output_dir / path.parent.name / path.name
    ).read_bytes() == path.read_bytes()


def test_distance_missing_and_csv(tmp_path):
    source = tmp_path / "input"
    sac(source, dist=4000)
    report = tmp_path / "errors.csv"
    result = select_event_waveforms(
        source, tmp_path / "out", distance_deg=(30, 90), errors_csv=report
    )
    assert result.failed == 1
    assert "gcarc" in result.issue_samples[0].error
    with report.open() as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == ["source", "error"]
    assert len(rows) == 2
    result = select_event_waveforms(source, tmp_path / "km", distance_km=(4000, 4000))
    assert result.succeeded == 1


def test_quality_and_distance_skips(tmp_path):
    source = tmp_path / "input"
    sac(source, gcarc=40)
    result = select_event_waveforms(
        source, tmp_path / "out", trace_filter=TraceFilter(min_duration_seconds=101)
    )
    assert result.skipped_by_reason["quality"] == 1
    assert not result.has_issues
    assert not list(result.output_dir.iterdir())
    result = select_event_waveforms(source, tmp_path / "other", distance_deg=(50, None))
    assert result.skipped_by_reason["distance"] == 1


def test_collisions_and_existing_output(tmp_path):
    first = sac(tmp_path / "first")
    second = sac(tmp_path / "second")
    result = select_event_waveforms([first, second], tmp_path / "out", overwrite=True)
    assert result.failed == 2
    assert not list(result.output_dir.iterdir())
    assert select_event_waveforms([first], tmp_path / "out").succeeded == 1
    assert select_event_waveforms([first], tmp_path / "out").failed == 1
    assert (
        select_event_waveforms([first], tmp_path / "out", overwrite=True).succeeded == 1
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"distance_km": (1, 2), "distance_deg": (1, 2)},
        {"distance_deg": (90, 30)},
        {"distance_deg": (-1, 90)},
        {"distance_deg": (0, 181)},
        {"distance_km": (0, float("nan"))},
        {"starttime": "2025-01-01", "endtime": "2024-01-01"},
    ],
)
def test_invalid_parameters(tmp_path, kwargs):
    with pytest.raises(ValueError):
        select_event_waveforms(tmp_path / "input", tmp_path / "out", **kwargs)


def test_output_must_be_separate(tmp_path):
    source = tmp_path / "input"
    sac(source)
    with pytest.raises(ValueError, match="separate"):
        select_event_waveforms(source, source / "out")


def test_time_end_exclusive_and_missing_origin(tmp_path):
    source = tmp_path / "input"
    path = sac(source)
    result = select_event_waveforms(source, tmp_path / "out", endtime="2024-01-01")
    assert result.skipped_by_reason["time"] == 1
    trace = obspy.read(str(path))[0]
    del trace.stats.sac.o
    trace.write(str(path), format="SAC")
    result = select_event_waveforms(
        source, tmp_path / "missing", starttime="2023-01-01"
    )
    assert result.failed == 1
    assert "header: o" in result.issue_samples[0].error
    assert select_event_waveforms(source, tmp_path / "unrestricted").succeeded == 1


def test_invalid_distance_and_unreadable_file(tmp_path):
    source = tmp_path / "input"
    sac(source, gcarc=-12345)
    bad = source / "20240101000000" / "broken.sac"
    bad.write_text("not a SAC file")
    result = select_event_waveforms(source, tmp_path / "out", distance_deg=(0, 180))
    assert result.failed == 2
