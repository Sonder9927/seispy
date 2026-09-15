"""Waveform archive coverage contracts."""

from datetime import date
from pathlib import Path
from unittest.mock import patch

import matplotlib.pyplot as plt
import numpy as np
import pytest
from obspy import Trace, UTCDateTime

from seispy.waveform import (
    WaveformCoverageReport,
    plot_waveform_coverage,
    scan_waveform_coverage,
    summarize_waveform_coverage,
    waveform_coverage,
)


def _waveform(
    root,
    station,
    year,
    julday,
    *,
    start_seconds=0,
    duration_seconds=10,
    channel="BHZ",
    name="trace.sac",
    content=b"data",
):
    starttime = UTCDateTime(year=year, julday=julday) + start_seconds
    trace = Trace(data=np.arange(duration_seconds, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = station
    trace.stats.location = ""
    trace.stats.channel = channel
    trace.stats.starttime = starttime
    suffix = Path(name).suffix.lower()
    if suffix == ".mseed":
        filename = f"NZ.{station}.{year}.{julday:03d}.mseed"
        output_format = "MSEED"
    elif suffix == ".sac":
        filename = (
            f"NZ.{station}..{channel}.{year}.{julday:03d}."
            f"{starttime.strftime('%H%M%S')}.sac"
        )
        output_format = "SAC"
    else:
        filename = name
        output_format = None
    path = root / "NZ" / station / str(year) / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    if not content:
        path.write_bytes(content)
    elif output_format:
        trace.write(str(path), format=output_format)
    else:
        path.write_bytes(content)
    return path


def test_scan_measures_sample_intervals_instead_of_file_presence(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=43_200)

    result = scan_waveform_coverage(tmp_path / "NZ")

    assert result.loc[0, "coverage_seconds"] == pytest.approx(43_200)
    assert result.loc[0, "coverage_percent"] == pytest.approx(50)
    assert result.loc[0, "trace_count"] == 1


def test_filename_mode_estimates_canonical_station_day_without_reading(tmp_path):
    path = tmp_path / "NZ" / "AAA" / "2024" / "NZ.AAA.2024.001.mseed"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not waveform data")

    with patch("seispy.waveform.coverage.read") as read_waveform:
        result = scan_waveform_coverage(tmp_path / "NZ", read_mode="filename")

    read_waveform.assert_not_called()
    assert result.loc[0, "read_mode"] == "filename"
    assert result.loc[0, "coverage_seconds"] == pytest.approx(86_400)
    assert result.loc[0, "coverage_percent"] == pytest.approx(100)


def test_header_mode_rejects_a_damaged_canonical_file(tmp_path, caplog):
    path = tmp_path / "NZ" / "AAA" / "2024" / "NZ.AAA.2024.001.mseed"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"not waveform data")

    result = scan_waveform_coverage(tmp_path / "NZ", read_mode="header")

    assert result.empty
    assert "ignoring invalid waveform file" in caplog.text


def test_scan_rejects_unknown_read_mode(tmp_path):
    (tmp_path / "NZ").mkdir()

    with pytest.raises(ValueError, match="read_mode"):
        scan_waveform_coverage(tmp_path / "NZ", read_mode="full")


def test_scan_splits_a_cross_midnight_trace_between_utc_days(tmp_path):
    _waveform(
        tmp_path,
        "AAA",
        2024,
        1,
        start_seconds=86_390,
        duration_seconds=20,
    )

    result = scan_waveform_coverage(tmp_path / "NZ")

    assert list(result["date"].dt.date) == [date(2024, 1, 1), date(2024, 1, 2)]
    assert list(result["coverage_seconds"]) == pytest.approx([10, 10])


def test_scan_unions_overlapping_channels_without_double_counting(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=100, channel="BHZ")
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=100, channel="BHN")

    result = scan_waveform_coverage(tmp_path / "NZ")

    assert result.loc[0, "coverage_seconds"] == pytest.approx(100)
    assert result.loc[0, "file_count"] == 2
    assert result.loc[0, "trace_count"] == 2


def test_scan_ignores_empty_and_invalid_files(tmp_path, caplog):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 2, content=b"")
    invalid = tmp_path / "NZ" / "BBB" / "2024" / "invalid.sac"
    invalid.parent.mkdir(parents=True)
    invalid.write_bytes(b"invalid")

    result = scan_waveform_coverage(tmp_path / "NZ")

    assert list(result["station"]) == ["AAA"]
    assert "ignoring invalid waveform file" in caplog.text


def test_summary_uses_seconds_across_the_requested_period(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=86_400)
    _waveform(tmp_path, "AAA", 2024, 2, duration_seconds=43_200)
    coverage = scan_waveform_coverage(tmp_path / "NZ")

    summary = summarize_waveform_coverage(
        coverage, start_date="2024-01-01", end_date="2024-01-03"
    ).iloc[0]

    assert summary["covered_days"] == 2
    assert summary["fully_covered_days"] == 1
    assert summary["coverage_seconds"] == pytest.approx(129_600)
    assert summary["missing_seconds"] == pytest.approx(129_600)
    assert summary["coverage_percent"] == pytest.approx(50)


def test_plot_represents_daily_coverage_as_a_heat_map(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=86_400)
    _waveform(tmp_path, "AAA", 2024, 2, duration_seconds=43_200)
    coverage = scan_waveform_coverage(tmp_path / "NZ")

    figure, axes = plot_waveform_coverage(
        coverage, start_date="2024-01-01", end_date="2024-01-02"
    )

    assert axes.images[0].get_array().tolist() == [[100.0, 50.0]]
    assert axes.get_xlabel() == "Date (UTC)"
    plt.close(figure)


def test_plot_inherits_the_callers_font_family(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=86_400)
    coverage = scan_waveform_coverage(tmp_path / "NZ")

    with plt.rc_context({"font.family": "serif"}):
        figure, axes = plot_waveform_coverage(coverage)

    assert axes.xaxis.label.get_fontfamily() == ["serif"]
    plt.close(figure)


def test_waveform_coverage_exports_figure_and_summary(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1, duration_seconds=86_400)
    figure_path = tmp_path / "reports" / "coverage.pdf"
    csv_path = tmp_path / "reports" / "coverage.csv"

    report = waveform_coverage(
        tmp_path / "NZ",
        start_date="2024-01-01",
        end_date="2024-01-01",
        output_figure=figure_path,
        output_csv=csv_path,
    )

    assert isinstance(report, WaveformCoverageReport)
    assert report.coverage.loc[0, "coverage_percent"] == pytest.approx(100)
    assert figure_path.stat().st_size > 0
    assert csv_path.stat().st_size > 0
    plt.close(report.figure)
