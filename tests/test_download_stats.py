from datetime import date
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pytest
from obspy import Trace, UTCDateTime

from seispy.download import (
    DownloadAnalysis,
    download_status,
    plot_download_availability,
    scan_download_availability,
    summarize_download_availability,
)


def _waveform(root, station, year, julday, name="trace.sac", content=b"data"):
    network_root = root / "NZ"
    starttime = UTCDateTime(year=year, julday=julday)
    trace = Trace(data=np.arange(10, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = station
    trace.stats.location = ""
    trace.stats.channel = "BHZ"
    trace.stats.starttime = starttime
    suffix = Path(name).suffix.lower()
    if suffix == ".mseed":
        filename = f"NZ.{station}.{year}.{julday:03d}.mseed"
        path = network_root / station / str(year) / filename
        output_format = "MSEED"
    elif suffix == ".sac":
        filename = f"NZ.{station}..BHZ.D.{year}.{julday:03d}.000000.sac"
        path = network_root / station / str(year) / filename
        output_format = "SAC"
    else:
        path = network_root / station / str(year) / name
        output_format = None
    path.parent.mkdir(parents=True, exist_ok=True)
    if not content:
        path.write_bytes(content)
    elif output_format:
        trace.write(str(path), format=output_format)
    else:
        path.write_bytes(content)
    return path


def test_scan_ignores_empty_files_and_invalid_directories(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 2, content=b"")
    _waveform(tmp_path, "BBB", 2024, 366, name="trace.mseed")
    invalid = tmp_path / "NZ" / "BBB" / "2023" / "invalid.sac"
    invalid.parent.mkdir(parents=True)
    invalid.write_bytes(b"not a SAC file")
    _waveform(tmp_path, "CCC", 2024, 3, name="notes.txt")

    result = scan_download_availability(tmp_path / "NZ")

    assert list(result["station"]) == ["AAA", "BBB"]
    assert list(result["date"].dt.date) == [date(2024, 1, 1), date(2024, 12, 31)]
    assert list(result["file_count"]) == [1, 1]
    assert all(size > 0 for size in result["size_bytes"])


def test_summary_reports_missing_days_for_requested_period(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 3)
    _waveform(tmp_path, "BBB", 2024, 2)
    availability = scan_download_availability(tmp_path / "NZ")

    summary = summarize_download_availability(
        availability, start_date="2024-01-01", end_date="2024-01-03"
    ).set_index("station")

    assert summary.loc["AAA", "available_days"] == 2
    assert summary.loc["AAA", "missing_days"] == 1
    assert summary.loc["AAA", "availability_percent"] == pytest.approx(200 / 3)
    assert summary.loc["BBB", "availability_percent"] == pytest.approx(100 / 3)


def test_scan_ignores_valid_sac_whose_path_disagrees_with_header(tmp_path, caplog):
    canonical = _waveform(tmp_path, "AAA", 2024, 1)
    wrong = canonical.with_name("wrong.sac")
    canonical.replace(wrong)

    result = scan_download_availability(tmp_path / "NZ")

    assert result.empty
    assert "does not match the SAC header" in caplog.text


def test_plot_saves_publication_ready_vector_output(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 2)
    _waveform(tmp_path, "BBB", 2024, 3)
    availability = scan_download_availability(tmp_path / "NZ")
    output = tmp_path / "figures" / "availability.svg"

    figure, axes = plot_download_availability(
        availability,
        output,
        start_date="2024-01-01",
        end_date="2024-01-03",
        station_order="availability",
    )

    assert output.is_file()
    assert output.stat().st_size > 0
    assert axes.get_xlabel() == "Date (UTC)"
    plt.close(figure)


def test_download_status_exports_combined_figure_and_summary(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 2)
    _waveform(tmp_path, "BBB", 2024, 1)
    figure_path = tmp_path / "reports" / "availability.pdf"
    csv_path = tmp_path / "reports" / "availability.csv"

    report = download_status(
        tmp_path / "NZ",
        start_date="2024-01-01",
        end_date="2024-01-02",
        output_figure=figure_path,
        output_csv=csv_path,
    )

    assert isinstance(report, DownloadAnalysis)
    assert report.figure_path == figure_path
    assert report.csv_path == csv_path
    assert figure_path.stat().st_size > 0
    assert csv_path.stat().st_size > 0
    assert [text.get_text() for text in report.axes.texts] == [
        "100.0%",
        "50.0%",
        "Completeness",
    ]
    plt.close(report.figure)
