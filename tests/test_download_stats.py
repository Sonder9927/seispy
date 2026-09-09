from datetime import date

import matplotlib.pyplot as plt
import pytest

from seispy.download import (
    DownloadAnalysis,
    download_status,
    plot_download_availability,
    scan_download_availability,
    summarize_download_availability,
)


def _waveform(root, station, year, julday, name="trace.sac", content=b"data"):
    path = root / station / str(year) / f"{julday:03d}" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path


def test_scan_ignores_empty_files_and_invalid_directories(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 2, content=b"")
    _waveform(tmp_path, "BBB", 2024, 366, name="trace.mseed")
    _waveform(tmp_path, "BBB", 2023, 366)
    _waveform(tmp_path, "CCC", 2024, 3, name="notes.txt")

    result = scan_download_availability(tmp_path)

    assert list(result["station"]) == ["AAA", "BBB"]
    assert list(result["date"].dt.date) == [date(2024, 1, 1), date(2024, 12, 31)]
    assert list(result["file_count"]) == [1, 1]
    assert list(result["size_bytes"]) == [4, 4]


def test_summary_reports_missing_days_for_requested_period(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 3)
    _waveform(tmp_path, "BBB", 2024, 2)
    availability = scan_download_availability(tmp_path)

    summary = summarize_download_availability(
        availability, start_date="2024-01-01", end_date="2024-01-03"
    ).set_index("station")

    assert summary.loc["AAA", "available_days"] == 2
    assert summary.loc["AAA", "missing_days"] == 1
    assert summary.loc["AAA", "availability_percent"] == pytest.approx(200 / 3)
    assert summary.loc["BBB", "availability_percent"] == pytest.approx(100 / 3)


def test_plot_saves_publication_ready_vector_output(tmp_path):
    _waveform(tmp_path, "AAA", 2024, 1)
    _waveform(tmp_path, "AAA", 2024, 2)
    _waveform(tmp_path, "BBB", 2024, 3)
    availability = scan_download_availability(tmp_path)
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
        tmp_path,
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
