"""Measure and visualize waveform archive coverage from trace headers."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
from obspy import UTCDateTime, read

from seispy.archive import WaveformIdentity, matches_mseed_path

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

_SECONDS_PER_DAY = 86_400.0
_COVERAGE_COLUMNS = [
    "network",
    "station",
    "date",
    "read_mode",
    "file_count",
    "trace_count",
    "size_bytes",
    "coverage_seconds",
    "coverage_percent",
]
_SUMMARY_COLUMNS = [
    "network",
    "station",
    "covered_days",
    "expected_days",
    "fully_covered_days",
    "coverage_seconds",
    "expected_seconds",
    "missing_seconds",
    "coverage_percent",
    "first_date",
    "last_date",
]
_OKABE_ITO_BLUE = "#0072B2"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaveformCoverageReport:
    """Daily and aggregate waveform coverage plus its optional artifacts."""

    coverage: pd.DataFrame
    summary: pd.DataFrame
    figure: Figure
    axes: Axes
    figure_path: Path | None
    csv_path: Path | None


def waveform_coverage(
    net_dir: str | Path,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    extensions: Iterable[str] = (".sac", ".mseed"),
    read_mode: Literal["filename", "header"] = "header",
    station_order: Literal["name", "coverage"] = "name",
    output_figure: str | Path | None = None,
    output_csv: str | Path | None = None,
    color: str = _OKABE_ITO_BLUE,
    title: str = "Waveform data coverage",
    dpi: int = 300,
) -> WaveformCoverageReport:
    """Measure, summarize, and plot waveform time coverage.

    ``read_mode="header"`` measures the union of trace sample intervals,
    clipped to UTC days. ``read_mode="filename"`` avoids waveform reads and
    estimates each valid canonical station-day filename as fully covered.
    """
    coverage = scan_waveform_coverage(
        net_dir,
        start_date=start_date,
        end_date=end_date,
        extensions=extensions,
        read_mode=read_mode,
    )
    summary = summarize_waveform_coverage(
        coverage, start_date=start_date, end_date=end_date
    )
    figure, axes = plot_waveform_coverage(
        coverage,
        start_date=start_date,
        end_date=end_date,
        station_order=station_order,
        color=color,
        title=title,
        dpi=dpi,
    )
    percentages = summary.set_index("station")["coverage_percent"]
    for row, label in enumerate(axes.get_yticklabels()):
        axes.annotate(
            f"{percentages[label.get_text()]:.1f}%",
            xy=(1.015, row),
            xycoords=("axes fraction", "data"),
            ha="left",
            va="center",
            fontsize=8.5,
            color="#333333",
            annotation_clip=False,
        )
    axes.annotate(
        "Coverage",
        xy=(1.015, 1.02),
        xycoords="axes fraction",
        ha="left",
        va="bottom",
        fontsize=8.5,
        fontweight="bold",
        annotation_clip=False,
    )
    figure_path = _save_figure(figure, output_figure, dpi)
    csv_path = _save_summary(summary, output_csv)
    return WaveformCoverageReport(
        coverage, summary, figure, axes, figure_path, csv_path
    )


def scan_waveform_coverage(
    net_dir: str | Path,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    extensions: Iterable[str] = (".sac", ".mseed"),
    read_mode: Literal["filename", "header"] = "header",
) -> pd.DataFrame:
    """Return measured or filename-estimated coverage for each station-day."""
    root = Path(net_dir).expanduser()
    if not root.is_dir():
        raise FileNotFoundError(f"waveform directory does not exist: {root}")
    start, end = _date_bounds(start_date, end_date)
    suffixes = _normalize_extensions(extensions)
    if read_mode not in {"filename", "header"}:
        raise ValueError("read_mode must be 'filename' or 'header'")
    archive_root = root.parent
    grouped: dict[tuple[str, str, date], dict[str, object]] = {}
    candidates = (
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in suffixes
    )
    for path in candidates:
        size = path.stat().st_size
        if size == 0:
            continue
        try:
            if read_mode == "filename":
                network, station, observed = _filename_identity(path, archive_root)
                if (start and observed < start) or (end and observed > end):
                    continue
                day_start = float(UTCDateTime(observed))
                key = (network, station, observed)
                item = grouped.setdefault(
                    key, {"files": set(), "trace_count": None, "intervals": []}
                )
                item["files"].add(path)
                item["intervals"].append((day_start, day_start + _SECONDS_PER_DAY))
                continue
            stream = read(path, headonly=True)
            _validate_archive_path(path, archive_root, stream)
            for trace in stream:
                identity = WaveformIdentity.from_trace(trace)
                for observed, interval in _daily_trace_intervals(trace):
                    if (start and observed < start) or (end and observed > end):
                        continue
                    key = (identity.network, identity.station, observed)
                    item = grouped.setdefault(
                        key, {"files": set(), "trace_count": 0, "intervals": []}
                    )
                    item["files"].add(path)
                    item["trace_count"] += 1
                    item["intervals"].append(interval)
        except Exception as exc:
            logger.warning("ignoring invalid waveform file %s: %s", path, exc)
    rows = []
    for (network, station, observed), item in grouped.items():
        files = item["files"]
        covered = _interval_union_seconds(item["intervals"])
        rows.append(
            {
                "network": network,
                "station": station,
                "date": pd.Timestamp(observed),
                "read_mode": read_mode,
                "file_count": len(files),
                "trace_count": item["trace_count"],
                "size_bytes": sum(path.stat().st_size for path in files),
                "coverage_seconds": covered,
                "coverage_percent": covered / _SECONDS_PER_DAY * 100,
            }
        )
    return pd.DataFrame(rows, columns=_COVERAGE_COLUMNS).sort_values(
        ["network", "station", "date"], ignore_index=True
    )


def summarize_waveform_coverage(
    coverage: pd.DataFrame,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
) -> pd.DataFrame:
    """Aggregate daily coverage over an inclusive UTC date range."""
    frame = _validate_coverage(coverage)
    if frame.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)
    start, end = _date_bounds(start_date, end_date)
    start = start or frame["date"].min().date()
    end = end or frame["date"].max().date()
    expected_days = (end - start).days + 1
    expected_seconds = expected_days * _SECONDS_PER_DAY
    selected = frame[frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))]
    grouped = selected.groupby(["network", "station"], sort=True)
    summary = grouped.agg(
        covered_days=("date", "nunique"),
        fully_covered_days=(
            "coverage_seconds",
            lambda values: int((values >= _SECONDS_PER_DAY).sum()),
        ),
        coverage_seconds=("coverage_seconds", "sum"),
        first_date=("date", "min"),
        last_date=("date", "max"),
    )
    summary["expected_days"] = expected_days
    summary["expected_seconds"] = expected_seconds
    summary["missing_seconds"] = expected_seconds - summary["coverage_seconds"]
    summary["coverage_percent"] = summary["coverage_seconds"] / expected_seconds * 100
    return summary.reset_index()[_SUMMARY_COLUMNS]


def plot_waveform_coverage(
    coverage: pd.DataFrame,
    output_file: str | Path | None = None,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    station_order: Literal["name", "coverage"] = "name",
    color: str = _OKABE_ITO_BLUE,
    title: str = "Waveform data coverage",
    dpi: int = 300,
) -> tuple[Figure, Axes]:
    """Plot daily coverage percentage as a station-by-day heat map."""
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    frame = _validate_coverage(coverage)
    if frame.empty:
        raise ValueError("coverage contains no station-days to plot")
    start, end = _date_bounds(start_date, end_date)
    start = start or frame["date"].min().date()
    end = end or frame["date"].max().date()
    selected = frame[frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))]
    if selected.empty:
        raise ValueError("no station-days fall inside the requested date range")
    totals = selected.groupby("station")["coverage_seconds"].sum()
    if station_order == "name":
        stations = sorted(totals.index)
    elif station_order == "coverage":
        stations = list(totals.sort_values(ascending=False).index)
    else:
        raise ValueError("station_order must be 'name' or 'coverage'")
    dates = pd.date_range(start, end, freq="D")
    matrix = (
        selected.pivot_table(
            index="station", columns="date", values="coverage_percent", aggfunc="max"
        )
        .reindex(index=stations, columns=dates, fill_value=0)
        .fillna(0)
        .to_numpy()
    )
    height = max(3.2, min(14.0, 1.2 + 0.32 * len(stations)))
    cmap = LinearSegmentedColormap.from_list("waveform_coverage", ["#FFFFFF", color])
    with plt.rc_context({"font.size": 9}):
        fig, ax = plt.subplots(figsize=(10.0, height), constrained_layout=True)
        ax.imshow(
            matrix,
            extent=(
                mdates.date2num(start),
                mdates.date2num(end + timedelta(days=1)),
                -0.5,
                len(stations) - 0.5,
            ),
            origin="lower",
            aspect="auto",
            interpolation="nearest",
            cmap=cmap,
            vmin=0,
            vmax=100,
        )
        locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
        ax.set_yticks(range(len(stations)), labels=stations)
        ax.set_xlabel("Date (UTC)")
        ax.set_ylabel("Station")
        ax.set_title(title, loc="left", fontweight="bold")
        ax.tick_params(axis="y", length=0)
        if output_file is not None:
            _save_figure(fig, output_file, dpi)
    return fig, ax


def _validate_archive_path(path, archive_root, stream) -> None:
    if not len(stream):
        raise ValueError("waveform stream is empty")
    if path.suffix.lower() == ".sac":
        if len(stream) != 1:
            raise ValueError("SAC file must contain exactly one trace")
        if not WaveformIdentity.from_trace(stream[0]).matches_sac_path(
            path, archive_root
        ):
            raise ValueError("filename or directory does not match the SAC header")
    elif not matches_mseed_path(path, archive_root, stream):
        raise ValueError("filename or directory does not match the MiniSEED header")


def _filename_identity(path: Path, archive_root: Path) -> tuple[str, str, date]:
    try:
        relative = path.relative_to(archive_root)
    except ValueError as exc:
        raise ValueError("file is outside the waveform archive") from exc
    if len(relative.parts) != 4:
        raise ValueError("expected network/station/year/file archive layout")
    network_dir, station_dir, year_dir, filename = relative.parts
    stem = Path(filename).stem
    fields = stem.split(".")
    suffix = path.suffix.lower()
    if suffix == ".mseed" and len(fields) == 4:
        network, station, year_text, julday_text = fields
    elif suffix == ".mseed" and len(fields) >= 6:
        network, station, _, _, year_text, julday_text, *_ = fields
    elif suffix == ".sac" and len(fields) == 7:
        network, station, _, _, year_text, julday_text, _ = fields
    else:
        raise ValueError("filename does not match a canonical waveform name")
    if network != network_dir or station != station_dir or year_text != year_dir:
        raise ValueError("filename or directory identity is inconsistent")
    try:
        year = int(year_text)
        julday = int(julday_text)
        observed = date(year, 1, 1) + timedelta(days=julday - 1)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError("filename contains an invalid UTC day") from exc
    if not 1 <= julday <= 366 or observed.year != year:
        raise ValueError("filename contains an invalid UTC day")
    return network, station, observed


def _daily_trace_intervals(trace):
    sample_interval = float(trace.stats.delta)
    start = UTCDateTime(trace.stats.starttime)
    end = UTCDateTime(trace.stats.endtime) + sample_interval
    cursor = start
    while cursor < end:
        observed = cursor.date
        next_day = UTCDateTime(observed + timedelta(days=1))
        clipped_end = min(end, next_day)
        yield observed, (float(cursor), float(clipped_end))
        cursor = clipped_end


def _interval_union_seconds(intervals) -> float:
    ordered = sorted(intervals)
    if not ordered:
        return 0.0
    total = 0.0
    start, end = ordered[0]
    for current_start, current_end in ordered[1:]:
        if current_start <= end:
            end = max(end, current_end)
        else:
            total += end - start
            start, end = current_start, current_end
    return min(_SECONDS_PER_DAY, total + end - start)


def _normalize_extensions(extensions):
    if isinstance(extensions, str):
        extensions = (extensions,)
    suffixes = {
        value.lower() if value.startswith(".") else f".{value.lower()}"
        for value in extensions
    }
    if not suffixes:
        raise ValueError("extensions must contain at least one file suffix")
    return suffixes


def _date_bounds(start, end):
    start_date = pd.Timestamp(start).date() if start is not None else None
    end_date = pd.Timestamp(end).date() if end is not None else None
    if start_date and end_date and start_date > end_date:
        raise ValueError("start_date must not be later than end_date")
    return start_date, end_date


def _validate_coverage(coverage):
    missing = {"network", "station", "date", "coverage_seconds"}.difference(
        coverage.columns
    )
    if missing:
        raise ValueError(f"coverage is missing columns: {', '.join(sorted(missing))}")
    frame = coverage.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return frame


def _save_figure(figure, output_file, dpi):
    if output_file is None:
        return None
    destination = Path(output_file).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(destination, dpi=dpi, bbox_inches="tight", facecolor="white")
    return destination


def _save_summary(summary, output_csv):
    if output_csv is None:
        return None
    destination = Path(output_csv).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(destination, index=False, encoding="utf-8")
    return destination
