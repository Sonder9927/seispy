"""Inspect and visualize daily waveform download availability."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
from obspy import read

from seispy.archive import (
    WaveformIdentity,
    matches_mseed_path,
    stream_day_identity,
)

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

_AVAILABILITY_COLUMNS = ["station", "date", "file_count", "size_bytes"]
_SUMMARY_COLUMNS = [
    "station",
    "available_days",
    "expected_days",
    "missing_days",
    "availability_percent",
    "first_date",
    "last_date",
]
_OKABE_ITO_BLUE = "#0072B2"
_DEFAULT_FONT = "DejaVu Sans"
_CJK_FONT_CANDIDATES = (
    "Noto Sans CJK SC",
    "Source Han Sans SC",
    "PingFang SC",
    "Microsoft YaHei",
    "Hiragino Sans GB",
    "SimHei",
    "Arial Unicode MS",
)
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DownloadAvailabilityReport:
    """Results produced by :func:`download_status`.

    Attributes:
        availability: One row per available station-day.
        summary: Completeness statistics for each station.
        figure: Matplotlib availability figure.
        axes: Main timeline axes.
        figure_path: Saved figure path, when requested.
        csv_path: Saved summary path, when requested.
    """

    availability: pd.DataFrame
    summary: pd.DataFrame
    figure: Figure
    axes: Axes
    figure_path: Path | None
    csv_path: Path | None


def download_status(
    data_dir: str | Path,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    extensions: Iterable[str] = (".sac", ".mseed"),
    station_order: Literal["name", "availability"] = "name",
    output_figure: str | Path | None = None,
    output_csv: str | Path | None = None,
    color: str = _OKABE_ITO_BLUE,
    title: str = "Waveform data availability",
    dpi: int = 300,
) -> DownloadAvailabilityReport:
    """Scan, summarize, and plot a waveform download archive in one call.

    The figure combines station timelines with completeness percentages. The
    detailed table and plot can optionally be exported as CSV and PNG, SVG, or
    PDF files.

    Args:
        data_dir: Network directory containing station subdirectories.
        start_date: Inclusive analysis start. Defaults to the first observed day.
        end_date: Inclusive analysis end. Defaults to the last observed day.
        extensions: Waveform suffixes to include.
        station_order: Sort stations alphabetically or by available-day count.
        output_figure: Optional PNG, SVG, or PDF destination.
        output_csv: Optional completeness-table destination.
        color: Matplotlib-compatible availability color.
        title: Figure title.
        dpi: Raster resolution used when saving.

    Returns:
        Availability data, summary table, figure, axes, and saved paths.

    Examples:
        ```python
        report = download_status(
            "data/mseed/NZ",
            start_date="2024-01-01",
            end_date="2024-12-31",
            output_figure="availability.pdf",
            output_csv="availability.csv",
        )
        report.summary.columns[0]
        # => 'station'
        ```
    """
    availability = scan_download_availability(
        data_dir,
        start_date=start_date,
        end_date=end_date,
        extensions=extensions,
    )
    summary = summarize_download_availability(
        availability, start_date=start_date, end_date=end_date
    )
    figure, axes = plot_download_availability(
        availability,
        start_date=start_date,
        end_date=end_date,
        station_order=station_order,
        color=color,
        title=title,
        dpi=dpi,
    )
    percentages = summary.set_index("station")["availability_percent"]
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
        "Completeness",
        xy=(1.015, 1.02),
        xycoords="axes fraction",
        ha="left",
        va="bottom",
        fontsize=8.5,
        fontweight="semibold",
        annotation_clip=False,
    )
    figure_path = _save_figure(figure, output_figure, dpi)
    csv_path = _save_summary(summary, output_csv)
    return DownloadAvailabilityReport(
        availability, summary, figure, axes, figure_path, csv_path
    )


def scan_download_availability(
    data_dir: str | Path,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    extensions: Iterable[str] = (".sac", ".mseed"),
) -> pd.DataFrame:
    """Scan a flattened ``station/year/files`` waveform archive.

    Station and date identities are read from waveform headers. Files whose
    canonical paths disagree with their headers are reported and ignored.

    Args:
        data_dir: Network directory containing station subdirectories.
        start_date: Optional inclusive lower date bound.
        end_date: Optional inclusive upper date bound.
        extensions: Waveform suffixes to include, matched case-insensitively.

    Returns:
        One row per available station-day with station, date, file count, and
        total size in bytes. Rows are sorted by station and date.

    Raises:
        FileNotFoundError: If ``data_dir`` is not a directory.
        ValueError: If the date range or extensions are invalid.

    Examples:
        ```python
        availability = scan_download_availability("data/mseed/NZ")
        list(availability.columns)
        # => ['station', 'date', 'file_count', 'size_bytes']
        ```
    """
    root = Path(data_dir).expanduser()
    if not root.is_dir():
        raise FileNotFoundError(f"download directory does not exist: {root}")
    start, end = _date_bounds(start_date, end_date)
    suffixes = _normalize_extensions(extensions)
    archive_root = root.parent
    grouped: dict[tuple[str, date], list[Path]] = {}
    candidates = sorted(
        path
        for path in root.rglob("*")
        if path.is_file() and path.suffix.lower() in suffixes
    )
    for path in candidates:
        if path.stat().st_size == 0:
            continue
        try:
            stream = read(path, headonly=True)
            if path.suffix.lower() == ".sac":
                if len(stream) != 1:
                    raise ValueError("SAC file must contain exactly one trace")
                identity = WaveformIdentity.from_trace(stream[0])
                if not identity.matches_sac_path(path, archive_root):
                    raise ValueError(
                        "filename or directory does not match the SAC header"
                    )
                identities = [identity]
            else:
                network, station, year, julday = stream_day_identity(stream)
                if not matches_mseed_path(path, archive_root, stream):
                    raise ValueError(
                        "filename or directory does not match the MiniSEED header"
                    )
                identities = [WaveformIdentity.from_trace(stream[0])]
                if (network, station, year, julday) != (
                    identities[0].network,
                    identities[0].station,
                    identities[0].year,
                    identities[0].julday,
                ):
                    raise ValueError("inconsistent MiniSEED stream identity")
            for identity in identities:
                observed = identity.day
                if (start and observed < start) or (end and observed > end):
                    continue
                grouped.setdefault((identity.station, observed), []).append(path)
        except Exception as exc:
            logger.warning("ignoring invalid waveform file %s: %s", path, exc)
    rows = [
        {
            "station": station,
            "date": pd.Timestamp(observed),
            "file_count": len(set(files)),
            "size_bytes": sum(path.stat().st_size for path in set(files)),
        }
        for (station, observed), files in grouped.items()
    ]
    return pd.DataFrame(rows, columns=_AVAILABILITY_COLUMNS).sort_values(
        ["station", "date"], ignore_index=True
    )


def summarize_download_availability(
    availability: pd.DataFrame,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
) -> pd.DataFrame:
    """Calculate daily completeness for every station.

    Args:
        availability: Result from :func:`scan_download_availability`.
        start_date: Inclusive start used to calculate expected days. Defaults to
            the earliest observed date.
        end_date: Inclusive end used to calculate expected days. Defaults to the
            latest observed date.

    Returns:
        Per-station counts, availability percentage, and observed date limits.

    Examples:
        ```python
        summary = summarize_download_availability(availability)
        summary.sort_values("availability_percent", ascending=False).head()
        ```
    """
    frame = _validate_availability(availability)
    if frame.empty:
        return pd.DataFrame(columns=_SUMMARY_COLUMNS)
    start, end = _date_bounds(start_date, end_date)
    start = start or frame["date"].min().date()
    end = end or frame["date"].max().date()
    if start > end:
        raise ValueError("start_date must not be later than end_date")
    expected = (end - start).days + 1
    selected = frame[frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))]
    grouped = selected.groupby("station", sort=True)["date"]
    summary = grouped.agg(available_days="nunique", first_date="min", last_date="max")
    summary["expected_days"] = expected
    summary["missing_days"] = expected - summary["available_days"]
    summary["availability_percent"] = summary["available_days"] / expected * 100
    return summary.reset_index()[_SUMMARY_COLUMNS]


def plot_download_availability(
    availability: pd.DataFrame,
    output_file: str | Path | None = None,
    *,
    start_date: str | date | datetime | None = None,
    end_date: str | date | datetime | None = None,
    station_order: Literal["name", "availability"] = "name",
    color: str = _OKABE_ITO_BLUE,
    title: str = "Waveform data availability",
    dpi: int = 300,
) -> tuple[Figure, Axes]:
    """Plot station availability as a compact scientific timeline.

    The default color is the colorblind-safe Okabe-Ito blue. Saving format is
    inferred from ``output_file``; PNG, SVG, and PDF are suitable choices.

    Args:
        availability: Result from :func:`scan_download_availability`.
        output_file: Optional figure destination.
        start_date: Optional inclusive plot start.
        end_date: Optional inclusive plot end.
        station_order: Sort stations alphabetically or by available-day count.
        color: Matplotlib-compatible availability color.
        title: Figure title.
        dpi: Raster resolution used when saving.

    Returns:
        The Matplotlib figure and axes for further customization.

    Examples:
        ```python
        fig, ax = plot_download_availability(
            availability, "availability.pdf", station_order="availability"
        )
        ```
    """
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt

    frame = _validate_availability(availability)
    if frame.empty:
        raise ValueError("availability contains no station-days to plot")
    start, end = _date_bounds(start_date, end_date)
    start = start or frame["date"].min().date()
    end = end or frame["date"].max().date()
    selected = frame[frame["date"].between(pd.Timestamp(start), pd.Timestamp(end))]
    if selected.empty:
        raise ValueError("no station-days fall inside the requested date range")
    counts = selected.groupby("station")["date"].nunique()
    if station_order == "name":
        stations = sorted(counts.index)
    elif station_order == "availability":
        stations = list(counts.sort_values(ascending=False).index)
    else:
        raise ValueError("station_order must be 'name' or 'availability'")

    height = max(3.2, min(14.0, 1.2 + 0.32 * len(stations)))
    font_family = _plot_font_family((title, *stations))
    with plt.rc_context(
        {
            "font.family": font_family,
            "font.size": 9,
            "axes.titleweight": "semibold",
        }
    ):
        fig, ax = plt.subplots(figsize=(10.0, height), constrained_layout=True)
        for row, station in enumerate(stations):
            if row % 2 == 0:
                ax.axhspan(row - 0.45, row + 0.45, color="#F2F2F2", zorder=0)
            dates = selected.loc[selected["station"] == station, "date"]
            spans = [
                (
                    mdates.date2num(interval_start),
                    (interval_end - interval_start).days + 1,
                )
                for interval_start, interval_end in _continuous_intervals(dates)
            ]
            ax.broken_barh(
                spans,
                (row - 0.32, 0.64),
                facecolors=color,
                edgecolors="none",
                alpha=0.9,
                zorder=2,
            )
        locator = mdates.AutoDateLocator(minticks=4, maxticks=10)
        ax.xaxis.set_major_locator(locator)
        ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(locator))
        ax.set_xlim(mdates.date2num(start), mdates.date2num(end + timedelta(days=1)))
        ax.set_ylim(-0.6, len(stations) - 0.4)
        ax.set_yticks(range(len(stations)), labels=stations)
        ax.set_xlabel("Date (UTC)")
        ax.set_ylabel("Station")
        ax.set_title(title, loc="left")
        ax.grid(axis="x", color="#B3B3B3", linewidth=0.6, alpha=0.55)
        ax.set_axisbelow(True)
        ax.spines[["top", "right", "left"]].set_visible(False)
        ax.tick_params(axis="y", length=0)
        if output_file is not None:
            _save_figure(fig, output_file, dpi)
    return fig, ax


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


def _julian_date(year, value):
    try:
        julian_day = int(value)
        observed = date(year, 1, 1) + timedelta(days=julian_day - 1)
    except (TypeError, ValueError, OverflowError):
        return None
    return observed if 1 <= julian_day <= 366 and observed.year == year else None


def _validate_availability(availability):
    missing = {"station", "date"}.difference(availability.columns)
    if missing:
        raise ValueError(
            f"availability is missing columns: {', '.join(sorted(missing))}"
        )
    frame = availability.copy()
    frame["date"] = pd.to_datetime(frame["date"]).dt.normalize()
    return frame


def _continuous_intervals(values):
    dates = sorted({timestamp.date() for timestamp in pd.to_datetime(values)})
    if not dates:
        return []
    intervals = []
    start = previous = dates[0]
    for current in dates[1:]:
        if current != previous + timedelta(days=1):
            intervals.append((start, previous))
            start = current
        previous = current
    intervals.append((start, previous))
    return intervals


def _save_figure(figure, output_file, dpi):
    if output_file is None:
        return None
    destination = Path(output_file).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    import matplotlib.pyplot as plt

    with plt.rc_context(
        {"pdf.fonttype": 42, "ps.fonttype": 42, "svg.fonttype": "none"}
    ):
        figure.savefig(destination, dpi=dpi, bbox_inches="tight", facecolor="white")
    return destination


def _plot_font_family(labels) -> str:
    """Choose an installed, conventional font without inheriting user config."""
    if not any(_contains_cjk(str(label)) for label in labels):
        return _DEFAULT_FONT

    from matplotlib import font_manager

    installed = {font.name for font in font_manager.fontManager.ttflist}
    return next(
        (name for name in _CJK_FONT_CANDIDATES if name in installed),
        _DEFAULT_FONT,
    )


def _contains_cjk(value: str) -> bool:
    return any(
        "\u3400" <= character <= "\u4dbf"
        or "\u4e00" <= character <= "\u9fff"
        or "\uf900" <= character <= "\ufaff"
        for character in value
    )


def _save_summary(summary, output_csv):
    if output_csv is None:
        return None
    destination = Path(output_csv).expanduser()
    destination.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(destination, index=False, encoding="utf-8")
    return destination
