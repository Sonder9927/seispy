import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import obspy
import pandas as pd
from rose import get_logger
from rose.batch import (
    ReportMixin,
    auto_save_report,
    commit_output,
    create_run_id,
    temporary_output_path,
)
from tqdm import tqdm

_LOG_FORMAT = {"file": "format.log", "name": "format", "level": logging.INFO}


@dataclass(frozen=True)
class FormatResult:
    """Describe one sampled SAC header-formatting issue."""

    source: Path
    status: str
    error: str
    destination: Path | None = None


@dataclass(frozen=True)
class _EventSummary:
    files_total: int = 0
    succeeded: int = 0
    failed: int = 0
    conflicts: int = 0
    samples: tuple[FormatResult, ...] = ()


@dataclass(frozen=True)
class FormatSummary(ReportMixin):
    """Summarize a batch SAC header-formatting run.

    This class is returned by :func:`format_head`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        events_total: Number of event directories discovered.
        events_processed: Number matched to event metadata.
        events_skipped: Number without matching event metadata.
        invalid_event_times: Number of unparseable event timestamps.
        files_total: Number of waveform files considered.
        succeeded: Number of files written successfully.
        failed: Number of files that failed processing.
        output_conflicts: Number of existing destinations not overwritten.
        error_samples: Bounded sample of formatting issues.
        output_dir: Root directory containing formatted files.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        >>> summary = format_head(...)
        >>> print(summary.succeeded, summary.failed)
    """

    run_id: str
    events_total: int
    events_processed: int
    events_skipped: int
    invalid_event_times: int
    files_total: int
    succeeded: int
    failed: int
    output_conflicts: int
    error_samples: tuple[FormatResult, ...]
    output_dir: Path
    duration_seconds: float
    report_path: Path | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)

def format_head(
    src_dir: str | Path,
    dest_dir: str | Path,
    events_csv: str | Path,
    stations_csv: str | Path,
    pattern: str = "*.sac",
    max_workers: int = 4,
    *,
    overwrite: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = None,
) -> FormatSummary:
    """Update SAC headers from event and station tables.

    Args:
        src_dir: Root containing one directory per event.
        dest_dir: Output root; it must be outside ``src_dir``.
        events_csv: Event table with time, latitude, longitude, and magnitude.
        stations_csv: Station table with station, latitude, and longitude.
        pattern: File pattern evaluated inside each event directory.
        max_workers: Maximum number of event worker processes.
        overwrite: Whether existing output files may be replaced.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Force JSON report creation on or off.

    Returns:
        Event and file counts, sampled issues, and output information.

    Raises:
        NotADirectoryError: If ``src_dir`` is not a directory.
        ValueError: If inputs, limits, or directory placement are invalid.

    Examples:
        >>> summary = format_head(
        ...     "events/raw", "events/formatted", "events.csv", "stations.csv",
        ...     max_workers=1,
        ... )
        >>> summary.output_dir.name
        'formatted'
    """
    started = time.monotonic()
    run_id = create_run_id()
    logger = get_logger(**_LOG_FORMAT)
    src_path = Path(src_dir).expanduser().resolve()
    dest_path = Path(dest_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(src_path)
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if dest_path == src_path or src_path in dest_path.parents:
        raise ValueError("dest_dir must be outside src_dir")
    dest_path.mkdir(parents=True, exist_ok=True)

    events = pd.read_csv(events_csv)
    stations = pd.read_csv(stations_csv)
    _require_columns(events, "events_csv", {"time", "latitude", "longitude", "mag"})
    _require_columns(stations, "stations_csv", {"station", "latitude", "longitude"})
    before = len(events)
    events["time"] = pd.to_datetime(events["time"], utc=True, errors="coerce")
    events = events.dropna(subset=["time"]).copy()
    invalid_times = before - len(events)
    events["event_dir"] = events["time"].dt.strftime("%Y%m%d%H%M%S")
    if events["event_dir"].duplicated().any():
        raise ValueError("events_csv contains duplicate event times at one-second precision")
    events_dict = events.set_index("event_dir").to_dict("index")
    if stations["station"].duplicated().any():
        raise ValueError("stations_csv contains duplicate station names")
    stations_dict = stations.set_index("station").to_dict("index")

    event_dirs = sorted(path for path in src_path.iterdir() if path.is_dir())
    tasks = [(path, events_dict[path.name]) for path in event_dirs if path.name in events_dict]
    skipped = len(event_dirs) - len(tasks)
    logger.info(
        "run_id=%s started src=%s dest=%s events=%d skipped=%d pattern=%s "
        "overwrite=%s max_workers=%d",
        run_id, src_path, dest_path, len(tasks), skipped, pattern, overwrite, max_workers,
    )
    summaries = []
    worker_limit = min(max_error_samples, 1)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                format_per_event, event_dir, event_info, dest_path, pattern,
                stations_dict, overwrite, worker_limit,
            ): event_dir
            for event_dir, event_info in tasks
        }
        with tqdm(total=len(tasks), desc="Formatting events") as bar:
            for future in as_completed(futures):
                event_dir = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    files = sorted(event_dir.glob(pattern))
                    result = _failed_event(files, exc, worker_limit)
                summaries.append(result)
                bar.update(1)

    combined = _combine(summaries, max_error_samples)
    summary = FormatSummary(
        run_id=run_id,
        events_total=len(event_dirs),
        events_processed=len(tasks),
        events_skipped=skipped,
        invalid_event_times=invalid_times,
        files_total=combined.files_total,
        succeeded=combined.succeeded,
        failed=combined.failed,
        output_conflicts=combined.conflicts,
        error_samples=combined.samples,
        output_dir=dest_path,
        duration_seconds=round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(summary, "format", save_report)
    if summary.report_path:
        logger.info("run_id=%s report=%s", run_id, summary.report_path)
    logger.info(
        "run_id=%s completed events=%d skipped=%d files=%d succeeded=%d "
        "failed=%d conflicts=%d invalid_times=%d duration=%.3f",
        run_id, summary.events_processed, summary.events_skipped, summary.files_total,
        summary.succeeded, summary.failed, summary.output_conflicts,
        summary.invalid_event_times, summary.duration_seconds,
    )
    for item in summary.error_samples:
        logger.error("run_id=%s status=%s source=%s destination=%s error=%s",
                     run_id, item.status, item.source, item.destination, item.error)
    if summary.failed > len(summary.error_samples):
        logger.warning("run_id=%s error_samples_truncated shown=%d total=%d",
                       run_id, len(summary.error_samples), summary.failed)
    print(f"SAC formatting [{run_id}]: {summary.succeeded} succeeded, "
          f"{summary.failed} failed, {summary.events_skipped} events skipped.")
    return summary


def format_per_event(
    event_dir, event_info, dest_path, pattern, stations_dict,
    overwrite=False, max_error_samples=1,
):
    files = sorted(event_dir.glob(pattern))
    succeeded = failed = conflicts = 0
    samples = []
    for source in files:
        destination = None
        temporary = None
        try:
            parts = source.stem.split(".")
            if len(parts) < 3:
                raise ValueError("expected filename event.station.channel.sac")
            station, channel = parts[1], parts[2]
            if station not in stations_dict:
                raise KeyError(f"station {station!r} not found in stations_csv")
            destination = dest_path / event_dir.name / f"{event_dir.name}.{station}.{channel}.sac"
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists() and not overwrite:
                raise FileExistsError(destination)
            trace = obspy.read(str(source))[0]
            _update_header(trace, station, channel, stations_dict[station], event_info)
            temporary = temporary_output_path(destination)
            trace.write(str(temporary), format="SAC")
            commit_output(temporary, destination, overwrite=overwrite)
            succeeded += 1
        except Exception as exc:
            if temporary:
                temporary.unlink(missing_ok=True)
            failed += 1
            conflict = isinstance(exc, FileExistsError)
            conflicts += int(conflict)
            if len(samples) < max_error_samples:
                samples.append(FormatResult(
                    source, "output_conflict" if conflict else "format_failed",
                    f"{type(exc).__name__}: {exc}", destination,
                ))
    return _EventSummary(len(files), succeeded, failed, conflicts, tuple(samples))


def _update_header(trace, station, channel, station_info, event_info):
    stats = trace.stats
    stats.station = station
    stats.channel = channel
    if not stats.location:
        stats.location = "10"
    sac = stats.sac
    sac.stla = _table_value(station_info, "latitude")
    sac.stlo = _table_value(station_info, "longitude")
    sac.stel = _table_value(station_info, "elevation", -12345)
    sac.stdp = _table_value(station_info, "depth", -12345)
    sac.evla = _table_value(event_info, "latitude")
    sac.evlo = _table_value(event_info, "longitude")
    sac.evel = _table_value(event_info, "elevation", -12345)
    sac.evdp = _table_value(event_info, "depth", -12345)
    sac.mag = _table_value(event_info, "mag")
    sac.lcalda = 1


def _table_value(values, key, default=None):
    value = values.get(key, default)
    if pd.isna(value):
        value = default
    if value is None:
        raise ValueError(f"missing required metadata: {key}")
    return value


def _require_columns(frame, name, required):
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing columns: {', '.join(sorted(missing))}")


def _failed_event(files, exc, limit):
    samples = (FormatResult(files[0], "event_failed",
                            f"{type(exc).__name__}: {exc}"),) if files and limit else ()
    return _EventSummary(len(files), 0, len(files), 0, samples)


def _combine(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[:max(0, limit - len(samples))])
    return _EventSummary(
        sum(x.files_total for x in items), sum(x.succeeded for x in items),
        sum(x.failed for x in items), sum(x.conflicts for x in items), tuple(samples),
    )
