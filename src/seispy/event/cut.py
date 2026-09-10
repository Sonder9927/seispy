import datetime
import logging
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import obspy
from rose import get_logger
from rose.batch import ReportMixin, auto_save_report, create_run_id
from tqdm import tqdm

from seispy._waveform import merge_short_gaps
from seispy.event.catalog import load_events, load_stations

_LOG_CUTEVENT = {
    "file": "cutevent.log",
    "name": "cut_event",
    "level": logging.INFO,
}


@dataclass(frozen=True)
class CutEventResult:
    """A sampled event/station processing issue."""

    event: str
    station: str
    status: str
    error: str
    source: Path | None = None


@dataclass(frozen=True)
class _CutCounts:
    tasks: int = 0
    succeeded: int = 0
    failed: int = 0
    outputs: int = 0
    read_failed: int = 0
    no_data: int = 0
    samples: tuple[CutEventResult, ...] = ()


@dataclass(frozen=True)
class CutEventSummary(ReportMixin):
    """Summarize an event-by-station waveform cutting run.

    This class is returned by :func:`cut_events`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        tasks_total: Number of event-station combinations considered.
        tasks_succeeded: Number that produced at least one output.
        tasks_failed: Number that did not complete successfully.
        outputs_written: Number of event waveform files written.
        input_read_failed: Number of source waveform read failures.
        no_data: Number of tasks with no usable waveform data.
        error_samples: Bounded sample of cutting issues.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        >>> summary = cut_events(...)
        >>> print(summary.outputs_written, summary.tasks_failed)
    """

    run_id: str
    tasks_total: int
    tasks_succeeded: int
    tasks_failed: int
    outputs_written: int
    input_read_failed: int
    no_data: int
    error_samples: tuple[CutEventResult, ...]
    duration_seconds: float
    report_path: Path | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.tasks_failed or self.input_read_failed)

def cut_events(
    src_dir: str | Path,
    dest_dir: str | Path,
    event_csv: str | Path,
    station_csv: str | Path | None = None,
    time_window: float = 10800,
    *,
    max_error_samples: int = 20,
    save_report: bool | None = None,
) -> CutEventSummary:
    """Cut event windows from continuous SAC data.

    Args:
        src_dir: Root directory containing continuous data by station.
        dest_dir: Destination root for event waveform files.
        event_csv: Event table consumed by :func:`load_events`.
        station_csv: Optional station table. Directory names are used if omitted.
        time_window: Window length after each event origin, in seconds.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Force JSON report creation on or off.

    Returns:
        Task counts, written output count, sampled issues, and run duration.

    Raises:
        ValueError: If error sampling or input metadata is invalid.

    Examples:
        >>> summary = cut_events(
        ...     "data/continuous", "data/events", "events.csv",
        ...     station_csv="stations.csv", time_window=10_800,
        ... )
        >>> summary.tasks_total >= summary.tasks_succeeded
        True
    """
    started = time.monotonic()
    run_id = create_run_id()
    logger = get_logger(**_LOG_CUTEVENT)
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")

    events = load_events(event_csv, time_window)
    stations = load_stations(src_dir, station_csv)
    logger.info(
        "run_id=%s started src=%s dest=%s stations=%d events=%d "
        "time_window=%s",
        run_id,
        src_dir,
        dest_dir,
        len(stations),
        len(events),
        time_window,
    )

    total = len(events) * len(stations)
    tasks_done = succeeded = failed = outputs = read_failed = no_data = 0
    samples = []
    with tqdm(total=total, desc="Processing...") as pbar:
        for station in stations:
            for event in events:
                result = cut_event_station(
                    event,
                    station,
                    src_dir,
                    dest_dir,
                    max_error_samples=min(max_error_samples, 1),
                )
                tasks_done += result.tasks
                succeeded += result.succeeded
                failed += result.failed
                outputs += result.outputs
                read_failed += result.read_failed
                no_data += result.no_data
                samples.extend(
                    result.samples[: max(0, max_error_samples - len(samples))]
                )
                pbar.update(1)
    summary = CutEventSummary(
        run_id=run_id,
        tasks_total=tasks_done,
        tasks_succeeded=succeeded,
        tasks_failed=failed,
        outputs_written=outputs,
        input_read_failed=read_failed,
        no_data=no_data,
        error_samples=tuple(samples),
        duration_seconds=round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(summary, "cut-events", save_report)
    if summary.report_path:
        logger.info("run_id=%s report=%s", run_id, summary.report_path)
    logger.info(
        "run_id=%s completed tasks=%d succeeded=%d failed=%d outputs=%d "
        "read_failed=%d no_data=%d duration=%.3f",
        run_id,
        summary.tasks_total,
        summary.tasks_succeeded,
        summary.tasks_failed,
        summary.outputs_written,
        summary.input_read_failed,
        summary.no_data,
        summary.duration_seconds,
    )
    for item in summary.error_samples:
        logger.error(
            "run_id=%s status=%s event=%s station=%s source=%s error=%s",
            run_id,
            item.status,
            item.event,
            item.station,
            item.source,
            item.error,
        )
    issue_total = summary.tasks_failed + summary.input_read_failed
    if issue_total > len(summary.error_samples):
        logger.warning(
            "run_id=%s error_samples_truncated shown=%d total_issues=%d",
            run_id,
            len(summary.error_samples),
            issue_total,
        )
    print(
        f"Cut events complete [{run_id}]: {summary.tasks_succeeded} succeeded, "
        f"{summary.tasks_failed} failed, {summary.outputs_written} outputs written."
    )
    return summary


def cut_event_station(
    event, station, src_dir, dest_dir, *, max_error_samples=1
) -> _CutCounts:
    """处理单个事件-台站组合"""
    # 生成时间覆盖范围
    year_jdays = _calculate_julian_dates(event["start"], event["end"])

    # 获取所有可能相关的SAC文件路径
    station_name = station["station"]
    event_name = event["start"].strftime("%Y%m%d%H%M%S")
    sac_files = _target_paths(src_dir, station_name, year_jdays)
    if not sac_files:
        samples = ()
        if max_error_samples:
            samples = (
                CutEventResult(
                    event_name,
                    station_name,
                    "no_data",
                    "no matching SAC files found",
                ),
            )
        return _CutCounts(tasks=1, failed=1, no_data=1, samples=samples)

    samples = []
    read_failed = 0
    had_issue = False
    channel_data = {}
    for sac_path in sac_files:
        try:
            st = obspy.read(sac_path)
            tr = st[0]
            if tr.stats.station != station_name:
                raise ValueError("station in SAC header does not match directory")
            channel = tr.stats.channel
            if channel in channel_data:
                channel_data[channel] += st
            else:
                channel_data[channel] = st
        except Exception as exc:
            read_failed += 1
            had_issue = True
            if len(samples) < max_error_samples:
                samples.append(
                    CutEventResult(
                        event_name,
                        station_name,
                        "input_read_failed",
                        f"{type(exc).__name__}: {exc}",
                        Path(sac_path),
                    )
                )

    outputs = 0
    if channel_data:
        event_dir = Path(dest_dir) / event_name
        event_dir.mkdir(parents=True, exist_ok=True)
        for channel, stream in channel_data.items():
            try:
                merged_tr = merge_short_gaps(stream)[0]
                trimed_tr = _trimmed_trace(merged_tr, event, station)
                out_name = f"{event_name}.{station_name}.{channel}.sac"
                trimed_tr.write(str(event_dir / out_name), format="SAC")
                outputs += 1
            except Exception as exc:
                had_issue = True
                if len(samples) < max_error_samples:
                    samples.append(
                        CutEventResult(
                            event_name,
                            station_name,
                            "output_failed",
                            f"{type(exc).__name__}: {exc}",
                        )
                    )
    if not outputs:
        had_issue = True
        if len(samples) < max_error_samples:
            samples.append(
                CutEventResult(
                    event_name,
                    station_name,
                    "no_output",
                    "no event channel was written",
                )
            )
    return _CutCounts(
        tasks=1,
        succeeded=int(not had_issue),
        failed=int(had_issue),
        outputs=outputs,
        read_failed=read_failed,
        samples=tuple(samples),
    )


def _combine_cut_counts(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[: max(0, limit - len(samples))])
    return _CutCounts(
        tasks=sum(item.tasks for item in items),
        succeeded=sum(item.succeeded for item in items),
        failed=sum(item.failed for item in items),
        outputs=sum(item.outputs for item in items),
        read_failed=sum(item.read_failed for item in items),
        no_data=sum(item.no_data for item in items),
        samples=tuple(samples),
    )


def _trimmed_trace(merged_tr, event, station):
    # trim to event time window
    trimed_tr = merged_tr.trim(event["start"], event["end"], nearest_sample=True)

    # header information
    data = trimed_tr.data
    npts = len(data)
    delta = trimed_tr.stats.delta
    if npts == 0:
        raise ValueError("Empty data after trim")
    start = trimed_tr.stats.starttime
    time_offset = start - event["start"]

    # update header
    if not trimed_tr.stats.location:
        trimed_tr.stats.location = "10"  # khole
    header_updates = {
        "delta": delta,
        "b": float(time_offset),
        "e": float(time_offset) + (npts - 1) * delta,
        "depmin": np.min(data),
        "depmax": np.max(data),
        "depmen": np.mean(data),
        "nzyear": start.year,
        "nzjday": start.julday,
        "nzhour": start.hour,
        "nzmin": start.minute,
        "nzsec": start.second,
        "nzmsec": start.microsecond // 1000,
        # necessary info of event
        "evla": event["latitude"],
        "evlo": event["longitude"],
        "evdp": event["depth"],
        "mag": event["mag"],
        "lcalda": 1,
        # optional info of station
        "stla": station.get("latitude", -12345),
        "stlo": station.get("longitude", -12345),
        "stel": station.get("elevation", -12345),
        "stdp": station.get("depth", -12345),
        # 参考时间 o 等
        # "o": 0.0,
    }

    trimed_tr.stats.sac.update(header_updates)
    return trimed_tr


def _calculate_julian_dates(start, end):
    """计算时间范围内包含的所有儒略日"""
    dates = set()
    current = start.datetime
    end = end.datetime

    while current <= end:
        year = current.year
        jday = current.timetuple().tm_yday
        dates.add((year, f"{jday:03d}"))
        current += datetime.timedelta(days=1)

    return sorted(dates)


def _target_paths(sac_base, station, year_jdays):
    """构建预测的SAC文件路径"""
    valid_paths = []
    for year, jday in year_jdays:
        dir_path = Path(sac_base) / station / str(year) / jday
        if not dir_path.exists():
            continue

        # 预期文件名模式：*.{year}.{jday}.*.sac
        pattern = f"*.{year}.{jday}.*.sac"
        valid_paths.extend(dir_path.glob(pattern))

    return valid_paths
