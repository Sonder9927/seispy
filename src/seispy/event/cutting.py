"""Cut event windows from a continuous waveform archive."""

import logging
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import obspy
from seispy.archive import WaveformIdentity
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    temporary_output_path,
)
from tqdm import tqdm

from seispy.waveform.integrity import merge_short_gaps
from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.catalog import load_events, load_stations

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CutEventIssue:
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
    samples: tuple[CutEventIssue, ...] = ()


@dataclass(frozen=True)
class CutEventSummary(BatchSummary):
    """Summarize an event-by-station waveform cutting run.

    This class is returned by :func:`cut_event_waveforms`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of event-station combinations considered.
        succeeded: Number that produced at least one output.
        failed: Number that did not complete successfully.
        outputs_written: Number of event waveform files written.
        input_read_failed: Number of source waveform read failures.
        no_data: Number of tasks with no usable waveform data.
        issue_samples: Bounded sample of cutting issues.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        ```python
        summary = cut_event_waveforms(...)
        print(summary.outputs_written, summary.failed)
        ```
    """

    total: int
    succeeded: int
    failed: int
    outputs_written: int
    input_read_failed: int
    no_data: int
    issue_samples: tuple[CutEventIssue, ...]

    @property
    def has_issues(self) -> bool:
        return bool(self.failed or self.input_read_failed)


def cut_event_waveforms(
    src_dir: str | Path,
    dest_dir: str | Path,
    event_csv: str | Path,
    station_csv: str | Path | None = None,
    time_window: float = 10800,
    *,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CutEventSummary:
    """Cut event windows from continuous SAC data.

    Args:
        src_dir: One network directory containing station/year SAC archives.
        dest_dir: Destination root for event waveform files.
        event_csv: Event table consumed by :func:`load_events`.
        station_csv: Optional station table. Directory names are used if omitted.
        time_window: Window length after each event origin, in seconds.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains it only when issues occur.
        save_log: Write a persistent run log. Defaults to ``True``.

    Returns:
        Task counts, written output count, sampled issues, and run duration.

    Raises:
        ValueError: If error sampling or input metadata is invalid.

    Examples:
        ```python
        summary = cut_event_waveforms(
            "data/continuous", "data/events", "events.csv",
            station_csv="stations.csv", time_window=10_800,
        )
        summary.total >= summary.succeeded
        # => True
        ```
    """
    run_id = new_run_id()
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")

    events = load_events(event_csv, time_window)
    stations = load_stations(src_dir, station_csv)
    archive_index = WaveformArchiveIndex.build(
        src_dir, stations={item["station"] for item in stations}
    )
    waveform_reader = WaveformReader()
    events.sort(key=lambda item: item["start"])
    total = len(events) * len(stations)
    tasks_done = succeeded = failed = outputs = no_data = 0
    read_failed = len(archive_index.issues)
    samples = [
        CutEventIssue(
            "",
            issue.path.parent.parent.name,
            "archive_index_failed",
            issue.error,
            issue.path,
        )
        for issue in archive_index.issues[:max_error_samples]
    ]
    artifact_root = Path(dest_dir).expanduser().resolve()
    with BatchRun(
        "cut-events",
        artifact_root,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=total,
            tasks_completed=0,
            succeeded=0,
            failed=0,
            outputs_written=0,
            input_read_failed=read_failed,
            no_data=0,
            issue_samples=tuple(samples),
        )
        run.info(
            "run_id=%s src=%s dest=%s stations=%d events=%d time_window=%s",
            run_id,
            src_dir,
            dest_dir,
            len(stations),
            len(events),
            time_window,
        )
        with tqdm(total=total, desc="Processing...") as pbar:
            for station in stations:
                for event in events:
                    result = cut_event_station(
                        event,
                        station,
                        src_dir,
                        dest_dir,
                        archive_index=archive_index,
                        waveform_reader=waveform_reader,
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
                    run.checkpoint(
                        completed=tasks_done,
                        total=total,
                        tasks_completed=tasks_done,
                        succeeded=succeeded,
                        failed=failed,
                        outputs_written=outputs,
                        input_read_failed=read_failed,
                        no_data=no_data,
                        issue_samples=tuple(samples),
                    )
                    pbar.update(1)
        summary = run.complete(
            CutEventSummary(
                run_id=run_id,
                total=tasks_done,
                succeeded=succeeded,
                failed=failed,
                outputs_written=outputs,
                input_read_failed=read_failed,
                no_data=no_data,
                issue_samples=tuple(samples),
                duration_seconds=0,
            )
        )
        for item in summary.issue_samples:
            run.error(
                "run_id=%s status=%s event=%s station=%s source=%s error=%s",
                run_id,
                item.status,
                item.event,
                item.station,
                item.source,
                item.error,
            )
        issue_total = summary.failed + summary.input_read_failed
        if issue_total > len(summary.issue_samples):
            run.warning(
                "run_id=%s error_samples_truncated shown=%d total_issues=%d",
                run_id,
                len(summary.issue_samples),
                issue_total,
            )
    print(
        f"Cut events complete [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed, {summary.outputs_written} outputs written."
    )
    return summary


def cut_event_station(
    event,
    station,
    src_dir,
    dest_dir,
    *,
    archive_index=None,
    waveform_reader=None,
    max_error_samples=1,
) -> _CutCounts:
    """Cut one event-station window using a reusable header index."""
    station_name = station["station"]
    event_name = event["start"].strftime("%Y%m%d%H%M%S")
    index = archive_index or WaveformArchiveIndex.build(
        src_dir, stations={station_name}
    )
    records = index.overlapping(station_name, event["start"], event["end"])
    if not records:
        samples = ()
        if max_error_samples:
            samples = (
                CutEventIssue(
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
    waveform_data = {}
    for record in records:
        try:
            st = (
                waveform_reader.read(record)
                if waveform_reader is not None
                else obspy.read(record.path)
            )
            if len(st) != 1:
                raise ValueError("SAC file must contain exactly one trace")
            tr = st[0]
            identity = WaveformIdentity.from_trace(tr)
            if identity.day_key != record.identity.day_key:
                raise ValueError("SAC identity changed after archive indexing")
            key = (
                identity.network,
                identity.station,
                identity.location,
                identity.channel,
                identity.quality,
            )
            if key in waveform_data:
                waveform_data[key] += st
            else:
                waveform_data[key] = st
        except Exception as exc:
            read_failed += 1
            had_issue = True
            if len(samples) < max_error_samples:
                samples.append(
                    CutEventIssue(
                        event_name,
                        station_name,
                        "input_read_failed",
                        f"{type(exc).__name__}: {exc}",
                        record.path,
                    )
                )

    outputs = 0
    if waveform_data:
        event_dir = Path(dest_dir) / event_name
        event_dir.mkdir(parents=True, exist_ok=True)
        channel_counts = {}
        for key in waveform_data:
            channel_counts[key[3]] = channel_counts.get(key[3], 0) + 1
        for identity_key, stream in waveform_data.items():
            temporary = None
            try:
                merged_tr = merge_short_gaps(stream)[0]
                trimed_tr = _trimmed_trace(merged_tr, event, station)
                out_name = _event_output_name(
                    event_name, identity_key, channel_counts[identity_key[3]]
                )
                destination = event_dir / out_name
                if destination.exists():
                    raise FileExistsError(destination)
                temporary = temporary_output_path(destination)
                trimed_tr.write(str(temporary), format="SAC")
                commit_output(temporary, destination)
                temporary = None
                outputs += 1
            except Exception as exc:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
                had_issue = True
                if len(samples) < max_error_samples:
                    samples.append(
                        CutEventIssue(
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
                CutEventIssue(
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


def _event_output_name(event_name, identity_key, same_channel_count):
    network, station, location, channel, quality = identity_key
    if same_channel_count == 1:
        return f"{event_name}.{station}.{channel}.sac"
    location = location or "--"
    return f"{event_name}.{network}.{station}.{location}.{channel}.{quality}.sac"


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
