"""Cut event windows from a continuous waveform archive."""

import logging
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from os import cpu_count
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

from seispy.waveform.integrity import merge_contiguous_segments
from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.catalog import load_events, load_stations

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CutEventIssue:
    """A sampled event/station processing issue."""

    event: str
    network: str
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
    source_dir: str | Path,
    event_csv: str | Path,
    *,
    output_dir: str | Path | None = None,
    station_csv: str | Path | None = None,
    time_window: float = 10800,
    pattern: str = "*.sac",
    max_workers: int | None = None,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CutEventSummary:
    """Cut event windows from continuous SAC data.

    Args:
        source_dir: Any directory tree containing SAC waveforms. Directory names
            do not need to encode network, station, or date metadata.
        event_csv: Event table consumed by :func:`load_events`.
        output_dir: Destination root. Defaults to a sibling named
            ``<source_dir.name>_events`` and must not be inside ``source_dir``.
        station_csv: Optional station table keyed by network and station.
        time_window: Window length after each event origin, in seconds.
        pattern: Recursive source filename pattern.
        max_workers: Station workers. Defaults to the smaller of 5 and the CPU
            count. Each worker reuses waveform reads across chronological events.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains it only when issues occur.
        save_log: Write a persistent run log. Defaults to ``True``.

    Returns:
        Task counts, written output count, sampled issues, and run duration.

    Raises:
        ValueError: If paths, concurrency, error sampling, or metadata are invalid.

    Examples:
        ```python
        summary = cut_event_waveforms(
            "data/continuous", "events.csv", output_dir="data/events",
            station_csv="stations.csv", time_window=10_800,
        )
        summary.total >= summary.succeeded
        # => True
        ```
    """
    run_id = new_run_id()
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if time_window <= 0:
        raise ValueError("time_window must be positive")
    workers = min(5, cpu_count() or 1) if max_workers is None else max_workers
    if workers < 1:
        raise ValueError("max_workers must be at least 1")

    source = Path(source_dir).expanduser().resolve()
    if not source.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {source}")
    artifact_root = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else source.with_name(f"{source.name}_events")
    )
    if artifact_root == source or source in artifact_root.parents:
        raise ValueError("output_dir must not be source_dir or one of its descendants")

    events = load_events(event_csv, time_window)
    events.sort(key=lambda item: item["start"])
    archive_index = WaveformArchiveIndex.build(source, pattern=pattern)
    stations = load_stations(set(archive_index.station_keys), station_csv)
    workers = min(workers, max(1, len(stations)))
    total = len(events) * len(stations)
    tasks_done = succeeded = failed = outputs = no_data = 0
    read_failed = len(archive_index.issues)
    samples = [
        CutEventIssue(
            "",
            "",
            "",
            "archive_index_failed",
            issue.error,
            issue.path,
        )
        for issue in archive_index.issues[:max_error_samples]
    ]
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
            "run_id=%s source=%s output=%s stations=%d events=%d time_window=%s workers=%d",
            run_id,
            source,
            artifact_root,
            len(stations),
            len(events),
            time_window,
            workers,
        )
        with tqdm(total=total, desc="Processing...") as pbar:
            station_tasks = (
                (
                    station,
                    archive_index.records_for_station(
                        str(station["network"]), str(station["station"])
                    ),
                )
                for station in stations
            )
            for result in _station_results(
                station_tasks,
                events,
                artifact_root,
                max_error_samples,
                workers,
            ):
                tasks_done += result.tasks
                succeeded += result.succeeded
                failed += result.failed
                outputs += result.outputs
                read_failed += result.read_failed
                no_data += result.no_data
                samples.extend(
                    result.samples[: max(0, max_error_samples - len(samples))]
                )
                pbar.update(result.tasks)
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
                "run_id=%s status=%s event=%s network=%s station=%s source=%s error=%s",
                run_id,
                item.status,
                item.event,
                item.network,
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


_WORKER_EVENTS = ()
_WORKER_OUTPUT_DIR = None
_WORKER_MAX_ERROR_SAMPLES = 0


def _station_results(tasks, events, output_dir, max_error_samples, max_workers):
    if max_workers == 1:
        _initialize_cut_worker(events, output_dir, max_error_samples)
        for task in tasks:
            yield _cut_station_events(*task)
        return

    with ProcessPoolExecutor(
        max_workers=max_workers,
        initializer=_initialize_cut_worker,
        initargs=(events, output_dir, max_error_samples),
    ) as executor:
        pending = set()
        for task in tasks:
            pending.add(executor.submit(_cut_station_events, *task))
            if len(pending) < max_workers * 2:
                continue
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()
        while pending:
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()


def _initialize_cut_worker(events, output_dir, max_error_samples):
    global _WORKER_EVENTS, _WORKER_OUTPUT_DIR, _WORKER_MAX_ERROR_SAMPLES
    _WORKER_EVENTS = events
    _WORKER_OUTPUT_DIR = output_dir
    _WORKER_MAX_ERROR_SAMPLES = max_error_samples


def _cut_station_events(station, records):
    index = WaveformArchiveIndex.from_records(records)
    reader = WaveformReader()
    results = []
    for event in _WORKER_EVENTS:
        remaining = max(
            0,
            _WORKER_MAX_ERROR_SAMPLES - sum(len(result.samples) for result in results),
        )
        results.append(
            cut_event_station(
                event,
                station,
                _WORKER_OUTPUT_DIR,
                archive_index=index,
                waveform_reader=reader,
                max_error_samples=remaining,
            )
        )
    return _combine_cut_counts(results, _WORKER_MAX_ERROR_SAMPLES)


def cut_event_station(
    event,
    station,
    dest_dir,
    *,
    archive_index,
    waveform_reader=None,
    max_error_samples=1,
) -> _CutCounts:
    """Cut one event-station window using a reusable header index."""
    network = str(station["network"])
    station_name = str(station["station"])
    event_name = _event_name(event["start"])
    records = archive_index.overlapping(
        network, station_name, event["start"], event["end"]
    )
    if not records:
        samples = ()
        if max_error_samples:
            samples = (
                CutEventIssue(
                    event_name,
                    network,
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
            if identity != record.identity:
                raise ValueError("SAC identity changed after archive indexing")
            key = (
                identity.network,
                identity.station,
                identity.location,
                identity.channel,
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
                        network,
                        station_name,
                        "input_read_failed",
                        f"{type(exc).__name__}: {exc}",
                        record.path,
                    )
                )

    outputs = 0
    if waveform_data:
        event_dir = Path(dest_dir) / event_name / network / station_name
        event_dir.mkdir(parents=True, exist_ok=True)
        for identity_key, stream in waveform_data.items():
            try:
                merged = merge_contiguous_segments(stream)
                segments = [
                    trace
                    for trace in merged
                    if trace.stats.starttime <= event["end"]
                    and trace.stats.endtime >= event["start"]
                ]
                for index, segment in enumerate(segments):
                    temporary = None
                    try:
                        trimmed_trace = _trimmed_trace(segment, event, station)
                        out_name = _event_output_name(
                            identity_key,
                            trimmed_trace.stats.starttime,
                            index if len(segments) > 1 else None,
                        )
                        destination = event_dir / out_name
                        if destination.exists():
                            raise FileExistsError(destination)
                        temporary = temporary_output_path(destination)
                        trimmed_trace.write(str(temporary), format="SAC")
                        commit_output(temporary, destination)
                        temporary = None
                        outputs += 1
                    finally:
                        if temporary is not None:
                            temporary.unlink(missing_ok=True)
            except Exception as exc:
                had_issue = True
                if len(samples) < max_error_samples:
                    samples.append(
                        CutEventIssue(
                            event_name,
                            network,
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
                    network,
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


def _event_name(starttime):
    return starttime.strftime("%Y%m%dT%H%M%S%f")[:-3]


def _event_output_name(identity_key, starttime, segment_index):
    network, station, location, channel = identity_key
    location = location or "--"
    identity = f"{network}.{station}.{location}.{channel}"
    if segment_index is None:
        return f"{identity}.sac"
    timestamp = starttime.strftime("%Y%m%dT%H%M%S%f")[:-3]
    return f"{identity}.T{timestamp}.S{segment_index + 1:03d}.sac"


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
    }
    optional_station_headers = {
        "stla": "latitude",
        "stlo": "longitude",
        "stel": "elevation",
        "stdp": "depth",
    }
    for sac_name, metadata_name in optional_station_headers.items():
        value = station.get(metadata_name)
        if value is not None and value == value:
            header_updates[sac_name] = value

    trimed_tr.stats.sac.update(header_updates)
    return trimed_tr
