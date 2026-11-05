"""Cut event windows from a continuous waveform archive."""

from collections import defaultdict
from functools import partial
from io import BytesIO
import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, replace
from os import cpu_count
from pathlib import Path
from queue import Empty, Full
from time import monotonic

import numpy as np
import obspy
from obspy.core.compatibility import round_away
from obspy.io.sac import SACTrace
from obspy.io.sac.util import get_sac_reftime, utcdatetime_to_sac_nztimes
from seispy._sac_geometry import sac_geometry
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    temporary_output_path,
)
from seispy._main_guard import require_reimport_safe_entry_point
from seispy.progress import call_with_warnings, progress_bar, resolve_worker_call

from seispy.waveform.integrity import (
    TraceFilter,
    merge_contiguous_segments,
    split_traces_by_filter,
)
from seispy.event.archive_index import WaveformArchiveIndex, WaveformReader
from seispy.event.catalog import _integer_second_window, load_events, load_stations

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
class CutEventCoverage:
    """Sample-grid coverage of one event/channel, without double-counting overlaps."""

    event: str
    network: str
    station: str
    location: str
    channel: str
    coverage_fraction: float
    covered_seconds: float
    requested_seconds: float
    segments: int
    has_gaps: bool
    has_overlaps: bool


@dataclass(frozen=True)
class _CutCounts:
    tasks: int = 0
    succeeded: int = 0
    failed: int = 0
    outputs: int = 0
    read_failed: int = 0
    no_data: int = 0
    samples: tuple[CutEventIssue, ...] = ()
    channels: int = 0
    partial_channels: int = 0
    gapped_channels: int = 0
    overlapping_channels: int = 0
    existing_outputs: int = 0
    existing_skipped: int = 0
    traces_filtered: int = 0
    skipped: int = 0
    coverage_samples: tuple[CutEventCoverage, ...] = ()


# Explicit additive fields; samples are bounded separately, never summed.
_CUT_COUNTERS = (
    "tasks",
    "succeeded",
    "failed",
    "outputs",
    "read_failed",
    "no_data",
    "channels",
    "partial_channels",
    "gapped_channels",
    "overlapping_channels",
    "existing_outputs",
    "existing_skipped",
    "traces_filtered",
    "skipped",
)
_REPORT_NAMES = {
    "tasks": "total",
    "outputs": "outputs_written",
    "read_failed": "input_read_failed",
}


class _CutAccumulator:
    """Accumulate counts and independently bounded issue/coverage samples."""

    def __init__(self, limit, coverage_limit=None):
        self.values = dict.fromkeys(_CUT_COUNTERS, 0)
        self.samples = []
        self.coverage_samples = []
        self.limit = limit
        self.coverage_limit = limit if coverage_limit is None else coverage_limit

    @property
    def tasks(self):
        return self.values["tasks"]

    def add(self, counts):
        for name in _CUT_COUNTERS:
            self.values[name] += getattr(counts, name)
        self.samples.extend(counts.samples[: max(0, self.limit - len(self.samples))])
        self.coverage_samples.extend(
            counts.coverage_samples[
                : max(0, self.coverage_limit - len(self.coverage_samples))
            ]
        )

    def counts(self):
        return _CutCounts(
            **self.values,
            samples=tuple(self.samples),
            coverage_samples=tuple(self.coverage_samples),
        )

    def report_fields(self):
        return {
            **{
                _REPORT_NAMES.get(name, name): value
                for name, value in self.values.items()
            },
            "issue_samples": tuple(self.samples),
            "coverage_samples": tuple(self.coverage_samples),
        }


class _CutIssues:
    """Record task failures even after the issue-sample allowance is exhausted."""

    def __init__(self, event, station, limit):
        self.identity = (
            _event_name(event["origin"]),
            str(station["network"]),
            str(station["station"]),
        )
        self.limit = limit
        self.samples = []
        self.failed = False

    def add(self, status, error, source=None):
        self.failed = True
        if len(self.samples) < self.limit:
            self.samples.append(CutEventIssue(*self.identity, status, error, source))

    def exception(self, status, error, source=None):
        self.add(status, f"{type(error).__name__}: {error}", source)


@dataclass(frozen=True)
class CutEventSummary(BatchSummary):
    """Summarize an event-by-station waveform cutting run.

    This class is returned by :func:`cut_event_waveforms`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of event-station combinations considered.
        succeeded: Number that wrote output without read/write failures.
        failed: Number with actual processing failures; excludes missing coverage.
        outputs_written: Number of event waveform files written.
        input_read_failed: Number of source waveform read failures.
        no_data: Tasks with no usable coverage, included in skipped.
        issue_samples: Bounded sample of cutting issues, excluding routine no_data.
        channels: Number of event/channel windows with usable trimmed samples.
        partial_channels: Windows with less than complete sample-grid coverage.
        gapped_channels: Windows with internal gaps between segments.
        overlapping_channels: Windows with unresolved overlapping segments.
        existing_outputs: Existing files preserved, including conflicting files.
        traces_filtered: Cut segments rejected by the optional trace filter.
        existing_skipped: Tasks with all expected outputs already matching (included in skipped).
        skipped: Tasks with no coverage, only filtered segments, or complete outputs.
        coverage_samples: Bounded details of partial or segmented windows.
        duration_seconds: Total elapsed wall-clock time.
        src_path: Resolved absolute source directory for this run.
        output_path: Resolved absolute output directory for this run.
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
    channels: int = 0
    partial_channels: int = 0
    gapped_channels: int = 0
    overlapping_channels: int = 0
    existing_outputs: int = 0
    existing_skipped: int = 0
    traces_filtered: int = 0
    skipped: int = 0
    coverage_samples: tuple[CutEventCoverage, ...] = ()

    @property
    def has_issues(self) -> bool:
        return bool(
            self.failed
            or self.input_read_failed
            or self.no_data
            or self.partial_channels
            or self.overlapping_channels
        )


def cut_event_waveforms(
    source_dir: str | Path,
    event_csv: str | Path,
    *,
    output_dir: str | Path | None = None,
    station_csv: str | Path | None = None,
    time_window: float = 10800,
    pattern: str = "*.sac",
    trace_filter: TraceFilter | None = None,
    max_workers: int | None = None,
    max_error_samples: int = 20,
    cache_max_bytes: int = 256 * 1024**2,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CutEventSummary:
    """Cut SAC event windows from continuous SAC or MiniSEED data.

    Args:
        source_dir: Any directory tree containing SAC or MiniSEED waveforms.
            Directory names do not need to encode network, station, or date metadata.
        event_csv: Event table consumed by :func:`load_events`. Events must
            occupy distinct UTC seconds to avoid output path collisions.
        output_dir: Destination root. SAC files are placed directly in
            ``YYYYmmddHHMMSS`` event directories. Defaults to a sibling named
            ``<source_dir.name>_events`` and must not be inside ``source_dir``.
        trace_filter: Optional policy applied to each continuous cut segment
            before writing. None preserves all segments. Filtered segments are
            counted separately; a fully filtered task is skipped, not failed.
        station_csv: Optional station table keyed by network and station.
        time_window: Window length in seconds, beginning at the event origin
            floored to an integer UTC second. The precise origin remains in
            the output event identity and SAC ``o`` header.
        pattern: Recursive source filename pattern. Defaults to ``"*.sac"``;
            use ``"*.mseed"`` for MiniSEED archives. Output is always SAC.
        max_workers: Station workers. Defaults to the smaller of 5 and the CPU
            count. Each worker reuses waveform reads across chronological events.
        max_error_samples: Maximum number of issues retained in the summary.
            Also bounds sampled channel-coverage details independently.
        cache_max_bytes: Maximum retained waveform-array bytes per worker.
            Defaults to 256 MiB. Zero disables retention. This is not a peak
            memory limit; reads, overlap comparisons, and output need extra RAM.
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
    require_reimport_safe_entry_point("cut_event_waveforms")
    run_id = new_run_id()
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if cache_max_bytes < 0:
        raise ValueError("cache_max_bytes cannot be negative")
    if not np.isfinite(time_window) or time_window <= 0:
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
    event_names = [_event_name(item["origin"]) for item in events]
    if len(event_names) != len(set(event_names)):
        raise ValueError(
            "event_csv contains multiple events in the same UTC second; "
            "second-resolution output paths would collide"
        )
    events.sort(key=lambda item: item["start"])
    archive_index = WaveformArchiveIndex.build(source, pattern=pattern)
    stations = load_stations(set(archive_index.station_keys), station_csv)
    workers = min(workers, max(1, len(stations)))
    total = len(events) * len(stations)
    totals = _CutAccumulator(max_error_samples)
    totals.add(
        _CutCounts(
            read_failed=len(archive_index.issues),
            samples=tuple(
                CutEventIssue(
                    "", "", "", "archive_index_failed", issue.error, issue.path
                )
                for issue in archive_index.issues[:max_error_samples]
            ),
        )
    )
    with BatchRun(
        "cut-events",
        artifact_root,
        src_path=source,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            **(totals.report_fields() | {"total": total, "tasks_completed": 0}),
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
        with progress_bar(total=total, desc="Cutting events", unit="task") as pbar:
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
                cache_max_bytes,
                trace_filter=trace_filter,
            ):
                totals.add(result)
                pbar.update(result.tasks)
                run.checkpoint(
                    completed=totals.tasks,
                    **(
                        totals.report_fields()
                        | {
                            "total": total,
                            "tasks_completed": totals.tasks,
                        }
                    ),
                )
        summary = run.complete(
            CutEventSummary(
                run_id=run_id,
                duration_seconds=0,
                **totals.report_fields(),
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
        if summary.no_data:
            run.warning(
                "run_id=%s status=no_data tasks=%d (included in skipped; details omitted)",
                run_id,
                summary.no_data,
            )
        if summary.partial_channels:
            run.warning(
                "run_id=%s status=partial_coverage channels=%d "
                "(requested windows not fully covered; see coverage_samples)",
                run_id,
                summary.partial_channels,
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
        f"{summary.failed} failed, {summary.skipped} skipped ({summary.no_data} no data), "
        f"{summary.traces_filtered} segments filtered, "
        f"{summary.outputs_written} outputs written."
    )
    return summary


_WORKER_EVENTS = ()
_WORKER_OUTPUT_DIR = None
_WORKER_MAX_ERROR_SAMPLES = 0
_WORKER_CACHE_BYTES = 256 * 1024**2
_WORKER_PROGRESS = None
_WORKER_STOP = None
_WORKER_TRACE_FILTER = None


def _station_results(
    tasks,
    events,
    output_dir,
    max_error_samples,
    max_workers,
    cache_max_bytes=256 * 1024**2,
    trace_filter=None,
):
    if max_workers == 1:
        for station, records in tasks:
            yield from _iter_station_counts(
                station,
                records,
                events,
                output_dir,
                max_error_samples,
                cache_max_bytes,
                trace_filter,
            )
        return

    context = multiprocessing.get_context()
    progress = context.Queue(maxsize=max_workers * 2)
    stop = context.Event()
    try:
        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_initialize_cut_worker,
            initargs=(
                events,
                output_dir,
                max_error_samples,
                cache_max_bytes,
                progress,
                stop,
                trace_filter,
            ),
        ) as executor:
            pending = set()
            tasks = iter(tasks)
            exhausted = False
            received = expected = 0
            try:
                # Each future returns its number of queue messages. Keep
                # draining after it finishes: Queue.put uses an async feeder.
                while pending or not exhausted or received < expected:
                    while not exhausted and len(pending) < max_workers * 2:
                        try:
                            task = next(tasks)
                        except StopIteration:
                            exhausted = True
                            break
                        pending.add(
                            executor.submit(
                                call_with_warnings, _cut_station_events, *task
                            )
                        )
                    if not pending and exhausted and received >= expected:
                        break
                    try:
                        result = progress.get(timeout=0.1)
                    except Empty:
                        pass
                    else:
                        received += 1
                        yield result
                    done = {future for future in pending if future.done()}
                    for future in done:
                        expected += resolve_worker_call(future.result())
                    pending -= done
            finally:
                stop.set()
                for future in pending:
                    future.cancel()
    finally:
        progress.close()
        progress.join_thread()


def _initialize_cut_worker(
    events,
    output_dir,
    max_error_samples,
    cache_max_bytes,
    progress,
    stop,
    trace_filter=None,
):
    global _WORKER_EVENTS, _WORKER_OUTPUT_DIR, _WORKER_MAX_ERROR_SAMPLES
    global _WORKER_CACHE_BYTES, _WORKER_PROGRESS, _WORKER_STOP, _WORKER_TRACE_FILTER
    _WORKER_TRACE_FILTER = trace_filter
    _WORKER_EVENTS = events
    _WORKER_OUTPUT_DIR = output_dir
    _WORKER_MAX_ERROR_SAMPLES = max_error_samples
    _WORKER_CACHE_BYTES = cache_max_bytes
    _WORKER_PROGRESS = progress
    # The parent waits for the advertised message count on normal completion.
    # On cancellation it may stop consuming; workers must not hang joining a
    # feeder whose pipe is full while the executor shuts down.
    _WORKER_PROGRESS.cancel_join_thread()
    _WORKER_STOP = stop


def _cut_station_events(station, records):
    sent = 0
    for counts in _iter_station_counts(
        station,
        records,
        _WORKER_EVENTS,
        _WORKER_OUTPUT_DIR,
        _WORKER_MAX_ERROR_SAMPLES,
        _WORKER_CACHE_BYTES,
        _WORKER_TRACE_FILTER,
    ):
        while not _WORKER_STOP.is_set():
            try:
                _WORKER_PROGRESS.put(counts, timeout=0.1)
                sent += 1
                break
            except Full:
                continue
        else:
            return sent
    return sent


def _iter_station_counts(
    station, records, events, output_dir, limit, cache_bytes, trace_filter=None
):
    index = WaveformArchiveIndex.from_records(records)
    reader = WaveformReader(max_bytes=cache_bytes)
    batch = []
    issues_used = coverage_used = 0
    last_report = monotonic()
    for event in events:
        result = cut_event_station(
            event,
            station,
            output_dir,
            archive_index=index,
            waveform_reader=reader,
            max_error_samples=max(0, limit - issues_used),
            max_coverage_samples=max(0, limit - coverage_used),
            trace_filter=trace_filter,
        )
        issues_used += len(result.samples)
        coverage_used += len(result.coverage_samples)
        batch.append(result)
        if len(batch) >= 16 or monotonic() - last_report >= 1.0:
            yield _combine_cut_counts(batch, limit)
            batch.clear()
            last_report = monotonic()
    if batch:
        yield _combine_cut_counts(batch, limit)


def cut_event_station(
    event,
    station,
    dest_dir,
    *,
    archive_index,
    waveform_reader=None,
    max_error_samples=1,
    max_coverage_samples=None,
    trace_filter: TraceFilter | None = None,
) -> _CutCounts:
    """Cut one event-station window using a reusable header index."""
    event = _integer_second_window(event)
    issues = _CutIssues(event, station, max_error_samples)
    event_name, network, station_name = issues.identity
    records = archive_index.overlapping(
        network, station_name, event["start"], event["end"]
    )
    if not records:
        return _CutCounts(tasks=1, skipped=1, no_data=1)

    totals = _CutAccumulator(max_error_samples, max_coverage_samples)
    waveform_data = defaultdict(obspy.Stream)
    reader = waveform_reader or WaveformReader(max_bytes=0)
    full_reads = _overlapping_identities(records)
    for record, stream, error in reader.read_many(
        records,
        starttime=event["start"],
        endtime=event["end"],
        padding_samples=2,
        full_records=(
            record.key for record in records if _nslc(record.identity) in full_reads
        ),
    ):
        if error is not None:
            totals.add(_CutCounts(read_failed=1))
            issues.exception("input_read_failed", error, record.path)
            continue
        waveform_data[_nslc(record.identity)] += stream

    time_headers = utcdatetime_to_sac_nztimes(event["start"])[0]
    annotate = partial(
        _set_event_headers,
        event=event,
        station=station,
        nztimes=time_headers,
        reference=get_sac_reftime(time_headers),
        geometry_cache={},
    )
    event_dir = Path(dest_dir) / event_name
    for identity, stream in waveform_data.items():
        totals.add(
            _cut_channel(
                stream,
                identity,
                event,
                event_dir,
                annotate,
                issues,
                max(0, totals.coverage_limit - len(totals.coverage_samples)),
                trace_filter,
            )
        )
    return _finish_cut_task(totals.counts(), issues)


def _cut_channel(
    stream, identity, event, event_dir, annotate, issues, coverage_limit, trace_filter
):
    """Cut one channel, retaining partial counts if a later output fails."""
    totals = _CutAccumulator(0, coverage_limit)
    try:
        merged = merge_contiguous_segments(stream)
        trimmed = [
            trace.trim(event["start"], event["end"], nearest_sample=True)
            for trace in merged
            if trace.stats.starttime <= event["end"] + trace.stats.delta / 2
            and trace.stats.endtime >= event["start"] - trace.stats.delta / 2
        ]
        trimmed = [trace for trace in trimmed if trace.stats.npts > 0]
        if trace_filter is not None:
            trimmed, rejected = split_traces_by_filter(trimmed, trace_filter)
            totals.add(_CutCounts(traces_filtered=len(rejected)))
        if not trimmed:
            return totals.counts()
        for trace in trimmed:
            annotate(trace)
        event_dir.mkdir(parents=True, exist_ok=True)
        coverage = _channel_coverage(trimmed, identity, event)
        sampled = (
            coverage.coverage_fraction < 1
            or coverage.has_overlaps
            or coverage.segments > 1
        )
        totals.add(
            _CutCounts(
                channels=1,
                partial_channels=int(coverage.coverage_fraction < 1),
                gapped_channels=int(coverage.has_gaps),
                overlapping_channels=int(coverage.has_overlaps),
                coverage_samples=(coverage,) if sampled and coverage_limit else (),
            )
        )
        for index, trace in enumerate(trimmed):
            name = _event_output_name(
                identity,
                event["origin"],
                index if len(trimmed) > 1 else None,
            )
            totals.add(_write_or_validate_event_output(trace, event_dir / name, issues))
    except Exception as exc:
        issues.exception("output_failed", exc)
    return totals.counts()


def _write_or_validate_event_output(trace, destination, issues):
    """Preserve existing files or atomically commit one new SAC output."""
    if destination.exists():
        if not _existing_event_output_matches(destination, trace):
            issues.add(
                "output_conflict",
                f"Existing output differs from expected SAC: {destination}",
            )
        return _CutCounts(existing_outputs=1)
    temporary = temporary_output_path(destination)
    try:
        _write_event_sac(trace, str(temporary))
        commit_output(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    return _CutCounts(outputs=1)


def _finish_cut_task(counts, issues):
    """Choose a task status once, after all channel results are known."""
    if issues.failed:
        if not counts.outputs:
            issues.add("no_output", "no event channel was written")
        status = {"failed": 1}
    elif counts.outputs:
        status = {"succeeded": 1}
    elif counts.existing_outputs:
        status = {"skipped": 1, "existing_skipped": 1}
    elif counts.traces_filtered and not counts.channels:
        status = {"skipped": 1}
    elif not counts.channels:
        status = {"skipped": 1, "no_data": 1}
    else:
        issues.add("no_output", "no event channel was written")
        status = {"failed": 1}
    return replace(counts, tasks=1, samples=tuple(issues.samples), **status)


def _nslc(identity):
    return identity.network, identity.station, identity.location, identity.channel


def _overlapping_identities(records):
    """Keep full-array comparisons for identities with overlapping source files."""
    ends = {}
    overlaps = set()
    for record in sorted(records, key=lambda item: item.starttime):
        key = _nslc(record.identity)
        previous_end = ends.get(key)
        if previous_end is not None and record.starttime <= previous_end:
            overlaps.add(key)
        ends[key] = (
            max(previous_end, record.endtime) if previous_end else record.endtime
        )
    return overlaps


def _channel_coverage(traces, identity, event):
    """Measure union coverage on the same inclusive sample grid as trim()."""
    if not traces:
        raise ValueError("No segments after trim")
    anchor = traces[0].stats.starttime
    delta = traces[0].stats.delta
    # Tie toward the later sample, including when the desired start precedes
    # this segment. Rounding a negative half away from zero falsely labels a
    # complete window as missing its first sample.
    lower = int(np.floor(round((event["start"] - anchor) / delta, 7) + 0.5))
    upper = int(np.floor(round((event["end"] - anchor) / delta, 7) + 0.5)) + 1
    intervals = []
    for trace in traces:
        first = int(round_away((trace.stats.starttime - anchor) / delta))
        last = first + trace.stats.npts
        if min(last, upper) > max(first, lower):
            intervals.append((max(first, lower), min(last, upper)))
    covered = 0
    end = None
    gaps = overlaps = False
    for first, last in sorted(intervals):
        if end is None:
            covered += last - first
        elif first > end:
            gaps = True
            covered += last - first
        else:
            overlaps |= first < end
            covered += max(0, last - end)
        end = last if end is None else max(end, last)
    requested = upper - lower
    return CutEventCoverage(
        _event_name(event.get("origin", event["start"])),
        *identity,
        covered / requested,
        covered * delta,
        requested * delta,
        len(traces),
        gaps,
        overlaps,
    )


def _event_name(starttime):
    return starttime.strftime("%Y%m%d%H%M%S")


def _event_output_name(identity_key, event_time, segment_index):
    network, station, location, channel = identity_key
    location = location or "--"
    timestamp = event_time.strftime("%Y%m%d%H%M%S")
    identity = f"{network}.{station}.{location}.{channel}.{timestamp}"
    if segment_index is None:
        return f"{identity}.sac"
    return f"{identity}.s{segment_index + 1:02d}.sac"


def _write_event_sac(trace, destination):
    """Use the fixed SAC writer without repeated waveform-plugin dispatch."""
    if np.ma.isMaskedArray(trace.data):
        raise NotImplementedError("Cannot write masked samples to SAC")
    SACTrace.from_obspy_trace(trace, keep_sac_header=True).write(
        destination, ascii=False, byteorder="little"
    )


def _existing_event_output_matches(destination, expected):
    """Compare full SAC headers and samples against the rebuilt expected cut."""
    try:
        # Binary SAC output has a 632-byte header and four bytes per sample.
        expected_size = 632 + 4 * expected.stats.npts
        if destination.stat().st_size != expected_size:
            return False
        with BytesIO() as buffer:
            _write_event_sac(expected, buffer)
            buffer.seek(0)
            with destination.open("rb") as existing:
                while chunk := buffer.read(1024 * 1024):
                    if existing.read(len(chunk)) != chunk:
                        return False
                return not existing.read(1)
    except Exception:
        return False


def _combine_cut_counts(items, limit):
    totals = _CutAccumulator(limit)
    for item in items:
        totals.add(item)
    return totals.counts()


def _set_event_headers(trimed_tr, event, station, nztimes, reference, geometry_cache):
    """Annotate an accepted window, sharing event time and geometry work."""
    npts = trimed_tr.stats.npts
    delta = trimed_tr.stats.delta
    start = trimed_tr.stats.starttime
    # Use the whole-second cut start as reference, retain the precise origin
    # in o, and shift existing picks to preserve their absolute times.
    if "sac" not in trimed_tr.stats:
        trimed_tr.stats.sac = {}
    sac = trimed_tr.stats.sac
    old_reference = get_sac_reftime(sac) if sac else reference
    for name in ("a", "f", *(f"t{i}" for i in range(10))):
        value = sac.get(name)
        if value is not None and value != -12345.0:
            sac[name] = float(value) + (old_reference - reference)
    time_offset = start - reference

    header_updates = {
        "delta": delta,
        "b": float(time_offset),
        "e": float(time_offset) + (npts - 1) * delta,
        **nztimes,
        "o": float(event["origin"] - reference),
        # IO (11) is valid only when the origin equals the whole-second reference.
        "iztype": 11 if event["origin"] == reference else 5,
        # necessary info of event
        "evla": event["latitude"],
        "evlo": event["longitude"],
        "evdp": event["depth"],
        "mag": event["mag"],
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

    sac.update(header_updates)
    coords = tuple(sac.get(name) for name in ("evla", "evlo", "stla", "stlo"))
    if coords not in geometry_cache:
        geometry_cache[coords] = sac_geometry(*coords)
    sac.update(geometry_cache[coords])
