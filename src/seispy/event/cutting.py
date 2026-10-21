"""Cut event windows from a continuous waveform archive."""

import logging
import multiprocessing
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from os import cpu_count
from pathlib import Path
from queue import Empty, Full
from time import monotonic

import numpy as np
import obspy
from obspy.core.compatibility import round_away
from obspy.io.sac.util import get_sac_reftime, utcdatetime_to_sac_nztimes
from obspy.geodetics import gps2dist_azimuth, kilometer2degrees
from seispy.archive import WaveformIdentity
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    temporary_output_path,
)
from seispy._main_guard import require_reimport_safe_entry_point
from seispy.progress import call_with_warnings, progress_bar, resolve_worker_call

from seispy.waveform.integrity import merge_contiguous_segments
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
    coverage_samples: tuple[CutEventCoverage, ...] = ()


@dataclass(frozen=True)
class CutEventSummary(BatchSummary):
    """Summarize an event-by-station waveform cutting run.

    This class is returned by :func:`cut_event_waveforms`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of event-station combinations considered.
        succeeded: Number that wrote output without read/write failures.
        failed: Number that did not complete successfully.
        outputs_written: Number of event waveform files written.
        input_read_failed: Number of source waveform read failures.
        no_data: Number of tasks with no usable waveform data.
        issue_samples: Bounded sample of cutting issues.
        channels: Number of event/channel windows with usable trimmed samples.
        partial_channels: Windows with less than complete sample-grid coverage.
        gapped_channels: Windows with internal gaps between segments.
        overlapping_channels: Windows with unresolved overlapping segments.
        existing_outputs: Existing files preserved instead of overwritten.
        coverage_samples: Bounded details of partial or segmented windows.
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
    channels: int = 0
    partial_channels: int = 0
    gapped_channels: int = 0
    overlapping_channels: int = 0
    existing_outputs: int = 0
    coverage_samples: tuple[CutEventCoverage, ...] = ()

    @property
    def has_issues(self) -> bool:
        return bool(
            self.failed
            or self.input_read_failed
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
    tasks_done = succeeded = failed = outputs = no_data = 0
    quality_totals = dict(
        channels=0,
        partial_channels=0,
        gapped_channels=0,
        overlapping_channels=0,
        existing_outputs=0,
    )
    coverage_samples = []
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
            **quality_totals,
            coverage_samples=(),
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
            ):
                tasks_done += result.tasks
                succeeded += result.succeeded
                failed += result.failed
                outputs += result.outputs
                read_failed += result.read_failed
                no_data += result.no_data
                for name in quality_totals:
                    quality_totals[name] += getattr(result, name)
                coverage_samples.extend(
                    result.coverage_samples[
                        : max(0, max_error_samples - len(coverage_samples))
                    ]
                )
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
                    **quality_totals,
                    coverage_samples=tuple(coverage_samples),
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
                **quality_totals,
                coverage_samples=tuple(coverage_samples),
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
_WORKER_CACHE_BYTES = 256 * 1024**2
_WORKER_PROGRESS = None
_WORKER_STOP = None


def _station_results(
    tasks,
    events,
    output_dir,
    max_error_samples,
    max_workers,
    cache_max_bytes=256 * 1024**2,
):
    if max_workers == 1:
        for station, records in tasks:
            yield from _iter_station_counts(
                station, records, events, output_dir, max_error_samples, cache_max_bytes
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
    events, output_dir, max_error_samples, cache_max_bytes, progress, stop
):
    global _WORKER_EVENTS, _WORKER_OUTPUT_DIR, _WORKER_MAX_ERROR_SAMPLES
    global _WORKER_CACHE_BYTES, _WORKER_PROGRESS, _WORKER_STOP
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


def _iter_station_counts(station, records, events, output_dir, limit, cache_bytes):
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
) -> _CutCounts:
    """Cut one event-station window using a reusable header index."""
    event = _integer_second_window(event)
    network = str(station["network"])
    station_name = str(station["station"])
    event_name = _event_name(event["origin"])
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
                    "no matching waveform traces found",
                ),
            )
        return _CutCounts(tasks=1, failed=1, no_data=1, samples=samples)

    samples = []
    read_failed = 0
    had_issue = False
    waveform_data = {}
    reader = waveform_reader or WaveformReader(max_bytes=0)
    full_reads = _overlapping_identities(records)
    for record in records:
        try:
            if _nslc(record.identity) in full_reads:
                # A conflict outside the cut must not turn into an identical
                # overlap merely because the comparison was cropped early.
                st = reader.read(record)
            else:
                st = reader.read(
                    record,
                    starttime=event["start"],
                    endtime=event["end"],
                    padding_samples=2,
                )
            if len(st) != 1:
                raise ValueError(
                    "Indexed waveform record must select exactly one trace"
                )
            tr = st[0]
            identity = WaveformIdentity.from_trace(tr)
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

    outputs = existing_outputs = no_data = 0
    channels = partial_channels = gapped_channels = overlapping_channels = 0
    coverage_samples = []
    coverage_limit = (
        max_error_samples if max_coverage_samples is None else max_coverage_samples
    )
    if waveform_data:
        event_dir = Path(dest_dir) / event_name
        event_dir.mkdir(parents=True, exist_ok=True)
        for identity_key, stream in waveform_data.items():
            try:
                merged = merge_contiguous_segments(stream)
                segments = [
                    trace
                    for trace in merged
                    if trace.stats.starttime <= event["end"] + trace.stats.delta / 2
                    and trace.stats.endtime >= event["start"] - trace.stats.delta / 2
                ]
                trimmed = [
                    _trimmed_trace(segment, event, station) for segment in segments
                ]
                trimmed = [trace for trace in trimmed if trace is not None]
                if not trimmed:
                    continue
                coverage = _channel_coverage(trimmed, identity_key, event)
                channels += 1
                partial_channels += int(coverage.coverage_fraction < 1)
                gapped_channels += int(coverage.has_gaps)
                overlapping_channels += int(coverage.has_overlaps)
                if (
                    coverage.coverage_fraction < 1
                    or coverage.has_overlaps
                    or coverage.segments > 1
                ) and len(coverage_samples) < coverage_limit:
                    coverage_samples.append(coverage)
                for index, trimmed_trace in enumerate(trimmed):
                    temporary = None
                    try:
                        out_name = _event_output_name(
                            identity_key,
                            event["origin"],
                            index if len(trimmed) > 1 else None,
                        )
                        destination = event_dir / out_name
                        if destination.exists():
                            existing_outputs += 1
                            had_issue = True
                            if len(samples) < max_error_samples:
                                samples.append(
                                    CutEventIssue(
                                        event_name,
                                        network,
                                        station_name,
                                        "output_exists",
                                        str(destination),
                                    )
                                )
                            continue
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
        no_data = int(not had_issue and channels == 0)
        had_issue = True
        if len(samples) < max_error_samples:
            samples.append(
                CutEventIssue(
                    event_name,
                    network,
                    station_name,
                    "no_data" if no_data else "no_output",
                    "no event channel was written",
                )
            )
    return _CutCounts(
        tasks=1,
        succeeded=int(not had_issue),
        failed=int(had_issue),
        outputs=outputs,
        read_failed=read_failed,
        no_data=no_data,
        samples=tuple(samples),
        channels=channels,
        partial_channels=partial_channels,
        gapped_channels=gapped_channels,
        overlapping_channels=overlapping_channels,
        existing_outputs=existing_outputs,
        coverage_samples=tuple(coverage_samples),
    )


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
        channels=sum(item.channels for item in items),
        partial_channels=sum(item.partial_channels for item in items),
        gapped_channels=sum(item.gapped_channels for item in items),
        overlapping_channels=sum(item.overlapping_channels for item in items),
        existing_outputs=sum(item.existing_outputs for item in items),
        coverage_samples=tuple(
            sample for item in items for sample in item.coverage_samples
        )[:limit],
    )


def _trimmed_trace(merged_tr, event, station):
    event = _integer_second_window(event)
    # trim to event time window
    trimed_tr = merged_tr.trim(event["start"], event["end"], nearest_sample=True)

    # header information
    data = trimed_tr.data
    npts = len(data)
    delta = trimed_tr.stats.delta
    if npts == 0:
        # The index deliberately uses a conservative half-sample margin.
        # An exact tie can select no sample from this segment; other segments
        # of the same channel must still be processed.
        return None
    start = trimed_tr.stats.starttime
    # Use the whole-second cut start as reference, retain the precise origin
    # in o, and shift existing picks to preserve their absolute times.
    nztimes, _ = utcdatetime_to_sac_nztimes(event["start"])
    reference = get_sac_reftime(nztimes)
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
        "depmin": np.min(data),
        "depmax": np.max(data),
        "depmen": np.mean(data),
        **nztimes,
        "o": float(event["origin"] - reference),
        # IO (11) is valid only when the origin equals the whole-second reference.
        "iztype": 11 if event["origin"] == reference else 5,
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
    # Setting lcalda alone does not refresh existing distance fields when an
    # ObsPy Trace is serialized. Never carry geometry from another event.
    for name in ("dist", "az", "baz", "gcarc"):
        sac.pop(name, None)
    coordinates = [sac.get(name) for name in ("evla", "evlo", "stla", "stlo")]
    if all(
        value is not None and value != -12345.0 and np.isfinite(value)
        for value in coordinates
    ):
        distance, azimuth, back_azimuth = gps2dist_azimuth(*coordinates)
        sac.update(
            dict(
                dist=distance / 1000,
                az=azimuth,
                baz=back_azimuth,
                gcarc=kilometer2degrees(distance / 1000),
            )
        )
    return trimed_tr
