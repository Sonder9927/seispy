"""Validate and organize waveform files into trusted canonical archives."""

import shutil
import warnings
from collections import defaultdict
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from obspy import Stream, read, read_inventory
from obspy.core.inventory import Inventory
from obspy.io.mseed import InternalMSEEDWarning
from seispy.progress import call_with_warnings, progress_bar, resolve_worker_call

from seispy.archive import WaveformIdentity, channel_mseed_path, matches_mseed_path
from seispy.waveform.mseed_recovery import filter_valid_mseed_records
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    resolve_separate_directory_trees,
    temporary_output_path,
)

_WORKER_INVENTORY = None


@dataclass(frozen=True)
class WaveformArchiveIssue:
    """Describe one sampled raw-response archival failure."""

    source: Path
    error: str


@dataclass(frozen=True)
class _ArchiveResult:
    succeeded: int = 0
    skipped: int = 0
    recovered: int = 0
    failed: int = 0
    files_written: int = 0
    traces_total: int = 0
    traces_written: int = 0
    traces_existing: int = 0
    traces_ignored_empty: int = 0
    traces_failed: int = 0
    issue: WaveformArchiveIssue | None = None


@dataclass(frozen=True)
class WaveformArchiveSummary(BatchSummary):
    """Summarize validation and organization into a trusted archive.

    Source-file counters are mutually exclusive: a source succeeds when at
    least one non-empty trace is archived, is skipped when all usable traces
    already exist, and fails only when no usable trace can be preserved.
    Trace counters expose partial recovery without turning it into a source
    failure.
    """

    total: int
    succeeded: int
    skipped: int
    recovered: int
    failed: int
    files_written: int
    traces_total: int
    traces_written: int
    traces_existing: int
    traces_ignored_empty: int
    traces_failed: int
    issue_samples: tuple[WaveformArchiveIssue, ...]
    source_dir: Path
    output_dir: Path
    output_format: Literal["mseed", "sac"]

    @property
    def has_issues(self) -> bool:
        return bool(self.failed or self.recovered)


def archive_waveforms(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    output_format: Literal["mseed", "sac"] = "mseed",
    inventory: str | Path | Inventory | None = None,
    pattern: str = "*.mseed.raw",
    max_workers: int = 5,
    overwrite: bool = False,
    discard_corrupt_records: bool = True,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> WaveformArchiveSummary:
    """Validate and organize MiniSEED responses or SAC files.

    Raw MiniSEED responses can be archived as MiniSEED or converted to SAC.
    Existing SAC files can be reorganized into canonical SAC paths by selecting
    them with ``pattern`` and ``output_format="sac"``. Valid traces are archived
    independently, so one trace or destination failure does not discard other
    usable traces from the same source. A source succeeds when at least one
    trace is preserved. Empty traces do not determine archive identity. Native
    waveform reads run in isolated worker processes. Source files are never
    modified or removed.

    Args:
        source_dir: Root containing raw MiniSEED responses or SAC files.
        output_dir: Root for the trusted waveform archive.
        output_format: Trusted archive format, ``"mseed"`` or ``"sac"``.
        inventory: Optional StationXML metadata used to verify NSLC epochs and
            sample rates.
        pattern: Recursive source filename pattern. Use, for example,
            ``"*.sac"`` to organize existing SAC files.
        max_workers: Number of isolated validation/archive processes.
        overwrite: Replace existing archive outputs.
        discard_corrupt_records: Recover independently valid records when full
            MiniSEED validation reports an integrity failure.
        max_error_samples: Maximum sampled failures in the result.
        save_report: Persist the continuously updated JSON report.
        save_log: Persist the human-readable run log.
    """
    if max_workers < 1 or max_error_samples < 0:
        raise ValueError(
            "max_workers must be positive and max_error_samples non-negative"
        )
    output_format = output_format.lower()
    if output_format not in {"mseed", "sac"}:
        raise ValueError("output_format must be 'mseed' or 'sac'")
    source, output = resolve_separate_directory_trees(source_dir, output_dir)
    if not source.is_dir():
        raise NotADirectoryError(source)
    output.mkdir(parents=True, exist_ok=True)
    files = tuple(sorted(path for path in source.rglob(pattern) if path.is_file()))
    manifest = (
        inventory
        if isinstance(inventory, Inventory) or inventory is None
        else read_inventory(str(inventory), format="STATIONXML")
    )
    run_id = new_run_id()
    counters = dict.fromkeys(
        (
            "succeeded",
            "skipped",
            "recovered",
            "failed",
            "files_written",
            "traces_total",
            "traces_written",
            "traces_existing",
            "traces_ignored_empty",
            "traces_failed",
        ),
        0,
    )
    issues = []
    with BatchRun(
        "waveform-archive",
        output,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
    ) as run:
        run.start(
            total=len(files),
            **counters,
            issue_samples=(),
            source_dir=source,
            output_dir=output,
            output_format=output_format,
        )
        run.info(
            "run_id=%s source=%s output=%s format=%s max_workers=%d",
            run_id,
            source,
            output,
            output_format,
            max_workers,
        )
        tasks = iter(files)
        pending = {}
        executor = _archive_executor(max_workers, manifest)
        try:
            with progress_bar(total=len(files), desc="Archiving", unit="file") as bar:
                while pending or tasks is not None:
                    while tasks is not None and len(pending) < max_workers * 3:
                        try:
                            path = next(tasks)
                        except StopIteration:
                            tasks = None
                            break
                        try:
                            future = executor.submit(
                                call_with_warnings,
                                _archive_one,
                                str(path),
                                str(source),
                                str(output),
                                output_format,
                                overwrite,
                                discard_corrupt_records,
                            )
                        except Exception as exc:
                            result = _failed_result(path, exc)
                            _record_result(result, counters, issues, max_error_samples)
                            _log_issue(run, result)
                            bar.update(1)
                            _checkpoint_archive(
                                run,
                                counters,
                                issues,
                                len(files),
                                source,
                                output,
                                output_format,
                            )
                            continue
                        pending[future] = path
                    if not pending:
                        continue
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        path = pending.pop(future)
                        try:
                            result = resolve_worker_call(future.result())
                        except Exception as exc:
                            result = _failed_result(path, exc)
                        _record_result(result, counters, issues, max_error_samples)
                        _log_issue(run, result)
                        bar.update(1)
                        _checkpoint_archive(
                            run,
                            counters,
                            issues,
                            len(files),
                            source,
                            output,
                            output_format,
                        )
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
        summary = run.complete(
            WaveformArchiveSummary(
                run_id=run_id,
                total=len(files),
                **counters,
                issue_samples=tuple(issues),
                source_dir=source,
                output_dir=output,
                output_format=output_format,
                duration_seconds=0,
            )
        )
    return summary


def _archive_executor(workers, inventory):
    return ProcessPoolExecutor(
        max_workers=workers,
        initializer=_initialize_archive_worker,
        initargs=(inventory,),
    )


def _failed_result(path, exc):
    return _ArchiveResult(
        failed=1,
        issue=WaveformArchiveIssue(path, f"{type(exc).__name__}: {exc}"),
    )


def _record_result(result, counters, issues, max_error_samples):
    for name in counters:
        counters[name] += getattr(result, name)
    if result.issue is not None and len(issues) < max_error_samples:
        issues.append(result.issue)


def _log_issue(run, result):
    if result.issue is not None:
        log = run.error if result.failed else run.warning
        status = "archive_failed" if result.failed else "archive_partial"
        log(
            "source=%s %s error=%s",
            result.issue.source,
            status,
            result.issue.error,
        )


def _checkpoint_archive(
    run,
    counters,
    issues,
    total,
    source,
    output,
    output_format,
):
    completed = sum(counters[name] for name in ("succeeded", "skipped", "failed"))
    run.checkpoint(
        completed=completed,
        total=total,
        **counters,
        issue_samples=tuple(issues),
        source_dir=source,
        output_dir=output,
        output_format=output_format,
    )


def _initialize_archive_worker(inventory):
    global _WORKER_INVENTORY
    _WORKER_INVENTORY = inventory


def _archive_one(
    source_name,
    source_root,
    output_root,
    output_format,
    overwrite,
    discard_corrupt_records,
):
    source = Path(source_name)
    filtered = None
    record_recovered = False
    try:
        is_sac = source.suffix.lower() == ".sac"
        if is_sac:
            if output_format != "sac":
                raise ValueError("SAC sources require output_format='sac'")
            stream = _read_full_sac(source)
            trusted_source = source
        else:
            try:
                stream = _read_full_mseed(source)
                trusted_source = source
            except InternalMSEEDWarning:
                if not discard_corrupt_records:
                    raise
                recovery_target = Path(output_root) / f"{source.name}.recovered"
                filtered = temporary_output_path(recovery_target)
                filter_valid_mseed_records(
                    source,
                    filtered,
                    network="*",
                    station="*",
                    location="*",
                    channel="*",
                )
                stream = _read_full_mseed(filtered)
                trusted_source = filtered
                record_recovered = True
        valid, empty, validation_errors = _classify_traces(stream)
        if not valid:
            detail = "; ".join(validation_errors) or "waveform stream contains no samples"
            raise ValueError(detail)
        if output_format == "mseed":
            result = _archive_mseed_traces(
                source,
                trusted_source,
                valid,
                empty,
                Path(source_root),
                Path(output_root),
                overwrite,
                allow_raw_copy=not validation_errors,
            )
        else:
            result = _archive_sac_traces(valid, Path(output_root), overwrite)
        written, existing, archived_traces, group_errors, transformed = result
        errors = [*validation_errors, *group_errors]
        traces_failed = len(stream) - len(empty) - archived_traces
        archived_any = archived_traces > 0
        all_existing = archived_any and written == 0 and not errors
        partial = bool(
            record_recovered or transformed or empty or errors or traces_failed
        )
        issue = None
        if errors:
            issue = WaveformArchiveIssue(source, "; ".join(errors))
        return _ArchiveResult(
            succeeded=int(archived_any and not all_existing),
            skipped=int(all_existing),
            recovered=int(archived_any and partial),
            failed=int(not archived_any),
            files_written=written,
            traces_total=len(stream),
            traces_written=archived_traces - existing,
            traces_existing=existing,
            traces_ignored_empty=len(empty),
            traces_failed=max(0, traces_failed),
            issue=issue,
        )
    except Exception as exc:
        traces_total = len(stream) if "stream" in locals() else 0
        traces_ignored_empty = (
            sum(trace.stats.npts == 0 for trace in stream)
            if "stream" in locals()
            else 0
        )
        return _ArchiveResult(
            failed=1,
            traces_total=traces_total,
            traces_ignored_empty=traces_ignored_empty,
            traces_failed=traces_total - traces_ignored_empty,
            issue=WaveformArchiveIssue(source, f"{type(exc).__name__}: {exc}"),
        )
    finally:
        if filtered is not None:
            filtered.unlink(missing_ok=True)


def _read_full_mseed(path):
    with warnings.catch_warnings():
        warnings.simplefilter("error", InternalMSEEDWarning)
        stream = read(path, format="MSEED")
    if not stream:
        raise ValueError("waveform stream is empty")
    return stream


def _read_full_sac(path):
    stream = read(path, format="SAC")
    if len(stream) != 1:
        raise ValueError(f"SAC source must contain exactly one trace: {path}")
    return stream


def _validate_inventory(stream):
    if _WORKER_INVENTORY is None:
        return
    for trace in stream:
        stats = trace.stats
        selected = _WORKER_INVENTORY.select(
            network=stats.network,
            station=stats.station,
            location=stats.location,
            channel=stats.channel,
            starttime=stats.starttime,
            endtime=stats.endtime,
        )
        channels = [channel for net in selected for sta in net for channel in sta]
        if not channels:
            raise ValueError(f"no inventory epoch matches {trace.id}")
        if not any(
            abs(float(channel.sample_rate) - float(stats.sampling_rate)) <= 1e-6
            for channel in channels
        ):
            raise ValueError(
                f"sample rate {stats.sampling_rate} does not match inventory "
                f"for {trace.id}"
            )


def _classify_traces(stream):
    valid = []
    empty = []
    errors = []
    for index, trace in enumerate(stream):
        if trace.stats.npts == 0:
            empty.append(trace)
            continue
        try:
            _validate_inventory([trace])
        except Exception as exc:
            errors.append(f"trace {index} ({trace.id}) failed validation: {exc}")
            continue
        valid.append(trace)
    return valid, empty, errors


def _archive_mseed_traces(
    source,
    trusted_source,
    traces,
    empty_traces,
    source_root,
    output_root,
    overwrite,
    *,
    allow_raw_copy,
):
    intended = output_root / source.relative_to(source_root).with_name(
        source.name.removesuffix(".raw")
    )
    if (
        allow_raw_copy
        and _matches_mseed_path(intended, output_root, traces)
        and _benign_empty_traces(traces, empty_traces)
    ):
        written, skipped = _archive_mseed(
            source,
            trusted_source,
            traces,
            source_root,
            output_root,
            overwrite,
        )
        existing = len(traces) if skipped else 0
        return written, existing, len(traces), (), False

    grouped = defaultdict(list)
    for trace in traces:
        grouped[WaveformIdentity.from_trace(trace).day_key].append(trace)

    files_written = 0
    traces_existing = 0
    traces_archived = 0
    errors = []
    intended_claimed = False
    for group in grouped.values():
        if not intended_claimed and _matches_mseed_path(
            intended, output_root, group
        ):
            destination = intended
            intended_claimed = True
        else:
            destination = _recovered_mseed_path(output_root, group)
        try:
            written, skipped = _write_mseed_group(group, destination, overwrite)
        except Exception as exc:
            errors.append(
                f"trace group {group[0].id} at "
                f"{group[0].stats.starttime} failed: {type(exc).__name__}: {exc}"
            )
            continue
        files_written += written
        traces_archived += len(group)
        if skipped:
            traces_existing += len(group)

    if empty_traces and not _benign_empty_traces(traces, empty_traces):
        errors.append(f"ignored {len(empty_traces)} incompatible empty trace(s)")
    return files_written, traces_existing, traces_archived, tuple(errors), True


def _matches_mseed_path(path, root, traces):
    try:
        return matches_mseed_path(path, root, traces)
    except ValueError:
        return False


def _benign_empty_traces(traces, empty_traces):
    identities = [WaveformIdentity.from_trace(trace) for trace in traces]
    valid_nslc = {
        (item.network, item.station, item.location, item.channel) for item in identities
    }
    valid_days = {item.day for item in identities}
    for trace in empty_traces:
        identity = WaveformIdentity.from_trace(trace)
        nslc = (
            identity.network,
            identity.station,
            identity.location,
            identity.channel,
        )
        if nslc not in valid_nslc:
            return False
        if identity.day not in valid_days and not any(
            (identity.day - day).days == 1
            and trace.stats.starttime.strftime("%H%M%S") == "000000"
            for day in valid_days
        ):
            return False
    return True


def _recovered_mseed_path(output_root, traces):
    first = min(traces, key=lambda trace: trace.stats.starttime)
    last = max(traces, key=lambda trace: trace.stats.endtime)
    endtime = last.stats.endtime + last.stats.delta
    stats = first.stats
    return channel_mseed_path(
        output_root,
        stats.network,
        stats.station,
        stats.location,
        stats.channel,
        stats.starttime,
        endtime,
    )


def _write_mseed_group(traces, destination, overwrite):
    destination = Path(destination)
    if destination.exists() and not overwrite:
        _validate_existing_mseed(destination, destination.parents[3])
        return 0, True
    temporary = temporary_output_path(destination)
    try:
        Stream(traces=traces).write(str(temporary), format="MSEED")
        written = _read_full_mseed(temporary)
        root = destination.parents[3]
        if not matches_mseed_path(destination, root, written):
            raise ValueError("written MiniSEED path disagrees with trace headers")
        commit_output(temporary, destination, overwrite=overwrite)
    finally:
        temporary.unlink(missing_ok=True)
    return 1, False


def _archive_sac_traces(traces, output_root, overwrite):
    files_written = 0
    traces_existing = 0
    traces_archived = 0
    errors = []
    claimed = set()
    for trace in traces:
        destination = WaveformIdentity.from_trace(trace).sac_path(output_root)
        if destination in claimed:
            errors.append(f"trace {trace.id} maps to duplicate path {destination}")
            continue
        claimed.add(destination)
        try:
            if destination.is_file() and not overwrite:
                _validate_sac_output(destination, trace)
                traces_existing += 1
                traces_archived += 1
                continue
            temporary = temporary_output_path(destination)
            try:
                trace.write(str(temporary), format="SAC")
                _validate_sac_output(temporary, trace)
                commit_output(temporary, destination, overwrite=overwrite)
            finally:
                temporary.unlink(missing_ok=True)
        except Exception as exc:
            errors.append(
                f"trace {trace.id} failed: {type(exc).__name__}: {exc}"
            )
            continue
        files_written += 1
        traces_archived += 1
    return files_written, traces_existing, traces_archived, tuple(errors), False


def _archive_mseed(source, trusted_source, stream, source_root, output_root, overwrite):
    relative = source.relative_to(source_root)
    if not relative.name.endswith(".mseed.raw"):
        raise ValueError(f"raw source does not end with .mseed.raw: {source}")
    destination = output_root / relative.with_name(relative.name.removesuffix(".raw"))
    if not matches_mseed_path(destination, output_root, stream):
        raise ValueError(
            f"raw response headers do not match intended archive path {destination}"
        )
    if destination.exists() and not overwrite:
        _validate_existing_mseed(destination, output_root)
        return 0, True
    temporary = temporary_output_path(destination)
    try:
        shutil.copyfile(trusted_source, temporary)
        commit_output(temporary, destination, overwrite=overwrite)
    finally:
        temporary.unlink(missing_ok=True)
    return 1, False


def _validate_existing_mseed(path, output_root):
    stream = _read_full_mseed(path)
    if not matches_mseed_path(path, output_root, stream):
        raise ValueError(f"existing MiniSEED path disagrees with headers: {path}")


def _validate_sac_output(path, expected):
    stream = read(path, format="SAC", headonly=True)
    if len(stream) != 1:
        raise ValueError("SAC output must contain exactly one trace")
    actual = WaveformIdentity.from_trace(stream[0])
    wanted = WaveformIdentity.from_trace(expected)
    tolerance = 0.5 / float(expected.stats.sampling_rate)
    if (
        actual.day_key != wanted.day_key
        or abs(stream[0].stats.starttime - expected.stats.starttime) > tolerance
    ):
        raise ValueError("SAC output identity does not match source trace")
