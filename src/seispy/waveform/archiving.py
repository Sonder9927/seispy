"""Validate raw waveform responses and commit trusted MiniSEED or SAC archives."""

import shutil
import warnings
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from obspy import read, read_inventory
from obspy.core.inventory import Inventory
from obspy.io.mseed import InternalMSEEDWarning
from tqdm import tqdm

from seispy.archive import WaveformIdentity, matches_mseed_path
from seispy.waveform.integrity import merge_short_gaps
from seispy.waveform.mseed_recovery import filter_valid_mseed_records
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    cleanup_outputs,
    commit_output,
    new_run_id,
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
    originals_removed: int = 0
    originals_retained: int = 0
    issue: WaveformArchiveIssue | None = None


@dataclass(frozen=True)
class WaveformArchiveSummary(BatchSummary):
    """Summarize conversion of raw responses into a trusted archive."""

    total: int
    succeeded: int
    skipped: int
    recovered: int
    failed: int
    files_written: int
    originals_removed: int
    originals_retained: int
    issue_samples: tuple[WaveformArchiveIssue, ...]
    source_dir: Path
    output_dir: Path
    output_format: Literal["mseed", "sac"]
    remove_original: bool

    @property
    def has_issues(self) -> bool:
        return bool(self.failed or self.recovered or self.originals_retained)


def archive_waveforms(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    output_format: Literal["mseed", "sac"] = "mseed",
    inventory: str | Path | Inventory | None = None,
    pattern: str = "*.mseed.raw",
    max_workers: int = 5,
    remove_original: bool = False,
    overwrite: bool = False,
    discard_corrupt_records: bool = True,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> WaveformArchiveSummary:
    """Validate raw responses and archive them as MiniSEED or SAC.

    Native MiniSEED operations run in isolated worker processes. A source is
    removed only after every output for that source commits successfully.
    Sources that required record recovery are retained as forensic evidence.

    Args:
        source_dir: Root containing ``.mseed.raw`` responses.
        output_dir: Root for the trusted waveform archive.
        output_format: Trusted archive format, ``"mseed"`` or ``"sac"``.
        inventory: Optional StationXML metadata used to verify NSLC epochs and
            sample rates.
        pattern: Recursive source filename pattern.
        max_workers: Number of isolated validation/archive processes.
        remove_original: Remove a fully valid raw source after all outputs
            commit. Recovered sources are retained.
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
    source = Path(source_dir).expanduser().resolve()
    output = Path(output_dir).expanduser().resolve()
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
            "originals_removed",
            "originals_retained",
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
            remove_original=remove_original,
        )
        run.info(
            "run_id=%s source=%s output=%s format=%s max_workers=%d remove_original=%s",
            run_id,
            source,
            output,
            output_format,
            max_workers,
            remove_original,
        )
        tasks = iter(files)
        pending = {}
        executor = _archive_executor(max_workers, manifest)
        try:
            with tqdm(total=len(files), desc="Archiving waveforms") as bar:
                while pending or tasks is not None:
                    while tasks is not None and len(pending) < max_workers * 3:
                        try:
                            path = next(tasks)
                        except StopIteration:
                            tasks = None
                            break
                        try:
                            future = executor.submit(
                                _archive_one,
                                str(path),
                                str(source),
                                str(output),
                                output_format,
                                remove_original,
                                overwrite,
                                discard_corrupt_records,
                            )
                        except Exception as exc:
                            result = _failed_result(path, exc, remove_original)
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
                                remove_original,
                            )
                            continue
                        pending[future] = path
                    if not pending:
                        continue
                    done, _ = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        path = pending.pop(future)
                        try:
                            result = future.result()
                        except Exception as exc:
                            result = _failed_result(path, exc, remove_original)
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
                            remove_original,
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
                remove_original=remove_original,
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


def _failed_result(path, exc, remove_original):
    return _ArchiveResult(
        failed=1,
        originals_retained=int(remove_original),
        issue=WaveformArchiveIssue(path, f"{type(exc).__name__}: {exc}"),
    )


def _record_result(result, counters, issues, max_error_samples):
    for name in counters:
        counters[name] += getattr(result, name)
    if result.issue is not None and len(issues) < max_error_samples:
        issues.append(result.issue)


def _log_issue(run, result):
    if result.issue is not None:
        run.error(
            "source=%s archive_failed error=%s",
            result.issue.source,
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
    remove_original,
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
        remove_original=remove_original,
    )


def _initialize_archive_worker(inventory):
    global _WORKER_INVENTORY
    _WORKER_INVENTORY = inventory


def _archive_one(
    source_name,
    source_root,
    output_root,
    output_format,
    remove_original,
    overwrite,
    discard_corrupt_records,
):
    source = Path(source_name)
    filtered = None
    recovered = False
    discarded = 0
    try:
        try:
            stream = _read_full_mseed(source)
            trusted_source = source
        except InternalMSEEDWarning:
            if not discard_corrupt_records:
                raise
            filtered = temporary_output_path(source)
            recovery = filter_valid_mseed_records(
                source,
                filtered,
                network="*",
                station="*",
                location="*",
                channel="*",
            )
            stream = _read_full_mseed(filtered)
            trusted_source = filtered
            recovered = True
            discarded = recovery.discarded_records
        _validate_inventory(stream)
        if output_format == "mseed":
            written, skipped = _archive_mseed(
                source,
                trusted_source,
                stream,
                Path(source_root),
                Path(output_root),
                overwrite,
            )
        else:
            written, skipped = _archive_sac(stream, Path(output_root), overwrite)
        removed = 0
        retained = 0
        if remove_original and not recovered:
            source.unlink()
            removed = 1
        elif remove_original:
            retained = 1
        return _ArchiveResult(
            succeeded=int(not skipped),
            skipped=int(skipped),
            recovered=int(recovered),
            files_written=written,
            originals_removed=removed,
            originals_retained=retained,
        )
    except Exception as exc:
        return _ArchiveResult(
            failed=1,
            originals_retained=int(remove_original),
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


def _archive_sac(stream, output_root, overwrite):
    merge_short_gaps(stream)
    destinations = [
        WaveformIdentity.from_trace(trace).sac_path(output_root) for trace in stream
    ]
    if len(destinations) != len(set(destinations)):
        raise ValueError("multiple source traces map to the same SAC archive path")
    if destinations and all(path.is_file() for path in destinations) and not overwrite:
        for trace, destination in zip(stream, destinations, strict=True):
            _validate_sac_output(destination, trace)
        return 0, True
    temporary_paths = []
    try:
        for trace, destination in zip(stream, destinations, strict=True):
            temporary = temporary_output_path(destination)
            temporary_paths.append(temporary)
            trace.write(str(temporary), format="SAC")
            _validate_sac_output(temporary, trace)
        for temporary, destination in zip(
            tuple(temporary_paths), destinations, strict=True
        ):
            commit_output(temporary, destination, overwrite=overwrite)
            temporary_paths.remove(temporary)
    except Exception:
        cleanup_outputs(temporary_paths)
        raise
    return len(destinations), False


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
