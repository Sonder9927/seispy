"""Filter waveform traces by a reusable quality policy."""

import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import obspy

from seispy.progress import progress_bar, resolve_worker_call
from seispy.waveform.integrity import (
    DEFAULT_TRACE_FILTER,
    TraceFilter,
    split_traces_by_filter,
)
from seispy._pool import process_batches, validate_files_per_pool
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    resolve_separate_directory_trees,
    temporary_output_path,
)

logger = logging.getLogger(__name__)
DEFAULT_EXTENSIONS: tuple[str, ...] = (".mseed", ".miniseed", ".sac")


@dataclass(frozen=True)
class WaveformFilterIssue:
    """Describe one sampled filtered trace or filtering failure."""

    source: Path
    status: str
    error: str
    destination: Path | None = None


@dataclass(frozen=True)
class _Counts:
    total: int = 0
    succeeded: int = 0
    skipped: int = 0
    failed: int = 0
    traces_total: int = 0
    traces_written: int = 0
    traces_filtered: int = 0
    samples: tuple[WaveformFilterIssue, ...] = ()


@dataclass(frozen=True)
class WaveformFilterSummary(BatchSummary):
    """Summarize a trace-filtering run.

    A file succeeds when at least one trace is kept, is skipped when every trace
    is filtered out, and fails only when the file cannot be read or written.
    Filtering is a deliberate skip and never counts as an issue.
    """

    total: int
    succeeded: int
    skipped: int
    failed: int
    traces_total: int
    traces_written: int
    traces_filtered: int
    issue_samples: tuple[WaveformFilterIssue, ...]
    output_dir: Path

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)


def filter_waveforms(
    source_dir: str | Path,
    output_dir: str | Path,
    *,
    trace_filter: TraceFilter = DEFAULT_TRACE_FILTER,
    low_frequency: float | None = None,
    extensions: Sequence[str] | str = DEFAULT_EXTENSIONS,
    max_workers: int = 5,
    files_per_pool: int | None = 1024,
    batch_size: int | None = None,
    overwrite: bool = True,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> WaveformFilterSummary:
    """Keep only traces that satisfy a reusable quality policy.

    Every matching MiniSEED or SAC file is read, its traces are split by
    trace_filter, and the kept traces are written to the same relative path under
    output_dir. Files whose traces are all filtered are skipped without writing.

    Args:
        source_dir: Input tree containing MiniSEED and/or SAC files.
        output_dir: Separate output tree; relative paths are preserved.
        trace_filter: Reusable acceptance policy.
        low_frequency: Lowest frequency of interest in hertz, used to enforce
            trace_filter.min_periods. When omitted, min_periods is not applied.
        extensions: File suffixes to scan, for example (".mseed", ".sac").
        files_per_pool: Input files per pool generation across all workers.
            Defaults to 1024; None disables recycling. Uses spawn, requiring a
            __main__ guard in scripts. This is not a per-file memory limit.
        max_workers: Number of worker processes.
        batch_size: Files per worker task; by default enough batches for eight
            scheduling waves, capped at 32.
        overwrite: Replace existing outputs. When False a conflict fails the file.
        max_error_samples: Maximum sampled failures retained in the summary.
        save_report: Write a continuously updated JSON report.
        save_log: Write a persistent run log.

    Returns:
        Counts, sampled issues, output location, and run duration.

    Examples:
        ```python
        from seispy.waveform import TraceFilter, filter_waveforms
        filter_waveforms(
            "data/archive", "data/filtered",
            trace_filter=TraceFilter(min_duration_seconds=60),
        )
        ```
    """
    validate_files_per_pool(files_per_pool)
    run_id = new_run_id()
    if max_workers < 1 or max_error_samples < 0:
        raise ValueError(
            "max_workers must be positive and max_error_samples non-negative"
        )
    if batch_size is not None and batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    suffixes = _normalize_extensions(extensions)
    source, output = resolve_separate_directory_trees(source_dir, output_dir)
    if not source.is_dir():
        raise NotADirectoryError(source)
    files = tuple(
        path
        for path in sorted(source.rglob("*"))
        if path.is_file() and path.suffix.lower() in suffixes
    )
    output.mkdir(parents=True, exist_ok=True)
    actual_batch = _batch_size(len(files), max_workers, batch_size)
    with BatchRun(
        "waveform-filter",
        output,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=len(files),
            succeeded=0,
            skipped=0,
            failed=0,
            traces_total=0,
            traces_written=0,
            traces_filtered=0,
            issue_samples=(),
            output_dir=output,
        )
        run.info(
            "run_id=%s source=%s output=%s extensions=%s max_workers=%d",
            run_id,
            source,
            output,
            suffixes,
            max_workers,
        )
        combined = _run_filter_batches(
            files,
            source,
            output,
            max_workers,
            actual_batch,
            max_error_samples,
            trace_filter,
            low_frequency,
            overwrite,
            run,
            files_per_pool=files_per_pool,
        )
        summary = run.complete(
            WaveformFilterSummary(
                run_id=run_id,
                total=combined.total,
                succeeded=combined.succeeded,
                skipped=combined.skipped,
                failed=combined.failed,
                traces_total=combined.traces_total,
                traces_written=combined.traces_written,
                traces_filtered=combined.traces_filtered,
                issue_samples=combined.samples,
                output_dir=output,
                duration_seconds=0,
            )
        )
        for item in summary.issue_samples:
            run.info(
                "run_id=%s status=%s source=%s reason=%s",
                run_id,
                item.status,
                item.source,
                item.error,
            )
    print(
        f"Waveform filtering [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.skipped} skipped, {summary.failed} failed, "
        f"{summary.traces_written} traces kept."
    )
    return summary


def _run_filter_batches(
    files,
    source_root,
    output_root,
    max_workers,
    batch_size,
    max_error_samples,
    trace_filter,
    low_frequency,
    overwrite,
    run,
    *,
    files_per_pool=1024,
):
    total = len(files)
    worker_limit = min(max_error_samples, 1)
    combined = _Counts()
    with (
        process_batches(
            files,
            _process_batch,
            (
                source_root,
                output_root,
                worker_limit,
                trace_filter,
                low_frequency,
                overwrite,
            ),
            max_workers=max_workers,
            batch_size=batch_size,
            files_per_pool=files_per_pool,
            run=run,
        ) as results,
        progress_bar(total=total, desc="Filtering", unit="file") as bar,
    ):
        for batch, future in results:
            try:
                result = resolve_worker_call(future.result())
            except Exception as exc:
                result = _failed_batch(batch, exc, worker_limit)
            combined = _combine((combined, result), max_error_samples)
            bar.update(result.total)
            run.checkpoint(
                completed=combined.total,
                total=total,
                succeeded=combined.succeeded,
                skipped=combined.skipped,
                failed=combined.failed,
                traces_total=combined.traces_total,
                traces_written=combined.traces_written,
                traces_filtered=combined.traces_filtered,
                issue_samples=combined.samples,
                output_dir=output_root,
            )
    return combined


def _process_batch(
    files, source_root, output_root, limit, trace_filter, low_frequency, overwrite
):
    return _combine(
        [
            _filter_file(
                path,
                source_root,
                output_root,
                limit,
                trace_filter,
                low_frequency,
                overwrite,
            )
            for path in files
        ],
        limit,
    )


def _filter_file(
    source, source_root, output_root, limit, trace_filter, low_frequency, overwrite
):
    source = Path(source)
    destination = Path(output_root) / source.relative_to(source_root)
    samples = []
    traces_total = 0
    try:
        stream = obspy.read(str(source))
        traces_total = len(stream)
        if not traces_total:
            raise ValueError("waveform file contains no traces")
        kept, rejected = split_traces_by_filter(
            stream, trace_filter, low_frequency=low_frequency
        )
        for _, reason in rejected:
            if len(samples) < limit:
                samples.append(
                    WaveformFilterIssue(source, "trace_filtered", reason, destination)
                )
        if not kept:
            return _Counts(
                total=1,
                skipped=1,
                traces_total=traces_total,
                traces_filtered=len(rejected),
                samples=tuple(samples),
            )
        _write_kept(kept, destination, overwrite)
    except Exception as exc:
        sample = (
            (
                WaveformFilterIssue(
                    source,
                    "filter_failed",
                    f"{type(exc).__name__}: {exc}",
                    destination,
                ),
            )
            if limit
            else ()
        )
        return _Counts(total=1, failed=1, traces_total=traces_total, samples=sample)
    return _Counts(
        total=1,
        succeeded=1,
        traces_total=traces_total,
        traces_written=len(kept),
        traces_filtered=len(rejected),
        samples=tuple(samples),
    )


def _write_kept(kept, destination, overwrite):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.suffix.lower() == ".sac":
        if len(kept) != 1:
            raise ValueError("SAC output requires exactly one kept trace")
        temporary = temporary_output_path(destination)
        try:
            kept[0].write(str(temporary), format="SAC")
            commit_output(temporary, destination, overwrite=overwrite)
        finally:
            temporary.unlink(missing_ok=True)
        return
    temporary = temporary_output_path(destination)
    try:
        obspy.Stream(traces=kept).write(str(temporary), format="MSEED")
        commit_output(temporary, destination, overwrite=overwrite)
    finally:
        temporary.unlink(missing_ok=True)


def _normalize_extensions(extensions: Sequence[str] | str) -> tuple[str, ...]:
    values = (extensions,) if isinstance(extensions, str) else tuple(extensions)
    suffixes = []
    for value in values:
        suffix = value.lower()
        suffixes.append(suffix if suffix.startswith(".") else f".{suffix}")
    if not suffixes:
        raise ValueError("extensions must contain at least one suffix")
    return tuple(dict.fromkeys(suffixes))


def _batch_size(total, max_workers, requested):
    if requested is not None:
        return requested
    return max(1, min(32, math.ceil(total / (max_workers * 8)) or 1))


def _failed_batch(files, exc, limit):
    sample = (
        (
            WaveformFilterIssue(
                files[0], "filter_failed", f"{type(exc).__name__}: {exc}"
            ),
        )
        if files and limit
        else ()
    )
    return _Counts(total=len(files), failed=len(files), samples=sample)


def _combine(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[: max(0, limit - len(samples))])
    return _Counts(
        total=sum(x.total for x in items),
        succeeded=sum(x.succeeded for x in items),
        skipped=sum(x.skipped for x in items),
        failed=sum(x.failed for x in items),
        traces_total=sum(x.traces_total for x in items),
        traces_written=sum(x.traces_written for x in items),
        traces_filtered=sum(x.traces_filtered for x in items),
        samples=tuple(samples),
    )
