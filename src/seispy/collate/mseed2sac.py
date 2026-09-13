import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import obspy
from seispy._archive import WaveformIdentity, preserve_sac_quality
from seispy._batch import (
    BatchRun,
    BatchSummary,
    cleanup_outputs,
    commit_output,
    new_run_id,
    temporary_output_path,
)
from tqdm import tqdm

from seispy._waveform import merge_short_gaps

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Mseed2SacIssue:
    """Describe one sampled MiniSEED conversion issue."""

    source: Path
    status: str
    error: str
    destination: Path | None = None


@dataclass(frozen=True)
class _Counts:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    removed: int = 0
    removal_failed: int = 0
    traces_written: int = 0
    conflicts: int = 0
    samples: tuple[Mseed2SacIssue, ...] = ()


@dataclass(frozen=True)
class Mseed2SacSummary(BatchSummary):
    """Summarize a MiniSEED-to-SAC conversion run.

    This class is returned by :func:`mseed2sac`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        input_total: Number of MiniSEED inputs discovered.
        input_succeeded: Number converted successfully.
        input_failed: Number that could not be converted.
        originals_removed: Number of source files removed after conversion.
        removal_failed: Number of requested removals that failed.
        traces_written: Number of SAC traces committed to disk.
        output_conflicts: Number of existing destinations not overwritten.
        error_samples: Bounded sample of conversion and removal issues.
        output_dir: Root directory containing generated SAC files.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        ```python
        summary = mseed2sac(...)
        print(summary.traces_written, summary.input_failed)
        ```
    """

    input_total: int
    input_succeeded: int
    input_failed: int
    originals_removed: int
    removal_failed: int
    traces_written: int
    output_conflicts: int
    error_samples: tuple[Mseed2SacIssue, ...]
    output_dir: Path

    @property
    def has_issues(self) -> bool:
        return bool(self.input_failed or self.removal_failed)


def mseed2sac(
    source: str | Path,
    output_dir: str | Path,
    pattern: str = "*.miniseed",
    batch_size: int = 1000,
    max_workers: int = 5,
    *,
    remove_original: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> Mseed2SacSummary:
    """Convert a MiniSEED file or directory tree to SAC.

    Args:
        source: Input MiniSEED file or directory.
        output_dir: Destination root for the SAC directory tree.
        pattern: Recursive file pattern used when ``source`` is a directory.
        batch_size: Maximum number of input files assigned to each worker task.
        max_workers: Maximum number of worker processes.
        remove_original: Remove each input only after all its outputs commit.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains it only when issues occur.
        save_log: Write a persistent run log. Defaults to ``True``.

    Returns:
        Conversion counts, sampled issues, output location, and run duration.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        ValueError: If limits are invalid or output is nested inside the input.

    Examples:
        ```python
        summary = mseed2sac(
            "data/miniseed", "data/sac", remove_original=False
        )
        summary.output_dir.name
        # => 'sac'
        ```
    """
    run_id = new_run_id()
    source = Path(source).expanduser().resolve()
    output = Path(output_dir).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(source)
    if batch_size < 1 or max_workers < 1:
        raise ValueError("batch_size and max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if source.is_dir() and (output == source or source in output.parents):
        raise ValueError("output_dir must be outside the source directory")
    output.mkdir(parents=True, exist_ok=True)
    files = [source] if source.is_file() else sorted(source.rglob(pattern))
    files = [path for path in files if path.is_file()]
    batches = [files[i : i + batch_size] for i in range(0, len(files), batch_size)]
    with BatchRun(
        "mseed2sac",
        output,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=len(files),
            input_total=len(files),
            input_completed=0,
            input_succeeded=0,
            input_failed=0,
            originals_removed=0,
            removal_failed=0,
            traces_written=0,
            output_conflicts=0,
            error_samples=(),
            output_dir=output,
        )
        run.info(
            "run_id=%s source=%s output=%s remove_original=%s",
            run_id,
            source,
            output,
            remove_original,
        )
        combined = _run_conversion_batches(
            batches,
            output,
            remove_original,
            max_workers,
            max_error_samples,
            run,
        )
        summary = run.complete(
            Mseed2SacSummary(
                run_id=run_id,
                input_total=combined.total,
                input_succeeded=combined.succeeded,
                input_failed=combined.failed,
                originals_removed=combined.removed,
                removal_failed=combined.removal_failed,
                traces_written=combined.traces_written,
                output_conflicts=combined.conflicts,
                error_samples=combined.samples,
                output_dir=output,
                duration_seconds=0,
            )
        )
        for item in summary.error_samples:
            run.error(
                "run_id=%s status=%s source=%s destination=%s error=%s",
                run_id,
                item.status,
                item.source,
                item.destination,
                item.error,
            )
        if summary.input_failed + summary.removal_failed > len(summary.error_samples):
            run.warning(
                "run_id=%s error_samples_truncated shown=%d",
                run_id,
                len(summary.error_samples),
            )
    print(
        f"MiniSEED conversion [{run_id}]: {summary.input_succeeded} succeeded, "
        f"{summary.input_failed} failed, {summary.traces_written} SAC written."
    )
    return summary


def _run_conversion_batches(
    batches, output, remove_original, max_workers, max_error_samples, run
):
    counts = []
    total = sum(len(batch) for batch in batches)
    worker_limit = min(max_error_samples, 1)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _process_batch, batch, output, remove_original, worker_limit
            ): batch
            for batch in batches
        }
        with tqdm(total=total, desc="Converting MiniSEED") as bar:
            for future in as_completed(futures):
                batch = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    result = _failed_batch(batch, exc, worker_limit)
                counts.append(result)
                combined = _combine(counts, max_error_samples)
                bar.update(result.total)
                run.checkpoint(
                    completed=combined.total,
                    total=total,
                    input_total=total,
                    input_completed=combined.total,
                    input_succeeded=combined.succeeded,
                    input_failed=combined.failed,
                    originals_removed=combined.removed,
                    removal_failed=combined.removal_failed,
                    traces_written=combined.traces_written,
                    output_conflicts=combined.conflicts,
                    error_samples=combined.samples,
                    output_dir=output,
                )
    return _combine(counts, max_error_samples)


def _process_batch(files, output, remove_original, limit):
    return _combine(
        [_convert_file(f, output, remove_original, limit) for f in files], limit
    )


def _convert_file(source, output, remove_original, limit):
    created = []
    temporary = None
    destination = None
    try:
        stream = obspy.read(source)
        merge_short_gaps(stream)
        if not len(stream):
            raise ValueError("MiniSEED contains no traces")
        for trace in stream:
            preserve_sac_quality(trace)
            destination = _trace_destination(trace, output)
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                raise FileExistsError(destination)
            temporary = temporary_output_path(destination)
            trace.write(str(temporary), format="SAC")
            commit_output(temporary, destination)
            temporary = None
            created.append(destination)
    except Exception as exc:
        if temporary:
            temporary.unlink(missing_ok=True)
        cleanup_outputs(created)
        conflict = isinstance(exc, FileExistsError)
        sample = (
            (
                Mseed2SacIssue(
                    source,
                    "output_conflict" if conflict else "conversion_failed",
                    f"{type(exc).__name__}: {exc}",
                    destination,
                ),
            )
            if limit
            else ()
        )
        return _Counts(total=1, failed=1, conflicts=int(conflict), samples=sample)
    removed = removal_failed = 0
    samples = ()
    if remove_original:
        try:
            source.unlink()
            removed = 1
        except OSError as exc:
            removal_failed = 1
            samples = (
                (
                    Mseed2SacIssue(
                        source,
                        "original_removal_failed",
                        f"{type(exc).__name__}: {exc}",
                    ),
                )
                if limit
                else ()
            )
    return _Counts(1, 1, 0, removed, removal_failed, len(created), 0, samples)


def _trace_destination(trace, output):
    return WaveformIdentity.from_trace(trace).sac_path(output)


def build_sac_path(
    output: str | Path,
    network: str,
    station: str,
    location: str,
    channel: str,
    quality: str,
    starttime: Any,
) -> Path:
    """Build the canonical SeisPy path for one SAC trace.

    Args:
        output: Root output directory.
        network: Network code.
        station: Station code.
        location: Location code.
        channel: Channel code.
        quality: MiniSEED data-quality code.
        starttime: Trace start time with ``year``, ``julday``, and ``strftime``.

    Returns:
        The destination path; no directory is created.

    Examples:
        ```python
        from obspy import UTCDateTime
        path = build_sac_path(
            "out", "NZ", "WEL", "10", "BHZ", "D",
            UTCDateTime("2025-01-01"),
        )
        path.name.startswith("NZ.WEL.10.BHZ.D.2025.001")
        # => True
        ```
    """
    identity = WaveformIdentity(network, station, location, channel, quality, starttime)
    return identity.sac_path(output)


def _failed_batch(files, exc, limit):
    samples = (
        (Mseed2SacIssue(files[0], "conversion_failed", f"{type(exc).__name__}: {exc}"),)
        if files and limit
        else ()
    )
    return _Counts(total=len(files), failed=len(files), samples=samples)


def _combine(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[: max(0, limit - len(samples))])
    return _Counts(
        sum(x.total for x in items),
        sum(x.succeeded for x in items),
        sum(x.failed for x in items),
        sum(x.removed for x in items),
        sum(x.removal_failed for x in items),
        sum(x.traces_written for x in items),
        sum(x.conflicts for x in items),
        tuple(samples),
    )
