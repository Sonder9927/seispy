"""Convert MiniSEED waveforms to canonical SAC archive paths."""

import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import obspy
from seispy.archive import WaveformIdentity
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    cleanup_outputs,
    commit_output,
    new_run_id,
    resolve_separate_directory_trees,
    temporary_output_path,
)
from tqdm import tqdm

from seispy.waveform.integrity import merge_short_gaps

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class WaveformConversionIssue:
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
    traces_written: int = 0
    conflicts: int = 0
    samples: tuple[WaveformConversionIssue, ...] = ()


@dataclass(frozen=True)
class WaveformConversionSummary(BatchSummary):
    """Summarize a MiniSEED-to-SAC conversion run.

    This class is returned by :func:`convert_mseed_to_sac`; applications normally do not
    instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of MiniSEED inputs discovered.
        succeeded: Number converted successfully.
        failed: Number that could not be converted.
        traces_written: Number of SAC traces committed to disk.
        output_conflicts: Number of existing destinations not overwritten.
        issue_samples: Bounded sample of conversion issues.
        output_dir: Root directory containing generated SAC files.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        ```python
        summary = convert_mseed_to_sac(...)
        print(summary.traces_written, summary.failed)
        ```
    """

    total: int
    succeeded: int
    failed: int
    traces_written: int
    output_conflicts: int
    issue_samples: tuple[WaveformConversionIssue, ...]
    output_dir: Path

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)


def convert_mseed_to_sac(
    source: str | Path,
    output_dir: str | Path,
    pattern: str = "*.miniseed",
    batch_size: int = 1000,
    max_workers: int = 5,
    *,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> WaveformConversionSummary:
    """Convert a MiniSEED file or directory tree to SAC.

    Args:
        source: Input MiniSEED file or directory.
        output_dir: Destination root for the SAC directory tree.
        pattern: Recursive file pattern used when ``source`` is a directory.
        batch_size: Maximum number of input files assigned to each worker task.
        max_workers: Maximum number of worker processes.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains it only when issues occur.
        save_log: Write a persistent run log. Defaults to ``True``.

    Returns:
        Conversion counts, sampled issues, output location, and run duration.

    Raises:
        FileNotFoundError: If ``source`` does not exist.
        ValueError: If limits are invalid or input and output trees overlap.

    Examples:
        ```python
        summary = convert_mseed_to_sac("data/mseed", "data/sac")
        summary.output_dir.name
        # => 'sac'
        ```
    """
    run_id = new_run_id()
    source = Path(source).expanduser().resolve()
    if not source.exists():
        raise FileNotFoundError(source)
    if batch_size < 1 or max_workers < 1:
        raise ValueError("batch_size and max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if source.is_dir():
        source, output = resolve_separate_directory_trees(source, output_dir)
    else:
        output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    files = [source] if source.is_file() else sorted(source.rglob(pattern))
    files = [path for path in files if path.is_file()]
    batches = [files[i : i + batch_size] for i in range(0, len(files), batch_size)]
    with BatchRun(
        "convert_mseed_to_sac",
        output,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=len(files),
            input_completed=0,
            succeeded=0,
            failed=0,
            traces_written=0,
            output_conflicts=0,
            issue_samples=(),
            output_dir=output,
        )
        run.info("run_id=%s source=%s output=%s", run_id, source, output)
        combined = _run_conversion_batches(
            batches,
            output,
            max_workers,
            max_error_samples,
            run,
        )
        summary = run.complete(
            WaveformConversionSummary(
                run_id=run_id,
                total=combined.total,
                succeeded=combined.succeeded,
                failed=combined.failed,
                traces_written=combined.traces_written,
                output_conflicts=combined.conflicts,
                issue_samples=combined.samples,
                output_dir=output,
                duration_seconds=0,
            )
        )
        for item in summary.issue_samples:
            run.error(
                "run_id=%s status=%s source=%s destination=%s error=%s",
                run_id,
                item.status,
                item.source,
                item.destination,
                item.error,
            )
        if summary.failed > len(summary.issue_samples):
            run.warning(
                "run_id=%s error_samples_truncated shown=%d",
                run_id,
                len(summary.issue_samples),
            )
    print(
        f"MiniSEED conversion [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed, {summary.traces_written} SAC written."
    )
    return summary


def _run_conversion_batches(batches, output, max_workers, max_error_samples, run):
    counts = []
    total = sum(len(batch) for batch in batches)
    worker_limit = min(max_error_samples, 1)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_batch, batch, output, worker_limit): batch
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
                    input_completed=combined.total,
                    succeeded=combined.succeeded,
                    failed=combined.failed,
                    traces_written=combined.traces_written,
                    output_conflicts=combined.conflicts,
                    issue_samples=combined.samples,
                    output_dir=output,
                )
    return _combine(counts, max_error_samples)


def _process_batch(files, output, limit):
    return _combine([_convert_file(f, output, limit) for f in files], limit)


def _convert_file(source, output, limit):
    created = []
    temporary = None
    destination = None
    try:
        stream = obspy.read(source)
        merge_short_gaps(stream)
        if not len(stream):
            raise ValueError("MiniSEED contains no traces")
        for trace in stream:
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
                WaveformConversionIssue(
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
    return _Counts(total=1, succeeded=1, traces_written=len(created))


def _trace_destination(trace, output):
    return WaveformIdentity.from_trace(trace).sac_path(output)


def build_sac_path(
    output: str | Path,
    network: str,
    station: str,
    location: str,
    channel: str,
    starttime: Any,
) -> Path:
    """Build the canonical SeisPy path for one SAC trace.

    Args:
        output: Root output directory.
        network: Network code.
        station: Station code.
        location: Location code.
        channel: Channel code.
        starttime: Trace start time with ``year``, ``julday``, and ``strftime``.

    Returns:
        The destination path; no directory is created.

    Examples:
        ```python
        from obspy import UTCDateTime
        path = build_sac_path(
            "out", "NZ", "WEL", "10", "BHZ",
            UTCDateTime("2025-01-01"),
        )
        path.name.startswith("NZ.WEL.10.BHZ.2025.001")
        # => True
        ```
    """
    identity = WaveformIdentity(network, station, location, channel, starttime)
    return identity.sac_path(output)


def _failed_batch(files, exc, limit):
    samples = (
        (
            WaveformConversionIssue(
                files[0], "conversion_failed", f"{type(exc).__name__}: {exc}"
            ),
        )
        if files and limit
        else ()
    )
    return _Counts(total=len(files), failed=len(files), samples=samples)


def _combine(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[: max(0, limit - len(samples))])
    return _Counts(
        total=sum(x.total for x in items),
        succeeded=sum(x.succeeded for x in items),
        failed=sum(x.failed for x in items),
        traces_written=sum(x.traces_written for x in items),
        conflicts=sum(x.conflicts for x in items),
        samples=tuple(samples),
    )
