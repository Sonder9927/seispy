import logging
import os
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Sequence

import obspy
from rose import get_logger
from rose.batch import (
    ReportMixin,
    auto_save_report,
    commit_output,
    create_run_id,
    temporary_output_path,
)
from tqdm import tqdm

_LOG_RESAMPLE = {"name": "resample", "file": "resample.log", "level": logging.INFO}
DEFAULT_SAC_BATCH_SIZE = 100


@dataclass(frozen=True)
class ResampleResult:
    """A sampled resampling failure."""

    source: Path
    destination: Path
    error: str


@dataclass(frozen=True)
class _WorkerSummary:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    error_samples: tuple[ResampleResult, ...] = ()


@dataclass(frozen=True)
class ResampleSummary(ReportMixin):
    """Summarize a potentially large resampling run.

    This class is returned by :func:`resample_by_station`; applications
    normally do not instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of waveform files considered.
        succeeded: Number of files resampled successfully.
        failed: Number of files that failed processing.
        error_samples: Bounded sample of resampling failures.
        output_dir: Output root, or ``None`` when replacing source files.
        remove_original: Whether successful outputs replaced their sources.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        >>> summary = resample_by_station(...)
        >>> print(f"{summary.succeeded}/{summary.total}")
    """

    run_id: str
    total: int
    succeeded: int
    failed: int
    error_samples: tuple[ResampleResult, ...]
    output_dir: Path | None
    remove_original: bool
    duration_seconds: float
    report_path: Path | None = None


def resample_by_station(
    src_dir: str | Path,
    delta: float | Sequence[float],
    method: str = "obspy",
    pattern: str = "*.sac",
    max_workers: int = 5,
    *,
    output_dir: str | Path | None = None,
    remove_original: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = None,
    sac_batch_size: int = DEFAULT_SAC_BATCH_SIZE,
) -> ResampleSummary:
    """Resample SAC files grouped by station.

    ObsPy accepts one target sampling rate. SAC accepts one or more sequential
    decimation factors. By default files keep their names under a sibling
    ``<src_dir>_resampled`` directory. ``remove_original=True`` safely replaces
    each source only after a non-empty temporary result has been written.

    Args:
        src_dir: Root directory containing one subdirectory per station.
        delta: Target sampling rate for ObsPy, or SAC decimation factor(s).
        method: Processing backend, ``"obspy"`` or ``"sac"``.
        pattern: Recursive file pattern within each station directory.
        max_workers: Maximum number of station worker processes.
        output_dir: Optional output root. A sibling directory is used by default.
        remove_original: Safely replace source files instead of writing a copy.
        max_error_samples: Maximum number of failures retained in the summary.
        save_report: Force JSON report creation on or off.
        sac_batch_size: Maximum files handled by one SAC process.

    Returns:
        Processing counts, sampled errors, output location, and run duration.

    Raises:
        NotADirectoryError: If ``src_dir`` does not exist.
        ValueError: If the method, limits, or output policy is invalid.

    Examples:
        >>> summary = resample_by_station(
        ...     "data/sac", 1.0, output_dir="data/resampled",
        ...     remove_original=False,
        ... )
        >>> summary.remove_original
        False
    """
    started = time.monotonic()
    run_id = create_run_id()
    logger = get_logger(**_LOG_RESAMPLE)
    method = method.lower()
    src_path = Path(src_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {src_path}")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if method == "sac" and sac_batch_size < 1:
        raise ValueError("sac_batch_size must be at least 1")
    values = _normalize_delta(delta, method)
    worker = _resample_method(method)
    output_path = _resolve_output_dir(src_path, output_dir, remove_original)
    worker_sample_limit = min(max_error_samples, 1)

    logger.info(
        "run_id=%s started method=%s src_dir=%s output_dir=%s "
        "remove_original=%s delta=%s pattern=%s max_workers=%d",
        run_id,
        method,
        src_path,
        output_path,
        remove_original,
        values,
        pattern,
        max_workers,
    )
    stations = sorted(path for path in src_path.iterdir() if path.is_dir())
    batches = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for station in stations:
            args = (
                station,
                pattern,
                values,
                src_path,
                output_path,
                remove_original,
                worker_sample_limit,
            )
            if method == "sac":
                args += (sac_batch_size,)
            futures[executor.submit(worker, *args)] = station
        with tqdm(total=len(futures), desc="Resampling stations") as pbar:
            for future in as_completed(futures):
                station = futures[future]
                try:
                    batches.append(future.result())
                except Exception as exc:
                    batches.append(
                        _failed_batch(
                            _input_files(station, pattern),
                            src_path,
                            output_path,
                            remove_original,
                            exc,
                            worker_sample_limit,
                        )
                    )
                pbar.update(1)

    compact = _combine_batches(batches, max_error_samples)
    summary = ResampleSummary(
        run_id,
        compact.total,
        compact.succeeded,
        compact.failed,
        compact.error_samples,
        output_path,
        remove_original,
        round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(summary, "resample", summary.failed > 0, save_report)
    if summary.report_path:
        logger.info("run_id=%s report=%s", run_id, summary.report_path)
    logger.info(
        "run_id=%s completed total=%d succeeded=%d failed=%d duration_seconds=%.3f",
        run_id,
        summary.total,
        summary.succeeded,
        summary.failed,
        summary.duration_seconds,
    )
    for error in summary.error_samples:
        logger.error(
            "run_id=%s source=%s destination=%s error=%s",
            run_id,
            error.source,
            error.destination,
            error.error,
        )
    if summary.failed > len(summary.error_samples):
        logger.warning(
            "run_id=%s error_samples_truncated shown=%d total_errors=%d",
            run_id,
            len(summary.error_samples),
            summary.failed,
        )
    print(
        f"Resample complete [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed."
    )
    return summary


def _normalize_delta(delta, method):
    values = (
        (float(delta),)
        if isinstance(delta, (int, float))
        else tuple(float(value) for value in delta)
    )
    if not values or any(value <= 0 for value in values):
        raise ValueError("delta must contain positive values")
    if method == "obspy" and len(values) != 1:
        raise ValueError("the obspy method accepts one target sampling rate")
    if method == "sac" and any(
        value != int(value) or not 2 <= value <= 7 for value in values
    ):
        raise ValueError("SAC decimation factors must be integers from 2 through 7")
    return values


def _resample_method(method) -> Callable:
    if method == "obspy":
        return obspy_resample_by_station
    if method == "sac":
        return sac_resample_by_station
    raise ValueError(f"Unknown method: {method}")


def _resolve_output_dir(src_path, output_dir, remove_original):
    if remove_original:
        if output_dir is not None:
            raise ValueError("output_dir cannot be used when remove_original=True")
        return None
    destination = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else src_path.with_name(f"{src_path.name}_resampled")
    )
    if destination == src_path or src_path in destination.parents:
        raise ValueError("output_dir must be outside src_dir")
    destination.mkdir(parents=True, exist_ok=True)
    return destination


def _destination_for(target, src_root, output_dir, remove_original):
    if remove_original:
        return target
    if output_dir is None:
        raise ValueError("output_dir is required when remove_original=False")
    return output_dir / target.relative_to(src_root)


def _input_files(directory, pattern):
    return sorted(path for path in directory.rglob(pattern) if path.is_file())


def _combine_batches(batches, limit):
    samples = []
    for batch in batches:
        samples.extend(batch.error_samples[: max(0, limit - len(samples))])
    return _WorkerSummary(
        sum(x.total for x in batches),
        sum(x.succeeded for x in batches),
        sum(x.failed for x in batches),
        tuple(samples),
    )


def _failed_batch(targets, src_root, output_dir, remove_original, exc, limit):
    error = f"{type(exc).__name__}: {exc}"
    samples = tuple(
        ResampleResult(
            target,
            _destination_for(target, src_root, output_dir, remove_original),
            error,
        )
        for target in targets[:limit]
    )
    return _WorkerSummary(len(targets), 0, len(targets), samples)


def obspy_resample_by_station(
    directory,
    pattern,
    deltas,
    src_root,
    output_dir,
    remove_original,
    max_error_samples,
):
    targets = _input_files(directory, pattern)
    succeeded = failed = 0
    samples = []
    for target in targets:
        destination = _destination_for(target, src_root, output_dir, remove_original)
        temporary = temporary_output_path(destination)
        try:
            stream = obspy.read(target)
            for trace in stream:
                for factor in _decimation_factors(trace.stats.sampling_rate, deltas[0]):
                    trace.decimate(factor, no_filter=False, strict_length=False)
            stream.write(str(temporary), format="SAC")
            commit_output(temporary, destination, overwrite=True)
            succeeded += 1
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            failed += 1
            if len(samples) < max_error_samples:
                samples.append(
                    ResampleResult(target, destination, f"{type(exc).__name__}: {exc}")
                )
    return _WorkerSummary(len(targets), succeeded, failed, tuple(samples))


def sac_resample_by_station(
    directory,
    pattern,
    deltas,
    src_root,
    output_dir,
    remove_original,
    max_error_samples,
    batch_size=DEFAULT_SAC_BATCH_SIZE,
):
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    targets = _input_files(directory, pattern)
    environment = os.environ.copy()
    environment["SAC_DISPLAY_COPYRIGHT"] = "0"
    batches = []
    for start in range(0, len(targets), batch_size):
        batches.append(
            _sac_resample_batch(
                targets[start : start + batch_size],
                deltas,
                src_root,
                output_dir,
                remove_original,
                max_error_samples,
                environment,
            )
        )
    return _combine_batches(batches, max_error_samples)


def _decimation_factors(source_rate, target_rate):
    ratio = float(source_rate) / float(target_rate)
    rounded = round(ratio)
    if rounded < 1 or not abs(ratio - rounded) <= 1e-8 * max(1, rounded):
        raise ValueError(
            f"target sampling rate {target_rate:g} Hz must divide source rate "
            f"{source_rate:g} Hz by an integer"
        )
    remaining = rounded
    factors = []
    for factor in (10, 8, 7, 6, 5, 4, 3, 2):
        while remaining > 1 and remaining % factor == 0:
            factors.append(factor)
            remaining //= factor
    if remaining != 1:
        raise ValueError(f"decimation ratio {rounded} contains a factor larger than 10")
    return tuple(factors)


def _sac_resample_batch(
    targets, deltas, src_root, output_dir, remove_original, limit, environment
):
    jobs = []
    commands = ["readerr badfile fatal"]
    for target in targets:
        destination = _destination_for(target, src_root, output_dir, remove_original)
        temporary = temporary_output_path(destination)
        jobs.append((target, destination, temporary))
        commands.append(f"r {target}")
        commands.extend(f"decimate {int(delta)}" for delta in deltas)
        commands.append(f"w {temporary}")
    commands.append("q")
    try:
        completed = subprocess.run(
            ["sac"],
            input=("\n".join(commands) + "\n").encode(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=environment,
            check=False,
        )
        process_error = None
        if completed.returncode:
            detail = completed.stderr.decode(errors="replace").strip()
            process_error = RuntimeError(
                detail or f"SAC exited with {completed.returncode}"
            )
    except Exception as exc:
        process_error = exc

    succeeded = failed = 0
    samples = []
    for target, destination, temporary in jobs:
        try:
            if process_error is not None:
                raise process_error
            commit_output(temporary, destination, overwrite=True)
            succeeded += 1
            continue
        except Exception as exc:
            error = f"{type(exc).__name__}: {exc}"
        temporary.unlink(missing_ok=True)
        failed += 1
        if len(samples) < limit:
            samples.append(ResampleResult(target, destination, error))
    return _WorkerSummary(len(targets), succeeded, failed, tuple(samples))
