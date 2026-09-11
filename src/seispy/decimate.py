import logging
import os
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import Callable, Sequence

import obspy
import numpy as np
from rose import get_logger
from rose.batch import (
    ReportMixin,
    auto_save_report,
    commit_output,
    create_run_id,
    temporary_output_path,
)
from scipy.signal import resample_poly
from tqdm import tqdm

_LOG_DECIMATE = {
    "name": "decimate",
    "file": "decimate.log",
    "level": logging.INFO,
}
DEFAULT_BATCH_SIZE = 100


@dataclass(frozen=True)
class DecimationResult:
    """A sampled decimation failure."""

    source: Path
    destination: Path
    error: str


@dataclass(frozen=True)
class _WorkerSummary:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    error_samples: tuple[DecimationResult, ...] = ()


@dataclass(frozen=True)
class DecimationSummary(ReportMixin):
    """Summarize a potentially large decimation run.

    This class is returned by :func:`decimate_files`; applications
    normally do not instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of waveform files considered.
        succeeded: Number of files decimated successfully.
        failed: Number of files that failed processing.
        error_samples: Bounded sample of decimation failures.
        output_dir: Output root, or ``None`` when replacing source files.
        remove_original: Whether successful outputs replaced their sources.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        ```python
        summary = decimate_files(...)
        print(f"{summary.succeeded}/{summary.total}")
        ```
    """

    run_id: str
    total: int
    succeeded: int
    failed: int
    error_samples: tuple[DecimationResult, ...]
    output_dir: Path | None
    remove_original: bool
    duration_seconds: float
    report_path: Path | None = None

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)


def decimate_files(
    src_dir: str | Path,
    factors: int | Sequence[int],
    backend: str = "scipy",
    pattern: str = "*.sac",
    max_workers: int = 5,
    *,
    output_dir: str | Path | None = None,
    remove_original: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> DecimationSummary:
    """Decimate matching SAC files in balanced process batches.

    Both adapters accept the same ordered sequence of SAC-compatible integer
    factors. The SciPy adapter applies SAC's symmetric FIR filters with
    delay-compensated polyphase filtering; the SAC adapter invokes ``DECIMATE``.
    By default files keep their names under a sibling ``<src_dir>_decimated``
    directory. ``remove_original=True`` safely replaces
    each source only after a non-empty temporary result has been written.

    Args:
        src_dir: Root directory searched recursively for waveform files.
        factors: One factor or an ordered sequence of factors from 2 through 7.
            The output rate is the input rate divided by their product.
        backend: Processing backend, ``"scipy"`` or ``"sac"``. Both use the
            same SAC FIR coefficients and preserve sample alignment.
        pattern: Recursive file pattern below ``src_dir``.
        max_workers: Maximum number of file-batch worker processes.
        output_dir: Optional output root. A sibling directory is used by default.
        remove_original: Safely replace source files instead of writing a copy.
        max_error_samples: Maximum number of failures retained in the summary.
        save_report: Force JSON report creation on or off.
        batch_size: Maximum files handled by one worker task. With the SAC
            adapter, each batch is handled by one SAC process.

    Returns:
        Processing counts, sampled errors, output location, and run duration.

    Raises:
        NotADirectoryError: If ``src_dir`` does not exist.
        ValueError: If the backend, limits, or output policy is invalid.

    Examples:
        ```python
        summary = decimate_files(
            "data/sac", [5, 5, 4], output_dir="data/decimated",
            remove_original=False,
        )
        summary.remove_original
        # => False
        ```
    """
    started = time.monotonic()
    run_id = create_run_id()
    logger = get_logger(**_LOG_DECIMATE)
    backend = backend.lower()
    src_path = Path(src_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {src_path}")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    values = _normalize_factors(factors)
    worker = _decimation_backend(backend)
    output_path = _resolve_output_dir(src_path, output_dir, remove_original)
    worker_sample_limit = min(max_error_samples, 1)

    logger.info(
        "run_id=%s started backend=%s src_dir=%s output_dir=%s "
        "remove_original=%s factors=%s pattern=%s max_workers=%d batch_size=%d",
        run_id,
        backend,
        src_path,
        output_path,
        remove_original,
        values,
        pattern,
        max_workers,
        batch_size,
    )
    targets = _input_files(src_path, pattern)
    target_batches = tuple(_batched(targets, batch_size))
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for batch in target_batches:
            args = (
                batch,
                values,
                src_path,
                output_path,
                remove_original,
                worker_sample_limit,
            )
            futures[executor.submit(worker, *args)] = batch
        with tqdm(total=len(futures), desc="Decimating file batches") as pbar:
            for future in as_completed(futures):
                batch = futures[future]
                try:
                    results.append(future.result())
                except Exception as exc:
                    results.append(
                        _failed_batch(
                            batch,
                            src_path,
                            output_path,
                            remove_original,
                            exc,
                            worker_sample_limit,
                        )
                    )
                pbar.update(1)

    compact = _combine_batches(results, max_error_samples)
    summary = DecimationSummary(
        run_id=run_id,
        total=compact.total,
        succeeded=compact.succeeded,
        failed=compact.failed,
        error_samples=compact.error_samples,
        output_dir=output_path,
        remove_original=remove_original,
        duration_seconds=round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(summary, "decimate", save_report)
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
        f"Decimation complete [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed."
    )
    return summary


def _normalize_factors(factors):
    values = (factors,) if isinstance(factors, int) else tuple(factors)
    if not values or any(
        isinstance(value, bool) or not isinstance(value, int) or not 2 <= value <= 7
        for value in values
    ):
        raise ValueError("decimation factors must be integers from 2 through 7")
    return values


def _decimation_backend(backend) -> Callable:
    if backend == "scipy":
        return _scipy_decimate_batch
    if backend == "sac":
        return _sac_decimate_batch
    raise ValueError(f"Unknown backend: {backend}")


def _resolve_output_dir(src_path, output_dir, remove_original):
    if remove_original:
        if output_dir is not None:
            raise ValueError("output_dir cannot be used when remove_original=True")
        return None
    destination = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else src_path.with_name(f"{src_path.name}_decimated")
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


def _batched(values, size):
    for start in range(0, len(values), size):
        yield tuple(values[start : start + size])


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
        DecimationResult(
            target,
            _destination_for(target, src_root, output_dir, remove_original),
            error,
        )
        for target in targets[:limit]
    )
    return _WorkerSummary(len(targets), 0, len(targets), samples)


def _scipy_decimate_batch(
    targets,
    factors,
    src_root,
    output_dir,
    remove_original,
    max_error_samples,
):
    succeeded = failed = 0
    samples = []
    for target in targets:
        destination = _destination_for(target, src_root, output_dir, remove_original)
        temporary = temporary_output_path(destination)
        try:
            stream = obspy.read(target)
            for trace in stream:
                _sac_compatible_decimate_trace(trace, factors)
            stream.write(str(temporary), format="SAC")
            commit_output(temporary, destination, overwrite=True)
            succeeded += 1
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            failed += 1
            if len(samples) < max_error_samples:
                samples.append(
                    DecimationResult(
                        target, destination, f"{type(exc).__name__}: {exc}"
                    )
                )
    return _WorkerSummary(len(targets), succeeded, failed, tuple(samples))


@cache
def _sac_fir_coefficients(factor):
    filter_file = _sac_filter_file(factor)
    tokens = filter_file.read_text(encoding="ascii").split()
    half_length = int(tokens[7])
    half = np.asarray(tokens[8:], dtype=np.float64)
    if len(half) != half_length:
        raise RuntimeError(f"invalid SAC decimation filter: {filter_file}")
    return np.concatenate((half[:0:-1], half))


def _sac_filter_file(factor):
    candidates = []
    if sacaux := os.environ.get("SACAUX"):
        candidates.append(Path(sacaux) / "fir" / f"dec{factor}")
    if sachome := os.environ.get("SACHOME"):
        candidates.append(Path(sachome) / "aux" / "fir" / f"dec{factor}")
    candidates.append(Path("/usr/local/sac/aux/fir") / f"dec{factor}")
    for candidate in candidates:
        if candidate.is_file():
            return candidate
    raise FileNotFoundError(
        f"SAC FIR filter dec{factor} was not found; set SACAUX or SACHOME "
        "to a licensed SAC installation"
    )


def _sac_compatible_decimate_trace(trace, factors):
    data = np.asarray(trace.data, dtype=np.float64)
    for factor in factors:
        data = resample_poly(
            data,
            up=1,
            down=factor,
            window=_sac_fir_coefficients(factor),
            padtype="constant",
        )
        trace.stats.sampling_rate /= factor
    trace.data = np.asarray(data, dtype=np.float32)


def _sac_decimate_batch(targets, factors, src_root, output_dir, remove_original, limit):
    environment = os.environ.copy()
    environment["SAC_DISPLAY_COPYRIGHT"] = "0"
    jobs = []
    commands = ["readerr badfile fatal"]
    for target in targets:
        destination = _destination_for(target, src_root, output_dir, remove_original)
        temporary = temporary_output_path(destination)
        jobs.append((target, destination, temporary))
        commands.append(f"r {target}")
        commands.extend(f"decimate {factor}" for factor in factors)
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
            samples.append(DecimationResult(target, destination, error))
    return _WorkerSummary(len(targets), succeeded, failed, tuple(samples))
