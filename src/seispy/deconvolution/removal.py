"""Instrument-response deconvolution workflows."""

import logging
import multiprocessing
import math
import os
import subprocess
import tempfile
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

import obspy
import numpy as np
from obspy.core.inventory import Inventory

from seispy.progress import call_with_warnings, progress_bar, resolve_worker_call
from seispy.waveform.integrity import (
    DEFAULT_TRACE_FILTER,
    TraceFilter,
    merge_contiguous_segments,
    split_traces_by_filter,
    trace_header_rejection_reason,
    unusable_sample_reason,
)
from seispy.waveform.decimation import (
    _normalize_factors,
    _sac_compatible_decimate_trace,
)
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    resolve_separate_directory_trees,
    temporary_output_path,
)
from seispy.inventory import analyze_inventory

logger = logging.getLogger(__name__)
IssueStatus = Literal["deconvolution_failed", "trace_filtered"]
PreFilter = tuple[float, float, float, float]
DEFAULT_PRE_FILTER: PreFilter = (0.004, 0.006, 4.0, 5.0)
MAX_BATCH_SIZE = 32
TAPER_MAX_SECONDS = 150.0


@dataclass(frozen=True)
class DeconvolutionIssue:
    """A sampled issue; successful file details are not retained."""

    source: Path
    destination: Path
    status: IssueStatus
    error: str


@dataclass(frozen=True)
class _WorkerSummary:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    issue_samples: tuple[DeconvolutionIssue, ...] = ()
    skipped: int = 0
    traces_filtered: int = 0
    filter_samples: tuple[DeconvolutionIssue, ...] = ()


@dataclass(frozen=True)
class DeconvolutionSummary(BatchSummary):
    """Summarize a batch instrument-response removal run.

    This class is returned by :func:`deconvolve_waveforms`; applications
    normally do not instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of waveform files considered.
        succeeded: Number processed successfully.
        failed: Number that failed deconvolution.
        skipped: Number whose traces were all filtered out before processing.
        traces_filtered: Number of individual traces filtered out.
        issue_samples: Bounded sample of processing failures.
        filter_samples: Bounded sample of filtered traces and their reasons.
        output_dir: Output root.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.
    """

    total: int
    succeeded: int
    failed: int
    issue_samples: tuple[DeconvolutionIssue, ...]
    output_dir: Path
    skipped: int = 0
    traces_filtered: int = 0
    filter_samples: tuple[DeconvolutionIssue, ...] = ()

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)


def deconvolve_waveforms(
    source_dir: str | Path,
    inventory: str | Path | Inventory,
    *,
    output_dir: str | Path | None = None,
    backend: str = "obspy",
    pattern: str = "*.sac",
    max_workers: int = 5,
    batch_size: int | None = None,
    files_per_pool: int | None = 1024,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
    pre_filt: PreFilter = DEFAULT_PRE_FILTER,
    decimate_factors: int | Sequence[int] | None = None,
    trace_filter: TraceFilter = DEFAULT_TRACE_FILTER,
) -> DeconvolutionSummary:
    """Deconvolve every matching waveform in a directory tree.

    Args:
        source_dir: Input waveform tree. Its directory layout has no semantics.
        inventory: Safe StationXML path or ObsPy inventory with responses.
        output_dir: Separate output tree. Defaults to a sibling named with the
            ``_deconvolved`` suffix. Input and output trees cannot overlap.
        backend: Processing backend, ``"obspy"`` or ``"sac"``.
        pattern: Recursive input file pattern. MiniSEED requires ObsPy.
        max_workers: Maximum number of worker processes.
        batch_size: Files submitted per task. By default enough batches are
            created for eight scheduling waves, capped at 32 files each.
        files_per_pool: Maximum input files per pool generation, across all
            workers. Defaults to 1024 as an initial tunable limit; None disables
            recycling. Pools drain and exit before their replacements start.
            This is not a per-file memory limit. Script callers must use a
            __main__ guard because workers use the spawn start method.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains it only when issues occur.
        save_log: Write a persistent run log. Defaults to ``True``.
        pre_filt: Four corner frequencies in hertz. When the upper corners
            exceed Nyquist they are reduced per trace while preserving the
            requested low-frequency corners.
        decimate_factors: Optional factor or ordered factors from 2 through 7.
            After detrending and tapering, decimation with anti-alias filtering
            is applied before instrument-response removal. The final Nyquist
            frequency must stay above the second ``pre_filt`` corner. By
            default, no decimation is performed.
        trace_filter: Skip traces that cannot be processed meaningfully, such
            as single-sample, very short, or constant traces. Filtering is
            counted as skipped, never as a failure.

    Returns:
        Processing counts, sampled issues, output location, and run duration.

    Raises:
        NotADirectoryError: If ``source_dir`` does not exist.
        ValueError: If the backend, limits, or output policy is invalid, or if
            MiniSEED input is selected with the SAC backend.

    Notes:
        The default 150-second taper cap and
        ``pre_filt=(0.004, 0.006, 4.0, 5.0)`` target surface-wave periods up to
        150 seconds and body-wave frequencies up to 2 Hz. With integrated
        decimation, the final Nyquist frequency is checked before response
        removal. High-frequency pre-filter corners are lowered to fit below
        Nyquist when possible. Processing fails if the second corner leaves no
        room for a rolloff below ``0.95 * Nyquist``; the caller must still
        confirm that the adjusted passband covers the scientific frequency
        range of interest.

    Examples:
        ```python
        summary = deconvolve_waveforms(
            "data/sac", "data/metadata/stations.xml",
            output_dir="data/deconvolved",
        )
        summary.output_dir.name
        # => 'deconvolved'
        ```
    """
    run_id = new_run_id()
    backend = backend.lower()
    src_path = Path(source_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {src_path}")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if batch_size is not None and batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    if files_per_pool is not None and (
        isinstance(files_per_pool, bool)
        or not isinstance(files_per_pool, int)
        or files_per_pool < 1
    ):
        raise ValueError("files_per_pool must be a positive integer or None")
    pre_filt = _validate_pre_filt(pre_filt)
    factors = _normalize_decimate_factors(decimate_factors)
    _deconvolution_backend(backend)
    output_path = _resolve_output_dir(src_path, output_dir)
    worker_error_samples = min(max_error_samples, 1)
    targets = _input_files(src_path, pattern)
    if backend == "sac":
        miniseed = _first_miniseed_input(targets)
        if miniseed is not None:
            raise ValueError(
                f'backend="sac" does not support MiniSEED input: {miniseed}; '
                'use backend="obspy"'
            )
    inv = (
        inventory
        if isinstance(inventory, Inventory)
        else obspy.read_inventory(str(inventory))
    )
    response_analysis = analyze_inventory(inv)
    response_analysis.response_suitability.require_safe("response removal")
    total = len(targets)
    actual_batch_size = _batch_size(total, max_workers, batch_size)
    with tempfile.TemporaryDirectory(prefix="seispy-deconvolution-") as temp_dir:
        combined_pz = None
        if backend == "sac":
            combined_pz = _write_combined_sacpz(inv, Path(temp_dir) / "responses.pz")
        with BatchRun(
            "deconvolution",
            output_path,
            run_id=run_id,
            save_report=save_report,
            save_log=save_log,
            logger=logger,
        ) as run:
            run.start(
                total=total,
                succeeded=0,
                failed=0,
                skipped=0,
                traces_filtered=0,
                issue_samples=(),
                filter_samples=(),
                output_dir=output_path,
            )
            run.info(
                "run_id=%s backend=%s source_dir=%s output_dir=%s pattern=%s "
                "max_workers=%d batch_size=%d decimate_factors=%s",
                run_id,
                backend,
                src_path,
                output_path,
                pattern,
                max_workers,
                actual_batch_size,
                factors,
            )
            compact = _run_deconvolution_batches(
                targets,
                inv,
                backend,
                combined_pz,
                src_path,
                output_path,
                worker_error_samples,
                pre_filt,
                factors,
                trace_filter,
                max_workers,
                actual_batch_size,
                max_error_samples,
                total,
                run,
                files_per_pool=files_per_pool,
            )
            summary = run.complete(
                DeconvolutionSummary(
                    run_id=run_id,
                    total=compact.total,
                    succeeded=compact.succeeded,
                    failed=compact.failed,
                    skipped=compact.skipped,
                    traces_filtered=compact.traces_filtered,
                    issue_samples=compact.issue_samples,
                    filter_samples=compact.filter_samples,
                    output_dir=output_path,
                    duration_seconds=0,
                )
            )
            for issue in summary.issue_samples:
                run.error(
                    "run_id=%s status=%s source=%s destination=%s error=%s",
                    run_id,
                    issue.status,
                    issue.source,
                    issue.destination,
                    issue.error,
                )
            if summary.failed > len(summary.issue_samples):
                run.warning(
                    "run_id=%s issue_samples_truncated shown=%d total_issues=%d",
                    run_id,
                    len(summary.issue_samples),
                    summary.failed,
                )
            for filtered in summary.filter_samples:
                run.info(
                    "run_id=%s status=%s source=%s destination=%s reason=%s",
                    run_id,
                    filtered.status,
                    filtered.source,
                    filtered.destination,
                    filtered.error,
                )
    print(
        f"Deconvolution complete [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed, {summary.skipped} skipped."
    )
    return summary


_WORKER_INVENTORY = None
_WORKER_BACKEND = None
_WORKER_COMBINED_PZ = None


def _initialize_deconvolution_worker(inventory, backend, combined_pz):
    global _WORKER_BACKEND, _WORKER_COMBINED_PZ, _WORKER_INVENTORY
    _WORKER_INVENTORY = inventory
    _WORKER_BACKEND = backend
    _WORKER_COMBINED_PZ = combined_pz


def _run_deconvolution_batches(
    targets,
    inventory,
    backend,
    combined_pz,
    src_path,
    output_path,
    worker_error_samples,
    pre_filt,
    factors,
    trace_filter,
    max_workers,
    batch_size,
    max_error_samples,
    total,
    run,
    *,
    files_per_pool=1024,
):
    if not targets:
        return _WorkerSummary()
    accumulator = _SummaryAccumulator(max_error_samples)
    generation_size = files_per_pool or len(targets)
    with progress_bar(total=total, desc="Deconvolving", unit="file") as bar:
        for start in range(0, len(targets), generation_size):
            stop = min(start + generation_size, len(targets))
            target_batches = (
                tuple(targets[index : min(index + batch_size, stop)])
                for index in range(start, stop, batch_size)
            )
            run.info(
                "pool_generation=%d files=%d",
                start // generation_size + 1,
                stop - start,
            )
            with ProcessPoolExecutor(
                max_workers=max_workers,
                mp_context=multiprocessing.get_context("spawn"),
                initializer=_initialize_deconvolution_worker,
                initargs=(inventory, backend, combined_pz),
            ) as executor:
                futures = {}
                exhausted = False
                while futures or not exhausted:
                    while not exhausted and len(futures) < max_workers * 2:
                        try:
                            batch = next(target_batches)
                        except StopIteration:
                            exhausted = True
                            break
                        future = executor.submit(
                            call_with_warnings,
                            _process_worker_batch,
                            batch,
                            src_path,
                            output_path,
                            worker_error_samples,
                            pre_filt,
                            factors,
                            trace_filter,
                        )
                        futures[future] = batch
                    if not futures:
                        continue
                    done, _ = wait(futures, return_when=FIRST_COMPLETED)
                    broken = None
                    for future in done:
                        batch = futures.pop(future)
                        try:
                            result = resolve_worker_call(future.result())
                        except BrokenProcessPool as exc:
                            broken = exc
                            continue
                        except Exception as exc:
                            result = _failed_batch(
                                batch,
                                src_path,
                                output_path,
                                exc,
                                worker_error_samples,
                            )
                        accumulator.add(result)
                        bar.update(result.total)
                    _checkpoint_deconvolution(
                        run, accumulator.snapshot(), total, output_path
                    )
                    if broken is not None:
                        for future in futures:
                            future.cancel()
                        raise broken
    return accumulator.snapshot()


def _process_worker_batch(
    targets,
    src_path,
    output_path,
    limit,
    pre_filt,
    factors,
    trace_filter=DEFAULT_TRACE_FILTER,
):
    if _WORKER_BACKEND == "obspy":
        return _process_obspy_targets(
            targets,
            _WORKER_INVENTORY,
            src_path,
            output_path,
            limit,
            pre_filt,
            factors,
            trace_filter,
        )
    if _WORKER_BACKEND == "sac":
        environment = os.environ.copy()
        environment["SAC_DISPLAY_COPYRIGHT"] = "0"
        return _process_sac_batch(
            targets,
            _WORKER_INVENTORY,
            Path(_WORKER_COMBINED_PZ),
            src_path,
            output_path,
            limit,
            pre_filt,
            environment,
            factors,
            trace_filter,
        )
    raise ValueError(f"Unknown backend: {_WORKER_BACKEND}")


def _batch_size(total, max_workers, requested):
    if requested is not None:
        return requested
    return max(1, min(MAX_BATCH_SIZE, math.ceil(total / (max_workers * 8))))


def _batched(targets, size):
    for start in range(0, len(targets), size):
        yield tuple(targets[start : start + size])


def _checkpoint_deconvolution(run, compact, total, output_path):
    run.checkpoint(
        completed=compact.total,
        total=total,
        succeeded=compact.succeeded,
        failed=compact.failed,
        skipped=compact.skipped,
        traces_filtered=compact.traces_filtered,
        issue_samples=compact.issue_samples,
        filter_samples=compact.filter_samples,
        output_dir=output_path,
    )


class _SummaryAccumulator:
    """Keep counters and bounded samples, never historical worker results."""

    def __init__(self, limit):
        self.limit = limit
        self.counts = dict.fromkeys(
            ("total", "succeeded", "failed", "skipped", "traces_filtered"), 0
        )
        self.issue_samples = []
        self.filter_samples = []

    def add(self, result):
        for name in self.counts:
            self.counts[name] += getattr(result, name)
        self.issue_samples.extend(
            result.issue_samples[: max(0, self.limit - len(self.issue_samples))]
        )
        self.filter_samples.extend(
            result.filter_samples[: max(0, self.limit - len(self.filter_samples))]
        )

    def snapshot(self):
        return _WorkerSummary(
            **self.counts,
            issue_samples=tuple(self.issue_samples),
            filter_samples=tuple(self.filter_samples),
        )


def _combine_batches(batches, limit: int) -> _WorkerSummary:
    accumulator = _SummaryAccumulator(limit)
    for batch in batches:
        accumulator.add(batch)
    return accumulator.snapshot()


def _failed_batch(targets, src_root, output_dir, exc, limit):
    error = f"{type(exc).__name__}: {exc}"
    samples = tuple(
        DeconvolutionIssue(
            target,
            _destination_for(target, src_root, output_dir),
            "deconvolution_failed",
            error,
        )
        for target in targets[:limit]
    )
    return _WorkerSummary(
        total=len(targets), failed=len(targets), issue_samples=samples
    )


def _resolve_output_dir(src_path, output_dir):
    destination = (
        output_dir
        if output_dir is not None
        else src_path.with_name(f"{src_path.name}_deconvolved")
    )
    _, destination = resolve_separate_directory_trees(src_path, destination)
    destination.mkdir(parents=True, exist_ok=True)
    return destination


def _deconvolution_backend(backend) -> Callable:
    if backend == "obspy":
        return _process_obspy_targets
    if backend == "sac":
        return _process_sac_batch
    raise ValueError(f"Unknown backend: {backend}")


def _destination_for(target, src_root, output_dir):
    destination = output_dir / target.relative_to(src_root)
    return (
        destination
        if destination.suffix.lower() == ".sac"
        else destination.with_suffix(".sac")
    )


def _input_files(directory, pattern):
    return tuple(
        sorted(target for target in directory.rglob(pattern) if target.is_file())
    )


def _first_miniseed_input(targets):
    for target in targets:
        if target.suffix.lower() in {".mseed", ".miniseed", ".msd", ".seed"}:
            return target
        if target.suffix.lower() == ".sac":
            continue
        try:
            trace = obspy.read(target, headonly=True)[0]
        except Exception:
            continue
        if getattr(trace.stats, "_format", "").upper() == "MSEED":
            return target
    return None


def _write_combined_sacpz(inventory, destination):
    destination = Path(destination)
    export_inventory = inventory.copy()
    _normalize_sacpz_uncertainties(export_inventory)
    export_inventory.write(str(destination), format="SACPZ")
    contents = destination.read_text(encoding="utf-8")
    lines = contents.splitlines()
    blocks = sum(line.strip().startswith("ZEROS ") for line in lines)
    expected = sum(
        len(station.channels) for network in inventory for station in network
    )
    if blocks < 1:
        raise ValueError("inventory contains no response that can be exported as SACPZ")
    if blocks != expected:
        raise ValueError(
            f"SACPZ export contains {blocks} response blocks for {expected} "
            "channel epochs"
        )
    for field in ("NETWORK", "STATION", "LOCATION", "CHANNEL", "START", "END"):
        count = sum(line.lstrip().startswith(f"* {field}") for line in lines)
        if count != blocks:
            raise ValueError(f"SACPZ response blocks require annotated {field} fields")
    units = [
        line.partition(":")[2].strip().upper()
        for line in lines
        if line.lstrip().startswith("* INPUT UNIT")
    ]
    if len(units) != blocks or any(unit != "M" for unit in units):
        raise ValueError(
            "all SACPZ response blocks must use displacement input units (M)"
        )
    return destination


def _normalize_sacpz_uncertainties(inventory):
    """Make optional GeoNet uncertainty fields acceptable to ObsPy's writer."""
    for network in inventory:
        for station in network:
            for channel in station:
                response = channel.response
                if response is None:
                    continue
                for stage in response.response_stages:
                    for value in (
                        *getattr(stage, "poles", ()),
                        *getattr(stage, "zeros", ()),
                    ):
                        if getattr(value, "lower_uncertainty", None) is None:
                            value.lower_uncertainty = 0.0
                        if getattr(value, "upper_uncertainty", None) is None:
                            value.upper_uncertainty = 0.0


def _process_obspy_targets(
    targets,
    inv,
    src_root,
    output_dir,
    limit,
    pre_filt,
    decimate_factors,
    trace_filter=DEFAULT_TRACE_FILTER,
):
    return _combine_batches(
        (
            _process_one_obspy_target(
                target,
                inv,
                src_root,
                output_dir,
                limit,
                pre_filt,
                decimate_factors,
                trace_filter,
            )
            for target in targets
        ),
        limit,
    )


def _process_one_obspy_target(
    target,
    inv,
    src_root,
    output_dir,
    limit,
    pre_filt,
    decimate_factors,
    trace_filter=DEFAULT_TRACE_FILTER,
):
    succeeded = failed = skipped = filtered_traces = 0
    samples = []
    filter_samples = []
    base_destination = _destination_for(target, src_root, output_dir)
    temporary_outputs = []
    committed_outputs = []
    try:
        stream = obspy.read(target)
        merge_contiguous_segments(stream)
        kept, rejected = split_traces_by_filter(
            stream, trace_filter, low_frequency=pre_filt[0]
        )
        filtered_traces += len(rejected)
        for _, reason in rejected:
            if len(filter_samples) >= limit:
                break
            filter_samples.append(
                DeconvolutionIssue(target, base_destination, "trace_filtered", reason)
            )
        if not kept:
            skipped += 1
            return _WorkerSummary(
                total=1,
                skipped=1,
                traces_filtered=filtered_traces,
                filter_samples=tuple(filter_samples),
            )
        processed = remove_response(
            kept,
            inv,
            pre_filt=pre_filt,
            decimate_factors=decimate_factors,
        )
        destinations = _obspy_destinations(target, processed, src_root, output_dir)
        for trace, destination in zip(processed, destinations, strict=True):
            temporary = temporary_output_path(destination)
            temporary_outputs.append((temporary, destination))
            trace.write(str(temporary), format="SAC")
            _validate_output_trace(trace, temporary)
        for temporary, destination in temporary_outputs:
            commit_output(temporary, destination, overwrite=True)
            committed_outputs.append(destination)
    except Exception as exc:
        for temporary, _ in temporary_outputs:
            temporary.unlink(missing_ok=True)
        for destination in committed_outputs:
            destination.unlink(missing_ok=True)
        failed += 1
        if len(samples) < limit:
            samples.append(
                DeconvolutionIssue(
                    target,
                    base_destination,
                    "deconvolution_failed",
                    f"{type(exc).__name__}: {exc}",
                )
            )

    else:
        succeeded += 1
    return _WorkerSummary(
        total=1,
        succeeded=succeeded,
        failed=failed,
        issue_samples=tuple(samples),
        skipped=skipped,
        traces_filtered=filtered_traces,
        filter_samples=tuple(filter_samples),
    )


def _obspy_destinations(target, stream, src_root, output_dir):
    base = _destination_for(target, src_root, output_dir)
    if len(stream) == 1:
        return [base]
    destinations = []
    for index, trace in enumerate(stream, start=1):
        destinations.append(base.with_name(_segment_sac_name(target, trace, index)))
    if len(set(destinations)) != len(destinations):
        raise ValueError(f"MiniSEED traces produce duplicate output names: {target}")
    return destinations


def _segment_sac_name(target, trace, index):
    """Return a compact, unique name for one continuous output segment."""
    stats = trace.stats
    codes = (
        str(getattr(stats, "network", "")),
        str(getattr(stats, "station", "")),
        str(getattr(stats, "location", "") or "--"),
        str(getattr(stats, "channel", "")),
    )
    trace_id = ".".join(codes) if any(codes) else f"trace{index}"
    stem = target.stem
    if stem != trace_id and not stem.startswith(f"{trace_id}."):
        stem = f"{stem}.{trace_id}"
    day_suffix = stats.starttime.strftime("%Y.%j")
    time_of_day = stats.starttime.strftime("%H%M%S%f")[:-3]
    if stem.endswith(day_suffix):
        timestamp = f"T{time_of_day}"
    else:
        timestamp = f".{stats.starttime.strftime('%Y%j')}T{time_of_day}"
    return f"{stem}{timestamp}.sac"


def _process_sac_batch(
    targets,
    inv,
    combined_pz,
    src_root,
    output_dir,
    limit,
    pre_filt,
    environment,
    decimate_factors,
    trace_filter=DEFAULT_TRACE_FILTER,
):
    """Run a bounded group of files in one SAC process."""
    failed = skipped = 0
    filtered_traces = 0
    samples = []
    filter_samples = []
    prepared = []

    def record(target, destination, status: IssueStatus, exc):
        if len(samples) < limit:
            samples.append(
                DeconvolutionIssue(
                    target,
                    destination,
                    status,
                    f"{type(exc).__name__}: {exc}",
                )
            )

    commands = ["readerr badfile fatal"]
    for target in targets:
        destination = _destination_for(target, src_root, output_dir)
        temporary = temporary_output_path(destination)
        try:
            trace = obspy.read(target, headonly=True)[0]
            reason = trace_header_rejection_reason(
                trace, trace_filter, low_frequency=pre_filt[0]
            )
            if reason is not None:
                temporary.unlink(missing_ok=True)
                filtered_traces += 1
                skipped += 1
                if len(filter_samples) < limit:
                    filter_samples.append(
                        DeconvolutionIssue(
                            target, destination, "trace_filtered", reason
                        )
                    )
                continue
            final_rate = _final_sampling_rate(
                trace.stats.sampling_rate, decimate_factors, pre_filt
            )
            f1, f2, f3, f4 = _effective_pre_filt(pre_filt, final_rate)
            _response_epoch_for_trace(inv, trace)
            taper_width = _sac_taper_width(trace)
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            failed += 1
            record(target, destination, "deconvolution_failed", exc)
            continue
        prepared.append((target, destination, temporary, trace))
        commands.extend(
            (
                f"r {target}",
                f"rmean; rtr; taper type hanning width {taper_width:g}",
            )
        )
        commands.extend(f"decimate {factor}" for factor in decimate_factors)
        commands.extend(
            (
                f"trans from pol s {combined_pz} to none freq "
                f"{f1:g} {f2:g} {f3:g} {f4:g}",
                "mul 1.0e9",
                f"w {temporary}",
            )
        )

    if prepared:
        try:
            completed = subprocess.run(
                ["sac"],
                input=("\n".join((*commands, "q", ""))).encode(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                env=environment,
                check=False,
            )
        except Exception as exc:
            process_error = exc
        else:
            process_error = None
            if completed.returncode:
                detail = completed.stderr.decode(errors="replace").strip()
                process_error = RuntimeError(
                    detail or f"SAC exited with {completed.returncode}"
                )

        if process_error is not None and len(prepared) > 1:
            for _, _, temporary, _ in prepared:
                temporary.unlink(missing_ok=True)
            middle = len(prepared) // 2
            retried = [
                _process_sac_batch(
                    [item[0] for item in group],
                    inv,
                    combined_pz,
                    src_root,
                    output_dir,
                    limit,
                    pre_filt,
                    environment,
                    decimate_factors,
                )
                for group in (prepared[:middle], prepared[middle:])
            ]
            initial = _WorkerSummary(
                total=len(targets) - len(prepared),
                failed=failed,
                issue_samples=tuple(samples),
                skipped=skipped,
                traces_filtered=filtered_traces,
                filter_samples=tuple(filter_samples),
            )
            return _combine_batches([initial, *retried], limit)

        succeeded = 0
        for target, destination, temporary, expected in prepared:
            try:
                if process_error is not None:
                    raise process_error
                _validate_deconvolved_file(
                    temporary,
                    expected=expected,
                    decimate_factors=decimate_factors,
                )
                commit_output(temporary, destination, overwrite=True)
            except Exception as exc:
                temporary.unlink(missing_ok=True)
                failed += 1
                record(target, destination, "deconvolution_failed", exc)
                continue
            succeeded += 1
    else:
        succeeded = 0

    return _WorkerSummary(
        total=len(targets),
        succeeded=succeeded,
        failed=failed,
        issue_samples=tuple(samples),
        skipped=skipped,
        traces_filtered=filtered_traces,
        filter_samples=tuple(filter_samples),
    )


def _response_epoch_for_trace(inv, trace):
    """Select exactly one response epoch covering the complete trace."""
    stats = trace.stats
    selected = inv.select(
        network=stats.network,
        station=stats.station,
        location=getattr(stats, "location", ""),
        channel=stats.channel,
        time=stats.starttime,
    )
    matches = [
        channel for network in selected for station in network for channel in station
    ]
    if len(matches) != 1:
        raise ValueError(
            f"expected one response epoch for {trace.id} at {stats.starttime}, "
            f"found {len(matches)}"
        )
    channel = matches[0]
    if channel.end_date is not None and stats.endtime > channel.end_date:
        raise ValueError(
            f"response changes within {trace.id}: trace ends at {stats.endtime}, "
            f"epoch ends at {channel.end_date}"
        )
    metadata_rate = getattr(channel, "sample_rate", None)
    trace_rate = getattr(stats, "sampling_rate", None)
    if (
        metadata_rate is not None
        and trace_rate is not None
        and not np.isclose(
            float(metadata_rate), float(trace_rate), rtol=1e-7, atol=1e-9
        )
    ):
        raise ValueError(
            f"sample rate mismatch for {trace.id}: waveform={trace_rate}, "
            f"inventory={metadata_rate}"
        )
    selected.get_response(trace.id, stats.starttime)
    return selected, channel


def _validate_deconvolved_file(
    output, *, expected, processed=None, decimate_factors=()
) -> None:
    """Validate output using headers and, when available, in-memory samples."""
    output_trace = obspy.read(output, headonly=True)[0]
    _validate_trace_headers(expected, output_trace, decimate_factors)
    if processed is not None:
        for trace in processed:
            _validate_sample_values(trace.data)
        return

    # SAC maintains these extrema while writing. They provide a constant and
    # non-finite check without reading millions of samples back from disk.
    _validate_sac_extrema(output_trace)


def _validate_output_trace(expected, output) -> None:
    output_trace = obspy.read(output, headonly=True)[0]
    _validate_trace_headers(expected, output_trace)
    _validate_sample_values(expected.data)


def _validate_trace_headers(expected, output, decimate_factors=()) -> None:
    if output.stats.npts <= 0:
        raise ValueError("deconvolved output contains no samples")
    expected_npts = int(expected.stats.npts)
    for factor in decimate_factors:
        expected_npts = (expected_npts + factor - 1) // factor
    if output.stats.npts != expected_npts:
        raise ValueError("deconvolved output sample count differs from input")
    expected_rate = float(expected.stats.sampling_rate)
    for factor in decimate_factors:
        expected_rate /= factor
    if not np.isclose(output.stats.sampling_rate, expected_rate):
        raise ValueError("deconvolved output sampling rate differs from input")
    tolerance = 0.5 / expected_rate
    if abs(output.stats.starttime - expected.stats.starttime) > tolerance:
        raise ValueError("deconvolved output start time differs from input")


def _validate_sac_extrema(trace) -> None:
    sac = getattr(trace.stats, "sac", None)
    depmin = getattr(sac, "depmin", None)
    depmax = getattr(sac, "depmax", None)
    if depmin is not None and depmax is not None:
        if not np.isfinite((depmin, depmax)).all():
            raise ValueError("deconvolved output has non-finite amplitude extrema")
        if depmin == depmax:
            raise ValueError("deconvolved output is constant")


def _validate_sample_values(data) -> None:
    reason = unusable_sample_reason(data)
    if reason is not None:
        raise ValueError(f"deconvolved output {reason}")


def remove_response_from_file(
    file: str | Path,
    inv: str | Path | Inventory,
    pre_filt: PreFilter = DEFAULT_PRE_FILTER,
    decimate_factors: int | Sequence[int] | None = None,
) -> obspy.Stream:
    """Remove the instrument response from one waveform file.

    Args:
        file: Waveform file readable by ObsPy.
        inv: StationXML path or ObsPy inventory containing the matching response.
        pre_filt: Four corner frequencies in hertz. Upper corners are reduced
            when necessary so that the taper finishes below Nyquist.
        decimate_factors: Optional factor or ordered factors from 2 through 7.
            After detrending and tapering, decimation is applied before response
            removal. By default, no decimation is performed.

    Returns:
        The processed ObsPy stream.

    Examples:
        ```python
        stream = remove_response_from_file(
            "trace.sac", inventory, decimate_factors=[5, 5, 4]
        )
        ```
    """
    stream = obspy.read(file)
    merge_contiguous_segments(stream)
    return remove_response(
        stream, inv, pre_filt=pre_filt, decimate_factors=decimate_factors
    )


def remove_response(
    stream: obspy.Stream,
    inv: str | Path | Inventory,
    *,
    pre_filt: PreFilter = DEFAULT_PRE_FILTER,
    decimate_factors: int | Sequence[int] | None = None,
) -> obspy.Stream:
    """Remove the instrument response from every trace in a loaded stream."""
    inventory = obspy.read_inventory(str(inv)) if isinstance(inv, (str, Path)) else inv
    if isinstance(inventory, Inventory):
        analyze_inventory(inventory).response_suitability.require_safe(
            "response removal"
        )
    factors = _normalize_decimate_factors(decimate_factors)
    for tr in stream:
        response_inventory, _ = _response_epoch_for_trace(inventory, tr)
        final_rate = _final_sampling_rate(tr.stats.sampling_rate, factors, pre_filt)
        tr.detrend("demean")
        tr.detrend("linear")
        tr.taper(max_percentage=0.05, max_length=TAPER_MAX_SECONDS, type="hann")
        if factors:
            _sac_compatible_decimate_trace(tr, factors)
        effective_pre_filt = _effective_pre_filt(pre_filt, final_rate)
        tr.remove_response(
            inventory=response_inventory,
            water_level=None,
            pre_filt=effective_pre_filt,
            output="DISP",
            zero_mean=False,
            taper=False,
        )
        tr.data *= 1e9
    return stream


def _sac_taper_width(trace) -> float:
    duration = trace.stats.npts / float(trace.stats.sampling_rate)
    if duration <= 0:
        raise ValueError("trace duration must be positive")
    return min(0.05, TAPER_MAX_SECONDS / duration)


def _validate_pre_filt(pre_filt) -> PreFilter:
    try:
        values = tuple(float(value) for value in pre_filt)
    except (TypeError, ValueError) as exc:
        raise ValueError("pre_filt must contain four numeric frequencies") from exc
    if len(values) != 4 or not all(value > 0 for value in values):
        raise ValueError("pre_filt must contain four positive frequencies")
    if not all(left < right for left, right in zip(values, values[1:], strict=False)):
        raise ValueError("pre_filt frequencies must be strictly increasing")
    return values


def _normalize_decimate_factors(factors) -> tuple[int, ...]:
    if factors is None or factors == ():
        return ()
    return _normalize_factors(factors)


def _final_sampling_rate(sampling_rate, factors, pre_filt) -> float:
    rate = float(sampling_rate)
    if not np.isfinite(rate) or rate <= 0:
        raise ValueError("sampling rate must be positive and finite")
    for factor in factors:
        rate /= factor
    nyquist = rate / 2.0
    f2 = _validate_pre_filt(pre_filt)[1]
    if f2 >= nyquist:
        raise ValueError(
            f"decimation leaves Nyquist at {nyquist:g} Hz, which must exceed "
            f"pre_filt low passband corner {f2:g} Hz"
        )
    _effective_pre_filt(pre_filt, rate)
    return rate


def _effective_pre_filt(pre_filt, sampling_rate: float) -> PreFilter:
    """Keep a frequency taper valid and complete below the trace Nyquist."""
    f1, f2, f3, f4 = _validate_pre_filt(pre_filt)
    nyquist = float(sampling_rate) / 2.0
    if f2 >= nyquist:
        raise ValueError(
            f"pre_filt low passband corner {f2:g} Hz must be below "
            f"Nyquist ({nyquist:g} Hz)"
        )
    if f4 < nyquist:
        return f1, f2, f3, f4

    # Leave headroom below Nyquist and retain a finite high-frequency rolloff.
    f4 = nyquist * 0.95
    if f2 >= f4:
        raise ValueError(
            f"pre_filt low passband corner {f2:g} Hz leaves no room for a "
            f"high-frequency rolloff below 0.95 × Nyquist ({f4:g} Hz)"
        )
    f3 = min(f3, nyquist * 0.80)
    if f3 <= f2:
        f3 = f2 + (f4 - f2) * 0.5
    return f1, f2, f3, f4
