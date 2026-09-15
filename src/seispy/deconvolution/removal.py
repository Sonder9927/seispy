"""Instrument-response deconvolution workflows."""

import logging
import os
import subprocess
import tempfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

import obspy
import numpy as np
from obspy.core.inventory import Inventory
from tqdm import tqdm

from seispy.waveform.integrity import merge_contiguous_segments
from seispy.waveform.decimation import (
    _normalize_factors,
    _sac_compatible_decimate_trace,
)
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    temporary_output_path,
)
from seispy.inventory import analyze_inventory

logger = logging.getLogger(__name__)
IssueStatus = Literal["deconvolution_failed", "original_removal_failed"]
PreFilter = tuple[float, float, float, float]
DEFAULT_PRE_FILTER: PreFilter = (0.004, 0.006, 4.0, 5.0)
DEFAULT_SAC_BATCH_SIZE = 100
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
    removal_failed: int = 0
    issue_samples: tuple[DeconvolutionIssue, ...] = ()


@dataclass(frozen=True)
class DeconvolutionSummary(BatchSummary):
    """Summarize a batch instrument-response removal run.

    This class is returned by :func:`remove_instrument_response`; applications
    normally do not instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of waveform files considered.
        succeeded: Number processed successfully.
        failed: Number that failed deconvolution.
        removal_failed: Number of successful outputs whose source removal failed.
        response_conflicts: Number of input files found during preflight to have
            no unique response epoch.
        issue_samples: Bounded sample of processing and removal issues.
        output_dir: Output root, or ``None`` when replacing source files.
        remove_original: Whether successful outputs replace their sources.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        ```python
        summary = remove_instrument_response(...)
        print(summary.succeeded, summary.failed)
        ```
    """

    total: int
    succeeded: int
    failed: int
    removal_failed: int
    response_conflicts: int
    issue_samples: tuple[DeconvolutionIssue, ...]
    output_dir: Path | None
    remove_original: bool

    @property
    def has_issues(self) -> bool:
        return bool(self.failed or self.removal_failed)


def remove_instrument_response(
    net_dir: str | Path,
    resp: str | Path | Inventory,
    backend: str = "obspy",
    pattern: str = "*.sac",
    max_workers: int = 5,
    *,
    output_dir: str | Path | None = None,
    remove_original: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
    pre_filt: PreFilter = DEFAULT_PRE_FILTER,
    sac_batch_size: int = DEFAULT_SAC_BATCH_SIZE,
    decimate_factors: int | Sequence[int] | None = None,
) -> DeconvolutionSummary:
    """Remove responses from station-grouped SAC or MiniSEED waveform files.

    Args:
        net_dir: One network directory containing one subdirectory per station.
        resp: StationXML path or ObsPy inventory containing matching responses.
        backend: Processing backend, ``"obspy"`` or ``"sac"``.
        pattern: Recursive file pattern within each station directory. Use a
            MiniSEED pattern only with ``backend="obspy"``.
        max_workers: Maximum number of station worker processes.
        output_dir: Optional output root. A sibling directory is used by default.
        remove_original: Remove each source only after output commits safely.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains it only when issues occur.
        save_log: Write a persistent run log. Defaults to ``True``.
        pre_filt: Four corner frequencies in hertz. When the upper corners
            exceed Nyquist they are reduced per trace while preserving the
            requested low-frequency corners.
        sac_batch_size: Maximum files handled by one SAC process. This limits
            the failure scope while avoiding one process launch per file. Only
            used when ``backend="sac"``.
        decimate_factors: Optional factor or ordered factors from 2 through 7.
            After detrending and tapering, decimation with anti-alias filtering
            is applied before instrument-response removal. The final Nyquist
            frequency must stay above the second ``pre_filt`` corner. By
            default, no decimation is performed.

    Returns:
        Processing counts, sampled issues, output location, and run duration.

    Raises:
        NotADirectoryError: If ``net_dir`` does not exist.
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
        summary = remove_instrument_response(
            "data/sac/NZ", "data/metadata/stations.xml",
            output_dir="data/deconvolved/NZ",
            remove_original=False,
        )
        summary.remove_original
        # => False
        ```
    """
    run_id = new_run_id()
    backend = backend.lower()
    src_path = Path(net_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {src_path}")
    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if backend == "sac" and sac_batch_size < 1:
        raise ValueError("sac_batch_size must be at least 1")
    pre_filt = _validate_pre_filt(pre_filt)
    factors = _normalize_decimate_factors(decimate_factors)
    worker = _deconvolution_backend(backend)
    output_path = _resolve_output_dir(src_path, output_dir, remove_original)
    worker_error_samples = min(max_error_samples, 1)
    stations = sorted(path for path in src_path.iterdir() if path.is_dir())
    if backend == "sac":
        miniseed = _first_miniseed_input(stations, pattern)
        if miniseed is not None:
            raise ValueError(
                f'backend="sac" does not support MiniSEED input: {miniseed}; '
                'use backend="obspy"'
            )
    inv = resp if isinstance(resp, Inventory) else obspy.read_inventory(str(resp))
    response_analysis = analyze_inventory(inv)
    response_analysis.response_suitability.require_safe("response removal")
    response_conflicts = _preflight_response_conflicts(stations, pattern, inv)
    total = sum(len(_input_files(station, pattern)) for station in stations)
    artifact_root = output_path or src_path
    with BatchRun(
        "deconvolution",
        artifact_root,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=total,
            succeeded=0,
            failed=0,
            removal_failed=0,
            response_conflicts=response_conflicts,
            issue_samples=(),
            output_dir=output_path,
            remove_original=remove_original,
        )
        run.info(
            "run_id=%s backend=%s net_dir=%s output_dir=%s remove_original=%s "
            "pattern=%s max_workers=%d decimate_factors=%s",
            run_id,
            backend,
            src_path,
            output_path,
            remove_original,
            pattern,
            max_workers,
            factors,
        )
        if response_conflicts:
            run.warning(
                "run_id=%s response_preflight_conflicts=%d; affected files will "
                "fail without choosing a response arbitrarily",
                run_id,
                response_conflicts,
            )
        compact = _run_deconvolution_batches(
            stations,
            pattern,
            inv,
            backend,
            worker,
            src_path,
            output_path,
            remove_original,
            worker_error_samples,
            pre_filt,
            sac_batch_size,
            factors,
            max_workers,
            max_error_samples,
            response_conflicts,
            total,
            run,
        )
        summary = run.complete(
            DeconvolutionSummary(
                run_id=run_id,
                total=compact.total,
                succeeded=compact.succeeded,
                failed=compact.failed,
                removal_failed=compact.removal_failed,
                response_conflicts=response_conflicts,
                issue_samples=compact.issue_samples,
                output_dir=output_path,
                remove_original=remove_original,
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
        issue_total = summary.failed + summary.removal_failed
        if issue_total > len(summary.issue_samples):
            run.warning(
                "run_id=%s issue_samples_truncated shown=%d total_issues=%d",
                run_id,
                len(summary.issue_samples),
                issue_total,
            )
    print(
        f"Deconvolution complete [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed, {summary.removal_failed} originals not removed."
    )
    return summary


def _run_deconvolution_batches(
    stations,
    pattern,
    inventory,
    backend,
    worker,
    src_path,
    output_path,
    remove_original,
    worker_error_samples,
    pre_filt,
    sac_batch_size,
    factors,
    max_workers,
    max_error_samples,
    response_conflicts,
    total,
    run,
):
    batches: list[_WorkerSummary] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for station in stations:
            try:
                response = _get_response(backend, inventory, station.name)
            except Exception as exc:
                batches.append(
                    _failed_batch(
                        _input_files(station, pattern),
                        src_path,
                        output_path,
                        remove_original,
                        exc,
                        worker_error_samples,
                    )
                )
                continue
            worker_args = (
                station,
                pattern,
                response,
                src_path,
                output_path,
                remove_original,
                worker_error_samples,
                pre_filt,
            )
            if backend == "sac":
                worker_args += (sac_batch_size, factors)
            else:
                worker_args += (factors,)
            futures[executor.submit(worker, *worker_args)] = station
        initial = _combine_batches(batches, max_error_samples)
        if initial.total:
            _checkpoint_deconvolution(
                run, initial, total, response_conflicts, output_path, remove_original
            )
        with tqdm(total=len(futures), desc="Processing stations") as pbar:
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
                            worker_error_samples,
                        )
                    )
                compact = _combine_batches(batches, max_error_samples)
                _checkpoint_deconvolution(
                    run,
                    compact,
                    total,
                    response_conflicts,
                    output_path,
                    remove_original,
                )
                pbar.update(1)
    return _combine_batches(batches, max_error_samples)


def _checkpoint_deconvolution(
    run, compact, total, response_conflicts, output_path, remove_original
):
    run.checkpoint(
        completed=compact.total,
        total=total,
        succeeded=compact.succeeded,
        failed=compact.failed,
        removal_failed=compact.removal_failed,
        response_conflicts=response_conflicts,
        issue_samples=compact.issue_samples,
        output_dir=output_path,
        remove_original=remove_original,
    )


def _combine_batches(batches, limit: int) -> _WorkerSummary:
    samples = []
    for batch in batches:
        samples.extend(batch.issue_samples[: max(0, limit - len(samples))])
    return _WorkerSummary(
        sum(x.total for x in batches),
        sum(x.succeeded for x in batches),
        sum(x.failed for x in batches),
        sum(x.removal_failed for x in batches),
        tuple(samples),
    )


def _failed_batch(targets, src_root, output_dir, remove_original, exc, limit):
    error = f"{type(exc).__name__}: {exc}"
    samples = tuple(
        DeconvolutionIssue(
            target,
            _destination_for(target, src_root, output_dir, remove_original),
            "deconvolution_failed",
            error,
        )
        for target in targets[:limit]
    )
    return _WorkerSummary(
        total=len(targets), failed=len(targets), issue_samples=samples
    )


def _resolve_output_dir(src_path, output_dir, remove_original):
    if remove_original:
        if output_dir is not None:
            raise ValueError("output_dir cannot be used when remove_original=True")
        return None
    destination = (
        Path(output_dir).expanduser().resolve()
        if output_dir is not None
        else src_path.with_name(f"{src_path.name}_deconv")
    )
    if destination == src_path or src_path in destination.parents:
        raise ValueError("output_dir must be outside src_dir")
    destination.mkdir(parents=True, exist_ok=True)
    return destination


def _get_response(backend, resp, station):
    if backend in {"obspy", "sac"}:
        inv = resp.select(station=station)
        if len(inv):
            return inv
        raise ValueError(f"station={station!r} not found in the inventory")
    raise ValueError(f"Unknown backend: {backend}")


def _deconvolution_backend(backend) -> Callable:
    if backend == "obspy":
        return obspy_deconv
    if backend == "sac":
        return sac_deconv
    raise ValueError(f"Unknown backend: {backend}")


def _destination_for(target, src_root, output_dir, remove_original):
    if remove_original:
        return target.with_suffix(".deconv.sac")
    if output_dir is None:
        raise ValueError("output_dir is required when remove_original=False")
    destination = output_dir / target.relative_to(src_root)
    return (
        destination
        if destination.suffix.lower() == ".sac"
        else destination.with_suffix(".sac")
    )


def _input_files(directory, pattern):
    return sorted(
        target
        for target in directory.rglob(pattern)
        if not target.name.lower().endswith(".deconv.sac")
    )


def _first_miniseed_input(stations, pattern):
    for station in stations:
        for target in _input_files(station, pattern):
            if target.suffix.lower() in {".mseed", ".miniseed", ".msd", ".seed"}:
                return target
            # Unknown extensions are inspected so format, rather than naming,
            # remains authoritative without penalizing normal SAC collections.
            if target.suffix.lower() == ".sac":
                continue
            try:
                trace = obspy.read(target, headonly=True)[0]
            except Exception:
                continue
            if getattr(trace.stats, "_format", "").upper() == "MSEED":
                return target
    return None


def obspy_deconv(
    directory,
    pattern,
    inv,
    src_root,
    output_dir,
    remove_original,
    max_error_samples,
    pre_filt=DEFAULT_PRE_FILTER,
    decimate_factors=(),
):
    return _process_obspy_targets(
        _input_files(directory, pattern),
        inv,
        src_root,
        output_dir,
        remove_original,
        max_error_samples,
        pre_filt,
        decimate_factors,
    )


def _process_obspy_targets(
    targets,
    inv,
    src_root,
    output_dir,
    remove_original,
    limit,
    pre_filt,
    decimate_factors,
):
    succeeded = failed = removal_failed = 0
    samples = []
    for target in targets:
        base_destination = _destination_for(
            target, src_root, output_dir, remove_original
        )
        temporary_outputs = []
        committed_outputs = []
        try:
            # Call through the compatibility name so existing test and plugin
            # seams that patch it continue to affect the workflow.
            stream = remove_response_from_file(
                target,
                inv,
                pre_filt=pre_filt,
                decimate_factors=decimate_factors,
            )
            destinations = _obspy_destinations(
                target, stream, src_root, output_dir, remove_original
            )
            for trace, destination in zip(stream, destinations, strict=True):
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
            continue
        succeeded += 1
        if remove_original:
            try:
                target.unlink()
            except OSError as exc:
                removal_failed += 1
                if len(samples) < limit:
                    samples.append(
                        DeconvolutionIssue(
                            target,
                            base_destination,
                            "original_removal_failed",
                            f"{type(exc).__name__}: {exc}",
                        )
                    )
    return _WorkerSummary(
        len(targets), succeeded, failed, removal_failed, tuple(samples)
    )


def _obspy_destinations(target, stream, src_root, output_dir, remove_original):
    base = _destination_for(target, src_root, output_dir, remove_original)
    if len(stream) == 1:
        return [base]
    destinations = []
    for index, trace in enumerate(stream, start=1):
        destinations.append(
            base.with_name(_segment_sac_name(target, trace, index, remove_original))
        )
    if len(set(destinations)) != len(destinations):
        raise ValueError(f"MiniSEED traces produce duplicate output names: {target}")
    return destinations


def _segment_sac_name(target, trace, index, remove_original):
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
    marker = ".deconv" if remove_original else ""
    return f"{stem}{timestamp}{marker}.sac"


def sac_deconv(
    directory,
    pattern,
    inv,
    src_root,
    output_dir,
    remove_original,
    max_error_samples,
    pre_filt=DEFAULT_PRE_FILTER,
    batch_size=DEFAULT_SAC_BATCH_SIZE,
    decimate_factors=(),
):
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    decimate_factors = _normalize_decimate_factors(decimate_factors)
    targets = _input_files(directory, pattern)
    environment = os.environ.copy()
    environment["SAC_DISPLAY_COPYRIGHT"] = "0"
    with tempfile.TemporaryDirectory(prefix="seispy-sac-pz-") as cache_dir:
        response_cache = {}
        summaries = [
            _process_sac_batch(
                targets[start : start + batch_size],
                inv,
                Path(cache_dir),
                response_cache,
                src_root,
                output_dir,
                remove_original,
                max_error_samples,
                pre_filt,
                environment,
                decimate_factors,
            )
            for start in range(0, len(targets), batch_size)
        ]
    return _combine_batches(summaries, max_error_samples)


def _process_sac_batch(
    targets,
    inv,
    cache_dir,
    response_cache,
    src_root,
    output_dir,
    remove_original,
    limit,
    pre_filt,
    environment,
    decimate_factors,
):
    """Run a bounded group of files in one SAC process."""
    failed = removal_failed = 0
    samples = []
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
        destination = _destination_for(target, src_root, output_dir, remove_original)
        temporary = temporary_output_path(destination)
        try:
            trace = obspy.read(target, headonly=True)[0]
            final_rate = _final_sampling_rate(
                trace.stats.sampling_rate, decimate_factors, pre_filt
            )
            f1, f2, f3, f4 = _effective_pre_filt(pre_filt, final_rate)
            pzs = _sac_pz_for_trace(inv, trace, cache_dir, response_cache)
            taper_width = _sac_taper_width(trace)
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            failed += 1
            record(target, destination, "deconvolution_failed", exc)
            continue
        prepared.append((target, destination, temporary))
        commands.extend(
            (
                f"r {target}",
                f"rmean; rtr; taper type hanning width {taper_width:g}",
            )
        )
        commands.extend(f"decimate {factor}" for factor in decimate_factors)
        commands.extend(
            (
                f"trans from pol s {pzs} to none freq {f1:g} {f2:g} {f3:g} {f4:g}",
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
            for _, _, temporary in prepared:
                temporary.unlink(missing_ok=True)
            middle = len(prepared) // 2
            retried = [
                _process_sac_batch(
                    [item[0] for item in group],
                    inv,
                    cache_dir,
                    response_cache,
                    src_root,
                    output_dir,
                    remove_original,
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
            )
            return _combine_batches([initial, *retried], limit)

        succeeded = 0
        for target, destination, temporary in prepared:
            try:
                if process_error is not None:
                    raise process_error
                _validate_deconvolved_file(
                    target, temporary, decimate_factors=decimate_factors
                )
                commit_output(temporary, destination, overwrite=True)
            except Exception as exc:
                temporary.unlink(missing_ok=True)
                failed += 1
                record(target, destination, "deconvolution_failed", exc)
                continue
            succeeded += 1
            if remove_original:
                try:
                    target.unlink()
                except OSError as exc:
                    removal_failed += 1
                    record(target, destination, "original_removal_failed", exc)
    else:
        succeeded = 0

    return _WorkerSummary(
        len(targets), succeeded, failed, removal_failed, tuple(samples)
    )


def _sac_pz_for_trace(inv, trace, cache_dir, cache):
    """Return one cached SACPZ file selected by trace ID, time, and epoch."""
    selected, channel = _response_epoch_for_trace(inv, trace)
    key = (trace.id, str(channel.start_date), str(channel.end_date))
    cached = cache.get(key)
    if cached is not None:
        return cached

    destination = cache_dir / f"response-{len(cache):04d}.pz"
    selected.write(str(destination), format="SACPZ")
    contents = destination.read_text(encoding="utf-8")
    response_blocks = sum(
        line.strip().startswith("ZEROS ") for line in contents.splitlines()
    )
    input_units = [
        line.partition(":")[2].strip().upper()
        for line in contents.splitlines()
        if line.lstrip().startswith("* INPUT UNIT")
    ]
    if response_blocks != 1:
        destination.unlink(missing_ok=True)
        raise ValueError(f"SACPZ export contains {response_blocks} response blocks")
    if input_units != ["M"]:
        destination.unlink(missing_ok=True)
        unit = input_units[0] if input_units else "unknown"
        raise ValueError(f"SACPZ input unit must be displacement (M), got {unit}")
    cache[key] = destination
    return destination


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


def _preflight_response_conflicts(stations, pattern, inv) -> int:
    """Count affected files only when the inventory contains overlapping epochs."""
    if not _inventory_has_overlapping_epochs(inv):
        return 0
    conflicts = 0
    for station in stations:
        station_inventory = inv.select(station=station.name)
        for target in _input_files(station, pattern):
            try:
                stream = obspy.read(target, headonly=True)
            except Exception:
                continue
            if any(
                _trace_has_response_conflict(station_inventory, trace)
                for trace in stream
            ):
                conflicts += 1
    return conflicts


def _trace_has_response_conflict(inv, trace) -> bool:
    try:
        _response_epoch_for_trace(inv, trace)
    except Exception:
        return True
    return False


def _inventory_has_overlapping_epochs(inv) -> bool:
    groups = {}
    for network in inv:
        for station in network:
            for channel in station:
                key = (
                    network.code,
                    station.code,
                    channel.location_code or "",
                    channel.code,
                )
                groups.setdefault(key, []).append(channel)
    for epochs in groups.values():
        ordered = sorted(
            epochs,
            key=lambda item: (
                float("-inf") if item.start_date is None else item.start_date.timestamp
            ),
        )
        for previous, current in zip(ordered, ordered[1:], strict=False):
            if previous.end_date is None or current.start_date is None:
                return True
            if current.start_date <= previous.end_date:
                return True
    return False


def _validate_deconvolved_file(
    source, output, processed=None, decimate_factors=()
) -> None:
    """Validate output using headers and, when available, in-memory samples."""
    source_trace = obspy.read(source, headonly=True)[0]
    output_trace = obspy.read(output, headonly=True)[0]
    _validate_trace_headers(source_trace, output_trace, decimate_factors)
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
    if np.size(data) == 0:
        raise ValueError("deconvolved output contains no samples")
    if not np.isfinite(data).all():
        raise ValueError("deconvolved output contains NaN or infinite samples")
    if np.all(data == data[0]):
        raise ValueError("deconvolved output is constant")


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
    inventory = obspy.read_inventory(str(inv)) if isinstance(inv, (str, Path)) else inv
    if isinstance(inventory, Inventory):
        analyze_inventory(inventory).response_suitability.require_safe(
            "response removal"
        )
    st = obspy.read(file)
    merge_contiguous_segments(st)
    factors = _normalize_decimate_factors(decimate_factors)
    for tr in st:
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
    return st


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
