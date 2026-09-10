import logging
import os
import subprocess
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

import obspy
import numpy as np
from tqdm import tqdm

from seispy._waveform import merge_short_gaps
from rose import get_logger
from rose.batch import (
    ReportMixin,
    auto_save_report,
    commit_output,
    create_run_id,
    temporary_output_path,
)

_LOG_DECONVOLUTION = {
    "name": "deconvolution",
    "file": "deconvolution.log",
    "level": logging.INFO,
}
IssueStatus = Literal["deconvolution_failed", "original_removal_failed"]
PreFilter = tuple[float, float, float, float]
DEFAULT_PRE_FILTER: PreFilter = (0.004, 0.006, 4.0, 5.0)
DEFAULT_SAC_BATCH_SIZE = 100
TAPER_MAX_SECONDS = 600.0


@dataclass(frozen=True)
class DeconvolutionResult:
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
    issue_samples: tuple[DeconvolutionResult, ...] = ()


@dataclass(frozen=True)
class DeconvolutionSummary(ReportMixin):
    """Summarize a batch instrument-response removal run.

    This class is returned by :func:`deconvolution_by_station`; applications
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
        >>> summary = deconvolution_by_station(...)
        >>> print(summary.succeeded, summary.failed)
    """

    run_id: str
    total: int
    succeeded: int
    failed: int
    removal_failed: int
    response_conflicts: int
    issue_samples: tuple[DeconvolutionResult, ...]
    output_dir: Path | None
    remove_original: bool
    duration_seconds: float
    report_path: Path | None = None


def deconvolution_by_station(
    src_dir: str | Path,
    resp: str | Path,
    method: str = "obspy",
    pattern: str = "*.sac",
    max_workers: int = 5,
    *,
    output_dir: str | Path | None = None,
    remove_original: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = None,
    pre_filt: PreFilter = DEFAULT_PRE_FILTER,
    sac_batch_size: int = DEFAULT_SAC_BATCH_SIZE,
) -> DeconvolutionSummary:
    """Remove instrument responses from SAC files grouped by station.

    Args:
        src_dir: Root directory containing one subdirectory per station.
        resp: StationXML file containing the matching response metadata.
        method: Processing backend, ``"obspy"`` or ``"sac"``.
        pattern: Recursive file pattern within each station directory.
        max_workers: Maximum number of station worker processes.
        output_dir: Optional output root. A sibling directory is used by default.
        remove_original: Remove each source only after output commits safely.
        max_error_samples: Maximum number of issues retained in the summary.
        save_report: Force JSON report creation on or off.
        pre_filt: Four corner frequencies in hertz. When the upper corners
            exceed Nyquist they are reduced per trace while preserving the
            requested low-frequency corners.
        sac_batch_size: Maximum files handled by one SAC process. This limits
            the failure scope while avoiding one process launch per file. Only
            used when ``method="sac"``.

    Returns:
        Processing counts, sampled issues, output location, and run duration.

    Raises:
        NotADirectoryError: If ``src_dir`` does not exist.
        ValueError: If the method, limits, or output policy is invalid.

    Examples:
        >>> summary = deconvolution_by_station(
        ...     "data/sac", "stations.xml", output_dir="data/deconvolved",
        ...     remove_original=False,
        ... )
        >>> summary.remove_original
        False
    """
    started = time.monotonic()
    run_id = create_run_id()
    logger = get_logger(**_LOG_DECONVOLUTION)
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
    pre_filt = _validate_pre_filt(pre_filt)
    worker = deconv_by_method(method)
    output_path = _resolve_output_dir(src_path, output_dir, remove_original)
    worker_error_samples = min(max_error_samples, 1)
    logger.info(
        "run_id=%s started method=%s src_dir=%s output_dir=%s "
        "remove_original=%s pattern=%s max_workers=%d",
        run_id,
        method,
        src_path,
        output_path,
        remove_original,
        pattern,
        max_workers,
    )

    stations = sorted(path for path in src_path.iterdir() if path.is_dir())
    inv = obspy.read_inventory(str(resp))
    response_conflicts = _preflight_response_conflicts(stations, pattern, inv)
    if response_conflicts:
        logger.warning(
            "run_id=%s response_preflight_conflicts=%d; affected files will fail "
            "without choosing a response arbitrarily",
            run_id,
            response_conflicts,
        )
    batches: list[_WorkerSummary] = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for station in stations:
            try:
                response = _get_response(method, inv, station.name)
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
            if method == "sac":
                worker_args += (sac_batch_size,)
            future = executor.submit(worker, *worker_args)
            futures[future] = station
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
                pbar.update(1)

    compact = _combine_batches(batches, max_error_samples)
    summary = DeconvolutionSummary(
        run_id,
        compact.total,
        compact.succeeded,
        compact.failed,
        compact.removal_failed,
        response_conflicts,
        compact.issue_samples,
        output_path,
        remove_original,
        round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(
        summary,
        "deconvolution",
        summary.failed + summary.removal_failed > 0,
        save_report,
    )
    if summary.report_path:
        logger.info("run_id=%s report=%s", run_id, summary.report_path)
    logger.info(
        "run_id=%s completed total=%d succeeded=%d failed=%d "
        "removal_failed=%d duration_seconds=%.3f",
        run_id,
        summary.total,
        summary.succeeded,
        summary.failed,
        summary.removal_failed,
        summary.duration_seconds,
    )
    for issue in summary.issue_samples:
        logger.error(
            "run_id=%s status=%s source=%s destination=%s error=%s",
            run_id,
            issue.status,
            issue.source,
            issue.destination,
            issue.error,
        )
    issue_total = summary.failed + summary.removal_failed
    if issue_total > len(summary.issue_samples):
        logger.warning(
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
        DeconvolutionResult(
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


def _get_response(method, resp, station):
    if method in {"obspy", "sac"}:
        inv = resp.select(station=station)
        if len(inv):
            return inv
        raise ValueError(f"station={station!r} not found in the inventory")
    raise ValueError(f"Unknown method: {method}")


def deconv_by_method(method) -> Callable:
    if method == "obspy":
        return obspy_deconv
    if method == "sac":
        return sac_deconv
    raise ValueError(f"Unknown method: {method}")


def _destination_for(target, src_root, output_dir, remove_original):
    if remove_original:
        return target.with_suffix(".deconv.sac")
    if output_dir is None:
        raise ValueError("output_dir is required when remove_original=False")
    return output_dir / target.relative_to(src_root)


def _input_files(directory, pattern):
    return sorted(
        target
        for target in directory.rglob(pattern)
        if not target.name.lower().endswith(".deconv.sac")
    )


def _process_targets(targets, src_root, output_dir, remove_original, limit, process):
    succeeded = failed = removal_failed = 0
    samples = []
    for target in targets:
        destination = _destination_for(target, src_root, output_dir, remove_original)
        temporary = temporary_output_path(destination)
        try:
            processed = process(target, temporary)
            _validate_deconvolved_file(target, temporary, processed)
            commit_output(temporary, destination, overwrite=True)
        except Exception as exc:
            temporary.unlink(missing_ok=True)
            failed += 1
            if len(samples) < limit:
                samples.append(
                    DeconvolutionResult(
                        target,
                        destination,
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
                        DeconvolutionResult(
                            target,
                            destination,
                            "original_removal_failed",
                            f"{type(exc).__name__}: {exc}",
                        )
                    )
    return _WorkerSummary(
        len(targets), succeeded, failed, removal_failed, tuple(samples)
    )


def obspy_deconv(
    directory,
    pattern,
    inv,
    src_root,
    output_dir,
    remove_original,
    max_error_samples,
    pre_filt=DEFAULT_PRE_FILTER,
):
    def process(target, temporary):
        stream = stream_removed_response(target, inv, pre_filt=pre_filt)
        stream.write(str(temporary), format="SAC")
        return stream

    return _process_targets(
        _input_files(directory, pattern),
        src_root,
        output_dir,
        remove_original,
        max_error_samples,
        process,
    )


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
):
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")
    targets = _input_files(directory, pattern)
    environment = os.environ.copy()
    environment["SAC_DISPLAY_COPYRIGHT"] = "0"
    with tempfile.TemporaryDirectory(prefix="seispy-sac-pz-") as cache_dir:
        response_cache = {}
        summaries = []
        for start in range(0, len(targets), batch_size):
            summaries.append(
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
                )
            )
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
):
    """Run a bounded group of files in one SAC process."""
    failed = removal_failed = 0
    samples = []
    prepared = []

    def record(target, destination, status: IssueStatus, exc):
        if len(samples) < limit:
            samples.append(
                DeconvolutionResult(
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
            f1, f2, f3, f4 = _effective_pre_filt(pre_filt, trace.stats.sampling_rate)
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
                _validate_deconvolved_file(target, temporary)
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
                trace = obspy.read(target, headonly=True)[0]
            except Exception:
                continue
            try:
                _response_epoch_for_trace(station_inventory, trace)
            except Exception:
                conflicts += 1
    return conflicts


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
        for previous, current in zip(ordered, ordered[1:]):
            if previous.end_date is None or current.start_date is None:
                return True
            if current.start_date <= previous.end_date:
                return True
    return False


def _validate_deconvolved_file(source, output, processed=None) -> None:
    """Validate output using headers and, when available, in-memory samples."""
    source_trace = obspy.read(source, headonly=True)[0]
    output_trace = obspy.read(output, headonly=True)[0]
    if output_trace.stats.npts <= 0:
        raise ValueError("deconvolved output contains no samples")
    if output_trace.stats.npts != source_trace.stats.npts:
        raise ValueError("deconvolved output sample count differs from input")
    if not np.isclose(
        output_trace.stats.sampling_rate, source_trace.stats.sampling_rate
    ):
        raise ValueError("deconvolved output sampling rate differs from input")
    tolerance = 0.5 / float(source_trace.stats.sampling_rate)
    if abs(output_trace.stats.starttime - source_trace.stats.starttime) > tolerance:
        raise ValueError("deconvolved output start time differs from input")
    if processed is not None:
        for trace in processed:
            _validate_sample_values(trace.data)
        return

    # SAC maintains these extrema while writing. They provide a constant and
    # non-finite check without reading millions of samples back from disk.
    sac = getattr(output_trace.stats, "sac", None)
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


def stream_removed_response(
    file: str | Path,
    inv: Any,
    pre_filt: PreFilter = DEFAULT_PRE_FILTER,
) -> obspy.Stream:
    """Remove the instrument response from one waveform file.

    Args:
        file: Waveform file readable by ObsPy.
        inv: ObsPy inventory containing the matching response.
        pre_filt: Four corner frequencies in hertz. Upper corners are reduced
            when necessary so that the taper finishes below Nyquist.

    Returns:
        The processed ObsPy stream.

    Examples:
        >>> stream = stream_removed_response("trace.sac", inventory)
    """
    st = obspy.read(file)
    merge_short_gaps(st)
    for tr in st:
        response_inventory, _ = _response_epoch_for_trace(inv, tr)
        effective_pre_filt = _effective_pre_filt(pre_filt, tr.stats.sampling_rate)
        tr.detrend("demean")
        tr.detrend("linear")
        tr.taper(max_percentage=0.05, max_length=TAPER_MAX_SECONDS, type="hann")
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
    if not all(left < right for left, right in zip(values, values[1:])):
        raise ValueError("pre_filt frequencies must be strictly increasing")
    return values


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
    f3 = min(f3, nyquist * 0.80)
    if f3 <= f2:
        f3 = f2 + (f4 - f2) * 0.5
    return f1, f2, f3, f4
