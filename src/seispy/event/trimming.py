"""Trim existing event SAC files on their original sampling grids."""

import csv
import fnmatch
import math
import os
import shutil
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from fractions import Fraction
from pathlib import Path

import numpy as np
import obspy
from obspy.io.sac import SACTrace

from seispy.workflow import (
    commit_output,
    resolve_separate_directory_trees,
    temporary_output_path,
)


@dataclass(frozen=True)
class EventTrimIssue:
    """A source file that could not be read, trimmed or written."""

    source: Path
    error: str


@dataclass(frozen=True)
class EventTrimSummary:
    """File counts; insufficient coverage is a skip, not a failure."""

    total: int
    succeeded: int
    skipped: int
    failed: int
    skipped_by_reason: dict[str, int]
    issue_samples: tuple[EventTrimIssue, ...]
    output_dir: Path
    errors_csv: Path | None

    @property
    def has_issues(self) -> bool:
        return self.failed > 0


def _sample_window(trace, window):
    """Use exact rational arithmetic to avoid including a rounded end sample."""
    header = SACTrace.from_obspy_trace(trace)
    if any(offset is not None for offset in window) and (
        header.o is None or not math.isfinite(header.o)
    ):
        raise ValueError("missing or invalid SAC header: o")
    if header.b is None or not math.isfinite(header.b):
        raise ValueError("missing or invalid SAC header: b")
    if not math.isfinite(trace.stats.delta) or trace.stats.delta <= 0:
        raise ValueError("invalid sampling interval")
    if header.iftype != "itime" or not header.leven:
        raise ValueError("only evenly sampled SAC time series are supported")
    # Match ObsPy's microsecond-normalized SAC sampling interval (e.g. 0.04 s),
    # rather than accumulating float32 delta error over a long window.
    step = Fraction(str(trace.stats.delta))
    origin = (
        Fraction(str(header.o))
        if any(offset is not None for offset in window)
        else Fraction(0)
    )
    begin = Fraction(str(header.b))
    start, end = window
    first, stop = 0, trace.stats.npts
    if start is not None:
        target = origin + Fraction(str(start))
        # An explicit start requires temporal coverage, even when the late
        # first sample would be less than one sample interval from the target.
        if begin - target > Fraction(1, 1_000_000):
            return first, stop, step, "missing_start"
        first = max(0, math.ceil((target - begin) / step))
    if end is not None:
        stop = math.ceil((origin + Fraction(str(end)) - begin) / step)
        if stop > trace.stats.npts:
            return first, stop, step, "missing_end"
    reason = "empty_window" if first >= stop else None
    return first, stop, step, reason


def _trim_one(task):
    source, destination, output, window, overwrite = task
    temporary = None
    try:
        if source.is_symlink():
            raise ValueError("symlinked input files are not supported")
        if not destination.resolve().is_relative_to(output):
            raise ValueError("output path escapes output_dir through a symlink")
        stream = obspy.read(str(source), format="SAC", headonly=True)
        if len(stream) != 1:
            raise ValueError("expected a single-trace SAC file")
        trace = stream[0]
        first, stop, step, reason = _sample_window(trace, window)
        if reason is not None:
            return reason, None
        if not overwrite and (destination.exists() or destination.is_symlink()):
            raise FileExistsError(f"output already exists: {destination}")
        if window == (None, None):
            temporary = temporary_output_path(destination)
            shutil.copyfile(source, temporary)
            commit_output(temporary, destination, overwrite=overwrite)
            return "succeeded", None
        full = obspy.read(str(source), format="SAC")[0]
        if full.stats != trace.stats:
            raise ValueError("source headers changed during processing")
        samples = full.data[first:stop].copy()
        sac = SACTrace.from_obspy_trace(full)
        reference = sac.reftime
        new_begin = sac.b + float(first * step)
        sac.data = samples
        sac.b = new_begin
        temporary = temporary_output_path(destination)
        sac.write(str(temporary))
        check = SACTrace.read(str(temporary), checksize=True)
        picks = ("o", "a", "f", *(f"t{i}" for i in range(10)))
        if (
            check.npts != stop - first
            or check.reftime != reference
            or check.b != float(np.float32(new_begin))
            or check.delta != sac.delta
            or any(getattr(check, key) != getattr(sac, key) for key in picks)
            or not np.array_equal(check.data, samples, equal_nan=True)
            or (check.knetwk, check.kstnm, check.khole, check.kcmpnm)
            != (sac.knetwk, sac.kstnm, sac.khole, sac.kcmpnm)
        ):
            raise ValueError("trimmed SAC failed sample/header verification")
        commit_output(temporary, destination, overwrite=overwrite)
        return "succeeded", None
    except Exception as exc:
        return "failed", EventTrimIssue(source, f"{type(exc).__name__}: {exc}")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def trim_event_waveforms(
    src: str | Path,
    output_dir: str | Path,
    *,
    time_window=(0, 90 * 60),
    reference: str = "origin",
    pattern: str = "*.sac",
    overwrite: bool = False,
    max_workers: int = 5,
    errors_csv: str | Path | None = None,
) -> EventTrimSummary:
    """Trim existing event files to [origin + start, origin + end).

    Numeric offsets in time_window are finite seconds relative to the precise
    SAC origin (reference time plus o); only reference='origin' is supported.
    A None start keeps the original first sample; a None end keeps the original
    last sample, including it. (None, None) copies the original file byte-for-byte
    and does not require o. The default remains (0, 5400).
    Recursively match a basename glob, preserving relative directories and
    filenames in a separate output tree. Inputs are never modified.

    Keep original samples whose times fall inside the half-open window.
    An explicit start requires the original first sample to be no later than
    origin + start (allowing 1 microsecond of timestamp representation error).
    Otherwise skip as missing_start. An explicit end requires every grid sample
    before origin + end; missing trailing samples skip as missing_end. Empty
    resolved windows skip as empty_window. No sample must fall exactly on an
    edge. A complete fixed 5400-second window at 1/25 Hz contains
    5400/135000 samples respectively. Files are handled independently, without
    component alignment, padding, interpolation, resampling or segment merging.

    Preserve the SAC reference and origin/pick offsets; update b/e/npts and
    amplitude statistics. SAC b remains float32, with its inherent time
    representation precision. Standard binary SAC files are supported.
    Output samples and time/identity headers are read back and verified before
    commit. Existing output files fail unless overwrite=True.

    Print a compact summary and up to five errors. Optional errors_csv records
    all failures (not coverage skips) in source,error columns; it must be
    outside the source tree, not collide with waveform output, and is replaced.
    """
    source, output = resolve_separate_directory_trees(src, output_dir)
    if not source.is_dir():
        raise NotADirectoryError(source)
    if reference != "origin":
        raise ValueError("reference must be 'origin'")
    try:
        if isinstance(time_window, (str, bytes)) or len(time_window) != 2:
            raise ValueError
        window = tuple(None if value is None else float(value) for value in time_window)
        if any(x is not None and not math.isfinite(x) for x in window):
            raise ValueError
        if all(x is not None for x in window) and window[0] >= window[1]:
            raise ValueError
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(
            "time_window must contain two offsets, each finite or None, increasing when both are numeric"
        ) from exc
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers < 1
    ):
        raise ValueError("max_workers must be a positive integer")
    if not pattern or "/" in pattern or "\\" in pattern:
        raise ValueError("pattern must be a non-empty basename glob")
    report = Path(errors_csv).expanduser().resolve() if errors_csv is not None else None
    if report is not None and (report.is_relative_to(source) or report == output):
        raise ValueError("errors_csv must be outside src and cannot equal output_dir")
    paths = sorted(
        Path(parent) / name
        for parent, _, files in os.walk(source)
        for name in files
        if fnmatch.fnmatchcase(name, pattern)
    )
    destinations = [output / path.relative_to(source) for path in paths]
    if report in destinations:
        raise ValueError("errors_csv conflicts with a waveform output")
    output.mkdir(parents=True, exist_ok=True)
    tasks = (
        (path, destination, output, window, overwrite)
        for path, destination in zip(paths, destinations, strict=True)
    )
    counts = {
        "succeeded": 0,
        "failed": 0,
        "missing_start": 0,
        "missing_end": 0,
        "empty_window": 0,
    }
    issues = []
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for status, issue in pool.map(_trim_one, tasks):
            counts[status] += 1
            if issue is not None:
                issues.append(issue)
    if report is not None:
        temp = temporary_output_path(report)
        try:
            with temp.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(("source", "error"))
                writer.writerows((issue.source, issue.error) for issue in issues)
            commit_output(temp, report, overwrite=True)
        finally:
            temp.unlink(missing_ok=True)
    skipped = {
        key: counts[key] for key in ("missing_start", "missing_end", "empty_window")
    }
    summary = EventTrimSummary(
        len(paths),
        counts["succeeded"],
        sum(skipped.values()),
        counts["failed"],
        skipped,
        tuple(issues[:5]),
        output,
        report,
    )
    print(
        f"Event trim: {summary.total} files, {summary.succeeded} written, {summary.skipped} skipped, {summary.failed} failed"
    )
    print("Skipped: " + ", ".join(f"{key}={value}" for key, value in skipped.items()))
    for issue in summary.issue_samples:
        print(f"{issue.source}: {issue.error}")
    return summary
