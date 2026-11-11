"""Select existing single-trace SAC event files without rewriting samples."""

import csv
import math
import shutil
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import obspy
from obspy.io.sac import SACTrace

from seispy.waveform.integrity import TraceFilter, trace_rejection_reason
from seispy.workflow import commit_output, temporary_output_path


@dataclass(frozen=True)
class EventSelectionIssue:
    """A source file that could not be inspected or copied."""

    source: Path
    error: str


@dataclass(frozen=True)
class EventSelectionSummary:
    """Selection counts; only failures count as issues."""

    total: int
    succeeded: int
    skipped: int
    failed: int
    skipped_by_reason: dict[str, int]
    issue_samples: tuple[EventSelectionIssue, ...]
    output_dir: Path
    errors_csv: Path | None

    @property
    def has_issues(self) -> bool:
        return self.failed > 0


def _distance_bounds(value, maximum):
    if value is None:
        return None
    if len(value) != 2:
        raise ValueError("distance range must contain two bounds")
    low, high = (None if x is None else float(x) for x in value)
    if any(x is not None and (not math.isfinite(x) or x < 0) for x in (low, high)):
        raise ValueError("distance bounds must be finite and non-negative")
    if maximum is not None and any(x is not None and x > maximum for x in (low, high)):
        raise ValueError(f"distance bounds must not exceed {maximum}")
    if low is not None and high is not None and low > high:
        raise ValueError("distance lower bound exceeds upper bound")
    return low, high


def _select_one(task):
    source, destination, conflict, start, end, field, bounds, policy, overwrite = task
    temporary = None
    try:
        if conflict:
            raise ValueError("multiple sources map to the same output path")
        stream = obspy.read(str(source), format="SAC", headonly=policy is None)
        if len(stream) != 1:
            raise ValueError("expected a single-trace SAC file")
        trace = stream[0]
        if start is not None or end is not None:
            header = SACTrace.from_obspy_trace(trace)
            if header.o is None or not math.isfinite(header.o):
                raise ValueError("missing or invalid SAC header: o")
            origin = header.reftime + header.o
            if (start is not None and origin < start) or (
                end is not None and origin >= end
            ):
                return "time", None
        if bounds is not None:
            value = getattr(trace.stats.sac, field, None)
            if value is None or not math.isfinite(value) or value < 0:
                raise ValueError(f"missing or invalid SAC header: {field}")
            if field == "gcarc" and value > 180:
                raise ValueError("invalid SAC header: gcarc exceeds 180 degrees")
            low, high = bounds
            if (low is not None and value < low) or (high is not None and value > high):
                return "distance", None
        if policy is not None and trace_rejection_reason(trace, policy) is not None:
            return "quality", None
        temporary = temporary_output_path(destination)
        shutil.copyfile(source, temporary)
        commit_output(temporary, destination, overwrite=overwrite)
        return "succeeded", None
    except Exception as exc:
        return "failed", EventSelectionIssue(source, f"{type(exc).__name__}: {exc}")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def select_event_waveforms(
    src: str | Path | list[str | Path],
    output_dir: str | Path,
    *,
    pattern: str = "*.sac",
    starttime=None,
    endtime=None,
    distance_km=None,
    distance_deg=None,
    trace_filter: TraceFilter | None = None,
    overwrite: bool = False,
    max_workers: int = 5,
    errors_csv: str | Path | None = None,
) -> EventSelectionSummary:
    """Copy selected event SAC files to a separate directory.

    ``src`` is a directory or a list of directories/files. ``pattern`` is a
    basename glob applied to every file, including explicit list entries.
    Each file must live directly under its event directory; output paths are
    ``output_dir / source.parent.name / source.name``. Duplicate resolved input
    paths are processed once; destination collisions fail all involved inputs.

    Times are UTC event origins (SAC reference time plus ``o``), with an
    inclusive start and exclusive end. Distance ranges have inclusive bounds,
    either of which may be None. ``distance_km`` uses SAC ``dist`` and
    ``distance_deg`` uses ``gcarc``; they are mutually exclusive. Required
    missing metadata is a failure. Conditions are checked in time, distance,
    quality order; only the first rejection is counted. No traces are trimmed.

    Files are copied byte-for-byte. Existing outputs fail unless overwrite is
    True. Output and CSV paths must be separate from input trees. A compact
    summary and up to five errors are printed. Optional errors_csv contains
    all failures in two columns (source,error); no other reports are created.
    """
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    if not pattern or "/" in pattern or "\\" in pattern:
        raise ValueError("pattern must be a non-empty basename glob")
    if trace_filter is not None and not isinstance(trace_filter, TraceFilter):
        raise TypeError("trace_filter must be a TraceFilter or None")
    if distance_km is not None and distance_deg is not None:
        raise ValueError("distance_km and distance_deg are mutually exclusive")
    field = "dist" if distance_km is not None else "gcarc"
    bounds = _distance_bounds(
        distance_km if distance_km is not None else distance_deg,
        None if field == "dist" else 180,
    )
    start = obspy.UTCDateTime(starttime) if starttime is not None else None
    end = obspy.UTCDateTime(endtime) if endtime is not None else None
    if start is not None and end is not None and start >= end:
        raise ValueError("starttime must precede endtime")
    entries = [src] if isinstance(src, (str, Path)) else list(src)
    roots = [Path(p).expanduser().resolve() for p in entries]
    output = Path(output_dir).expanduser().resolve()
    report = Path(errors_csv).expanduser().resolve() if errors_csv is not None else None
    files = set()
    for root in roots:
        if not root.exists():
            raise FileNotFoundError(root)
        protected = root if root.is_dir() else root.parent
        if output.is_relative_to(protected) or protected.is_relative_to(output):
            raise ValueError("output_dir must be separate from input trees")
        if report is not None and (
            report.is_relative_to(protected) or report == output
        ):
            raise ValueError(
                "errors_csv must be separate from input trees and output_dir"
            )
        candidates = root.rglob(pattern) if root.is_dir() else [root]
        for path in candidates:
            if path.is_file() and path.match(pattern):
                resolved = path.resolve()
                if resolved.parent != path.absolute().parent:
                    raise ValueError(f"symlinked input files are not supported: {path}")
                files.add(resolved)
    sources = sorted(files)
    destinations = [output / p.parent.name / p.name for p in sources]
    if report in destinations:
        raise ValueError("errors_csv conflicts with a waveform output")
    counts = Counter(destinations)
    tasks = (
        (p, dest, counts[dest] > 1, start, end, field, bounds, trace_filter, overwrite)
        for p, dest in zip(sources, destinations, strict=True)
    )
    totals = Counter()
    issues = []
    output.mkdir(parents=True, exist_ok=True)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for status, issue in pool.map(_select_one, tasks):
            totals[status] += 1
            if issue is not None:
                issues.append(issue)
    if report is not None:
        temporary = temporary_output_path(report)
        try:
            with temporary.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(("source", "error"))
                writer.writerows((x.source, x.error) for x in issues)
            commit_output(temporary, report, overwrite=True)
        finally:
            temporary.unlink(missing_ok=True)
    skipped = {key: totals[key] for key in ("time", "distance", "quality")}
    summary = EventSelectionSummary(
        len(sources),
        totals["succeeded"],
        sum(skipped.values()),
        totals["failed"],
        skipped,
        tuple(issues[:5]),
        output,
        report,
    )
    print(
        f"Selection: {summary.total} files, {summary.succeeded} copied, "
        f"{summary.skipped} skipped, {summary.failed} failed"
    )
    print("Skipped: " + ", ".join(f"{key}={value}" for key, value in skipped.items()))
    for issue in summary.issue_samples:
        print(f"{issue.source}: {issue.error}")
    return summary
