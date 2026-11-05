"""Private batch and output support for waveform corrections."""

from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import obspy
from obspy.core.inventory import Inventory
from obspy.io.sac import SACTrace

from seispy._pool import process_batches, validate_files_per_pool
from seispy.archive import WaveformIdentity
from seispy.progress import resolve_worker_call
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    resolve_separate_directory_trees,
    temporary_output_path,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CorrectionIssue:
    station: str
    source: Path
    error: str


@dataclass(frozen=True)
class CorrectionSummary(BatchSummary):
    """Counters count files for clocks and station/location/day or event groups for rotation."""

    total: int
    succeeded: int
    failed: int
    skipped: int
    files_written: int
    ignored: int
    issue_samples: tuple[CorrectionIssue, ...]
    output_dir: Path

    @property
    def has_issues(self) -> bool:
        return bool(self.failed)


def inputs(
    source_dir,
    output_dir,
    inventory,
    pattern,
    max_workers,
    files_per_pool,
    max_error_samples,
    layout="archive",
):
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers < 1
    ):
        raise ValueError("max_workers must be a positive integer")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    if layout not in {"archive", "event"}:
        raise ValueError("layout must be archive or event")
    validate_files_per_pool(files_per_pool)
    source, output = resolve_separate_directory_trees(source_dir, output_dir)
    if not source.is_dir():
        raise NotADirectoryError(source)
    files = tuple(sorted(p for p in source.rglob(pattern) if p.is_file()))
    if layout == "event":
        files = tuple(
            p
            for p in files
            if p.relative_to(source).parts[0] != "logs"
            and not p.relative_to(source).parts[0].startswith(".")
        )
        if any(len(p.relative_to(source).parts) < 2 for p in files):
            raise ValueError("event layout requires files inside event subdirectories")
    if not files:
        raise ValueError("No waveform files matched")
    inv = (
        inventory
        if isinstance(inventory, Inventory)
        else obspy.read_inventory(str(inventory))
    )
    return source, output, files, inv


def read_one(path, *, headonly=False):
    stream = obspy.read(str(path), headonly=headonly)
    if len(stream) != 1:
        raise ValueError("Expected one trace per input SAC file")
    tr = stream[0]
    if not headonly and (not len(tr) or not np.isfinite(tr.data).all()):
        raise ValueError("Empty or non-finite waveform")
    return tr


def epoch(inv, tr):
    """Resolve one full-coverage NSLC epoch, retaining the parent Station."""
    matches = []
    for net in inv.select(
        network=tr.stats.network,
        station=tr.stats.station,
        location=tr.stats.location,
        channel=tr.stats.channel,
        time=tr.stats.starttime,
    ):
        for sta in net:
            for ch in sta:
                bounds = [v for v in (sta.end_date, ch.end_date) if v is not None]
                if any(tr.stats.endtime > end for end in bounds):
                    continue
                matches.append((sta, ch))
    if len(matches) != 1:
        raise ValueError(
            f"Expected one full-coverage metadata epoch for {tr.id}; found {len(matches)}"
        )
    return matches[0]


def value(node, key, default=None):
    return getattr(node, "extra", {}).get(key, {}).get("value", default)


def mark(tr, operation):
    slot, marker = ("kuser0", "SPCLK") if operation == "clock" else ("kuser1", "SPORI")
    existing = getattr(tr.stats.get("sac", {}), slot, "")
    if existing == marker:
        raise ValueError(f"{operation} correction already applied to {tr.id}")
    if existing and existing != "-12345":
        raise ValueError(
            f"SAC {slot} is occupied; preserve this field before correcting"
        )
    if "sac" not in tr.stats:
        tr.stats.sac = {}
    tr.stats.sac[slot] = marker


def write_group(traces, output, overwrite, *, destinations=None):
    """Stage and verify all outputs, then commit with rollback on ordinary errors."""
    if destinations is None:
        destinations = [WaveformIdentity.from_trace(t).sac_path(output) for t in traces]
    if len(destinations) != len(traces):
        raise ValueError("Expected one destination per output trace")
    if len(set(destinations)) != len(destinations):
        raise ValueError("Several output segments map to the same canonical filename")
    if not overwrite and any(p.exists() for p in destinations):
        raise FileExistsError(
            "Output already exists; use a new directory or overwrite=True"
        )
    staged, backups, committed = [], {}, []
    try:
        for tr, target in zip(traces, destinations, strict=True):
            temp = temporary_output_path(target)
            staged.append(temp)
            sac = SACTrace.from_obspy_trace(tr)
            relative_start = (tr.stats.starttime.ns - sac.reftime.ns) / 1e9
            # Bypass UTCDateTime.__sub__'s default microsecond rounding. Rebase
            # distant SAC references only when float32 b cannot preserve time.
            if abs(float(np.float32(relative_start)) - relative_start) > 5e-7:
                old_reference = sac.reftime
                picks = {
                    key: getattr(sac, key)
                    for key in ("a", "o", "f", *(f"t{i}" for i in range(10)))
                    if getattr(sac, key) is not None
                }
                sac.reftime = tr.stats.starttime
                shift = (old_reference.ns - sac.reftime.ns) / 1e9
                for key, pick in picks.items():
                    setattr(sac, key, pick + shift)
                sac.iztype = "ib"
                relative_start = (tr.stats.starttime.ns - sac.reftime.ns) / 1e9
            sac.b = relative_start
            sac.write(str(temp))
            check = read_one(temp)
            expected = np.asarray(tr.data, dtype=np.float32)
            if (
                check.id != tr.id
                or check.stats.npts != tr.stats.npts
                or abs(check.stats.starttime.ns - tr.stats.starttime.ns) > 1000
                or check.stats.delta != tr.stats.delta
                or not np.array_equal(check.data, expected)
            ):
                raise ValueError(f"Written SAC failed sample/time validation: {target}")
            if target.exists():
                backup = temporary_output_path(target)
                shutil.copyfile(target, backup)
                backups[target] = backup
        for temp, target in zip(staged, destinations, strict=True):
            commit_output(temp, target, overwrite=overwrite)
            committed.append(target)
    except BaseException:
        for target in reversed(committed):
            if target in backups:
                os.replace(backups[target], target)
            else:
                target.unlink(missing_ok=True)
        raise
    finally:
        for p in [*staged, *backups.values()]:
            p.unlink(missing_ok=True)
    return len(destinations)


def failure(paths, exc):
    return dict(
        status="failed",
        files_written=0,
        issue=CorrectionIssue("", Path(paths[0]), f"{type(exc).__name__}: {exc}"),
    )


def run(
    tasks,
    worker,
    args,
    *,
    operation,
    source,
    output,
    max_workers,
    files_per_pool,
    max_error_samples,
    ignored,
    initial_issues,
    save_report,
    save_log,
):
    if not tasks and not initial_issues:
        raise ValueError("No selected seismic channels found")
    counts = dict(succeeded=0, failed=len(initial_issues), skipped=0, files_written=0)
    issues = list(initial_issues[:max_error_samples])
    total = len(tasks) + len(initial_issues)
    run_id = new_run_id()
    with BatchRun(
        operation,
        output,
        src_path=source,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as batch_run:
        batch_run.start(total=total, **counts, ignored=ignored, issue_samples=issues)

        def consume(results):
            for result in results:
                counts[result["status"]] += 1
                counts["files_written"] += result["files_written"]
                if "issue" in result:
                    batch_run.error("%s", result["issue"].error)
                    if len(issues) < max_error_samples:
                        issues.append(result["issue"])
                batch_run.checkpoint(
                    total=total,
                    completed=sum(
                        counts[k] for k in ("succeeded", "failed", "skipped")
                    ),
                    **counts,
                    ignored=ignored,
                    issue_samples=issues,
                )

        if max_workers == 1:
            for task in tasks:
                consume(worker((task,), *args))
        else:
            with process_batches(
                tasks,
                worker,
                args,
                max_workers=max_workers,
                files_per_pool=files_per_pool,
                run=batch_run,
                operation=operation,
            ) as pending:
                for task_batch, future in pending:
                    try:
                        results = resolve_worker_call(future.result())
                    except Exception as exc:
                        results = [failure(task["paths"], exc) for task in task_batch]
                    consume(results)
        return batch_run.complete(
            CorrectionSummary(
                run_id=run_id,
                duration_seconds=0,
                total=total,
                **counts,
                ignored=ignored,
                issue_samples=tuple(issues),
                output_dir=output,
            )
        )
