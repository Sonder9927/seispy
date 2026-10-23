"""Populate station/event metadata while preserving SAC samples and times."""

from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from functools import lru_cache
import logging
import math
import os
from pathlib import Path

import pandas as pd

from seispy._sac_geometry import sac_geometry
from seispy.progress import progress_bar
from seispy.workflow import BatchRun, BatchSummary, new_run_id
from seispy.waveform._sac_header_io import edit_headers

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SacHeaderIssue:
    """One sampled file failure or existing destination."""

    source: Path
    status: str
    error: str
    destination: Path | None = None


@dataclass(frozen=True)
class SacHeaderSummary(BatchSummary):
    """File totals and bounded issues from a SAC metadata run.

    ``total = succeeded + failed + skipped``. ``unchanged`` is a subset of
    succeeded files whose metadata already matched; in-place runs do not write
    those files. ``output_conflicts`` counts destinations skipped because
    overwrite=False. Event counters describe distinct event path groups, not
    file counts; they are zero for station-only runs. Unmatched event groups
    are counted in events_skipped and their files fail visibly.
    """

    events_total: int
    events_processed: int
    events_skipped: int
    invalid_event_times: int
    total: int
    succeeded: int
    failed: int
    output_conflicts: int
    issue_samples: tuple[SacHeaderIssue, ...]
    output_dir: Path
    skipped: int = 0
    unchanged: int = 0

    @property
    def has_issues(self) -> bool:
        return bool(self.failed or self.invalid_event_times)


@dataclass
class _Counts:
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    skipped: int = 0
    unchanged: int = 0
    output_conflicts: int = 0
    samples: tuple[SacHeaderIssue, ...] = ()

    def add(self, other, limit):
        for name in (
            "total",
            "succeeded",
            "failed",
            "skipped",
            "unchanged",
            "output_conflicts",
        ):
            setattr(self, name, getattr(self, name) + getattr(other, name))
        self.samples += other.samples[: max(0, limit - len(self.samples))]

    def report(self):
        return dict(
            total=self.total,
            succeeded=self.succeeded,
            failed=self.failed,
            skipped=self.skipped,
            unchanged=self.unchanged,
            output_conflicts=self.output_conflicts,
            issue_samples=self.samples,
        )


def format_sac_headers(
    src_dir: str | Path,
    dest_dir: str | Path | None = None,
    events_csv: str | Path | None = None,
    stations_csv: str | Path | None = None,
    pattern: str = "*.sac",
    max_workers: int = 4,
    *,
    overwrite: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> SacHeaderSummary:
    """Fill SAC metadata by copying files or editing source headers in place.

    Args:
        src_dir: Recursively searched binary SAC archive (daily or event files).
        dest_dir: Separate output tree, preserving relative paths and names.
            None edits source headers in place without copying waveform data.
        events_csv: Optional event table: time, latitude, longitude, mag, and
            optional depth/elevation. Match UTC YYYYmmddHHMMSS directories.
            Omit to update station metadata only. Times and picks stay intact,
            including o; this function does not establish a new origin marker.
        stations_csv: Required station table: station, latitude, longitude;
            optional network, elevation and depth. Missing optional values
            preserve existing fields. Event depth is km; station heights and
            burial depths are metres. Identities come from SAC headers.
        pattern: Glob pattern matched recursively relative to src_dir.
        max_workers: Maximum number of I/O threads. One runs directly without
            creating a pool; no multiprocessing main guard is required.
        overwrite: Replace existing output files. False skips and counts them.
            Ignored for in-place updates, which always apply table metadata.
        max_error_samples: Bound the number of issues retained in the report.
        save_report: Write JSON progress/results (True by default); None keeps
            the report only on issues, False disables it.
        save_log: Write a persistent log (True by default). In-place reports
            and logs go under src_dir/logs, otherwise under dest_dir/logs.

    Returns:
        File counts, event matching counts, and bounded sampled issues.

    Notes:
        Supports evenly spaced binary SAC v6/v7 in either byte order. Only
        metadata bytes change; v7 double-precision coordinates are also updated.
        Samples, filenames, identity, reference time and picks are preserved.
        Copy mode stages a temporary copy before committing. In-place mode
        restores saved header/footer bytes on catchable write failures where
        possible; it cannot recover from power loss or forced termination.
        In-place symlinks and multiply linked files are rejected.

    Raises:
        ValueError: Invalid tables/options, overlapping source/output trees,
            or no files matched. Individual file failures are reported instead.
        NotADirectoryError: Source directory does not exist.
    """
    source = Path(src_dir).expanduser().resolve()
    destination = (
        Path(dest_dir).expanduser().resolve() if dest_dir is not None else None
    )
    if not source.is_dir():
        raise NotADirectoryError(source)
    if (
        not isinstance(max_workers, int)
        or isinstance(max_workers, bool)
        or max_workers < 1
    ):
        raise ValueError("max_workers must be a positive integer")
    if not isinstance(max_error_samples, int) or max_error_samples < 0:
        raise ValueError("max_error_samples must be a nonnegative integer")
    if destination is not None and (
        destination == source
        or source in destination.parents
        or destination in source.parents
    ):
        raise ValueError("src_dir and dest_dir must be separate, non-overlapping trees")
    if not pattern or Path(pattern).is_absolute() or ".." in Path(pattern).parts:
        raise ValueError("pattern must be a relative glob within src_dir")
    stations = _load_stations(stations_csv)
    events, invalid_times = _load_events(events_csv)
    files = _discover(source, pattern)
    if not files:
        raise ValueError("No SAC files matched pattern in src_dir")
    groups = (
        {
            _event_key(path, source) or str(path.parent.relative_to(source))
            for path in files
        }
        if events is not None
        else set()
    )
    matched = groups.intersection(events or {})
    event_counts = dict(
        events_total=len(groups),
        events_processed=len(matched),
        events_skipped=len(groups - matched),
        invalid_event_times=invalid_times,
    )
    output = destination or source
    run_id = new_run_id()
    # Per-run cache is bounded and shared by threads, never keyed by filenames.
    geometry = lru_cache(maxsize=8192)(sac_geometry)
    counts = _Counts()
    with BatchRun(
        "format",
        output,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=len(files),
            **event_counts,
            files_processed=0,
            succeeded=0,
            failed=0,
            skipped=0,
            output_conflicts=0,
            unchanged=0,
            issue_samples=(),
            output_dir=output,
        )
        run.info(
            "run_id=%s source=%s output=%s inplace=%s workers=%d overwrite=%s",
            run_id,
            source,
            output,
            destination is None,
            max_workers,
            overwrite,
        )
        if invalid_times:
            run.warning("run_id=%s invalid_event_times=%d", run_id, invalid_times)

        def process(batch):
            return _process_batch(
                batch,
                source,
                destination,
                stations,
                events,
                geometry,
                overwrite,
                max_error_samples,
            )

        with progress_bar(
            total=len(files), desc="Formatting headers", unit="file"
        ) as bar:
            for result in _run_batches(files, process, max_workers):
                counts.add(result, max_error_samples)
                run.checkpoint(
                    completed=counts.total,
                    total=len(files),
                    **event_counts,
                    files_processed=counts.total,
                    **{k: v for k, v in counts.report().items() if k != "total"},
                    output_dir=output,
                )
                bar.update(result.total)
        summary = run.complete(
            SacHeaderSummary(
                run_id=run_id,
                **event_counts,
                **counts.report(),
                output_dir=output,
                duration_seconds=0,
            )
        )
        for issue in summary.issue_samples:
            run.warning(
                "run_id=%s status=%s source=%s destination=%s error=%s",
                run_id,
                issue.status,
                issue.source,
                issue.destination,
                issue.error,
            )
    print(
        f"SAC formatting [{run_id}]: {summary.succeeded} succeeded, "
        f"{summary.failed} failed, {summary.skipped} skipped."
    )
    return summary


def _discover(root, pattern):
    files = []
    # Do not follow directory symlinks or scan our own logs on repeated runs.
    for folder, dirs, names in os.walk(root):
        dirs[:] = sorted(name for name in dirs if name != "logs")
        for name in sorted(names):
            path = Path(folder) / name
            relative = path.relative_to(root)
            candidate = pattern
            while True:
                if relative.match(candidate):
                    files.append(path)
                    break
                if not candidate.startswith("**/"):
                    break
                candidate = candidate[3:]
    return files


def _run_batches(files, process, workers):
    batches = (files[i : i + 64] for i in range(0, len(files), 64))
    if workers == 1 or len(files) <= 64:
        for batch in batches:
            yield process(batch)
        return
    with ThreadPoolExecutor(max_workers=workers) as executor:
        pending = set()
        while True:
            while len(pending) < 2 * workers:
                batch = next(batches, None)
                if batch is None:
                    break
                pending.add(executor.submit(process, batch))
            if not pending:
                break
            done, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in done:
                yield future.result()


def _process_batch(files, root, output, stations, events, geometry, overwrite, limit):
    counts = _Counts()
    for source in files:
        destination = output / source.relative_to(root) if output is not None else None
        try:
            if destination is not None and destination.exists() and not overwrite:
                raise FileExistsError(destination)
            key = _event_key(source, root) if events is not None else None
            if events is not None and key not in events:
                raise ValueError(
                    f"No matching event metadata for {source.relative_to(root)}"
                )
            event = events[key] if events is not None else None

            def updates(header, event=event):
                station = _match_station(
                    stations, header.text("knetwk"), header.text("kstnm")
                )
                return _metadata_updates(header, station, event, geometry)

            unchanged = edit_headers(source, destination, updates, overwrite=overwrite)
            counts.succeeded += 1
            counts.unchanged += int(unchanged)
        except Exception as exc:
            conflict = isinstance(exc, FileExistsError)
            counts.skipped += int(conflict)
            counts.output_conflicts += int(conflict)
            counts.failed += int(not conflict)
            if len(counts.samples) < limit:
                counts.samples += (
                    SacHeaderIssue(
                        source,
                        "output_conflict" if conflict else "format_failed",
                        f"{type(exc).__name__}: {exc}",
                        destination or source,
                    ),
                )
        counts.total += 1
    return counts


def _event_key(path, root):
    directories = (root.name, *path.relative_to(root).parts[:-1])
    return next(
        (
            name
            for name in reversed(directories)
            if len(name) == 14 and name.isascii() and name.isdigit()
        ),
        None,
    )


def _metadata_updates(header, station, event, geometry):
    fields = {"stla": station["latitude"], "stlo": station["longitude"]}
    for column, name in (("elevation", "stel"), ("depth", "stdp")):
        if column in station:
            fields[name] = station[column]
    if event is not None:
        fields.update(evla=event["latitude"], evlo=event["longitude"], mag=event["mag"])
        for column, name in (("elevation", "evel"), ("depth", "evdp")):
            if column in event:
                fields[name] = event[column]
    # Station-only edits leave event metadata intact, but refresh geometry if
    # station coordinates change so distances to an existing event cannot stale.
    if event is not None or any(
        fields[k] != header.number(k) for k in ("stla", "stlo")
    ):
        coordinates = tuple(
            fields.get(k, header.number(k)) for k in ("evla", "evlo", "stla", "stlo")
        )
        fields.update(geometry(*coordinates))
    return fields


def _load_stations(path):
    if path is None:
        raise ValueError("stations_csv is required")
    table = pd.read_csv(
        Path(path).expanduser(),
        dtype={"network": str, "station": str},
        keep_default_na=False,
    )
    _require_columns(table, "stations_csv", {"station", "latitude", "longitude"})
    rows = {}
    for record in table.to_dict("records"):
        name = str(record["station"]).strip()
        network = str(record.get("network", "")).strip() or None
        if not name:
            raise ValueError("stations_csv contains an empty station")
        key = network, name
        if (
            key in rows
            or (network is not None and (None, name) in rows)
            or (network is None and any(station == name for _, station in rows))
        ):
            raise ValueError(f"stations_csv contains ambiguous duplicate station {key}")
        rows[key] = _numeric_metadata(record, "stations_csv", event=False)
    return rows


def _match_station(stations, network, station):
    for key in ((network or None, station), (None, station)):
        if key in stations:
            return stations[key]
    if not network:
        candidates = [row for (_, name), row in stations.items() if name == station]
        if len(candidates) == 1:
            return candidates[0]
    raise KeyError(f"No unique station metadata for {network}.{station}")


def _load_events(path):
    if path is None:
        return None, 0
    table = pd.read_csv(Path(path).expanduser(), keep_default_na=False)
    _require_columns(table, "events_csv", {"time", "latitude", "longitude", "mag"})
    times = pd.to_datetime(table["time"], utc=True, errors="coerce", format="mixed")
    invalid = int(times.isna().sum())
    rows = {}
    for record, timestamp in zip(table.to_dict("records"), times, strict=True):
        if pd.isna(timestamp):
            continue
        key = timestamp.strftime("%Y%m%d%H%M%S")
        if key in rows:
            raise ValueError(
                "events_csv contains duplicate event times at one-second precision"
            )
        rows[key] = _numeric_metadata(record, "events_csv", event=True)
    return rows, invalid


def _numeric_metadata(record, label, *, event):
    required = {"latitude", "longitude", "mag"} if event else {"latitude", "longitude"}
    result = {}
    for key in (*sorted(required), "elevation", "depth"):
        value = record.get(key)
        if value is None or pd.isna(value) or str(value).strip() == "":
            if key in required:
                raise ValueError(f"{label}: missing {key}")
            continue
        try:
            number = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{label}: invalid {key}: {value}") from exc
        if key not in required and (math.isnan(number) or number == -12345):
            continue
        if (
            not math.isfinite(number)
            or number == -12345
            or abs(number) > 3.402823466e38
        ):
            raise ValueError(f"{label}: invalid {key}: {value}")
        if key == "latitude" and abs(number) > 90:
            raise ValueError(f"{label}: latitude must be between -90 and 90")
        if key == "longitude" and abs(number) > 180:
            raise ValueError(f"{label}: longitude must be between -180 and 180")
        result[key] = number
    return result


def _require_columns(frame, name, required):
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{name} is missing columns: {', '.join(sorted(missing))}")
