"""Replace numbered horizontal SAC pairs with inventory-oriented N/E pairs."""

from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
import csv
import os

import numpy as np
import obspy
from obspy.core.inventory import Inventory

from seispy.waveform._correction import mark, read_one, write_group
from seispy.waveform.orientation import _directions
from seispy.workflow import commit_output, temporary_output_path

_TIME_TOLERANCE_SAMPLES = 1e-5
_TIME_PRECISION_NS = 1000  # ObsPy SAC timestamps are rounded to microseconds.


@dataclass(frozen=True)
class EventRotationIssue:
    """One failed pair or unmatched/unreadable source file."""

    source: Path
    error: str


@dataclass(frozen=True)
class EventRotationSummary:
    """Pair counts exclude files that could not be paired or inspected."""

    total_pairs: int
    rotated_pairs: int
    failed_pairs: int
    unpaired_files: int
    invalid_files: int
    deletion_failed_pairs: int
    issue_samples: tuple[EventRotationIssue, ...]
    errors_csv: Path | None

    @property
    def has_issues(self):
        return bool(self.failed_pairs or self.unpaired_files or self.invalid_files)


def _destination(path, channel, suffix):
    tokens = path.name.split(".")
    if tokens.count(channel) != 1:
        raise ValueError(f"Filename must contain exactly one {channel} channel token")
    tokens[tokens.index(channel)] = channel[:2] + suffix
    return path.with_name(".".join(tokens))


def _metadata_index(inventory):
    index = defaultdict(list)
    for net in inventory:
        for sta in net:
            for channel in sta:
                index[(net.code, sta.code, channel.location_code, channel.code)].append(
                    (net, sta, channel)
                )
    return index


def _direction(index, trace):
    stats = trace.stats
    candidates = []
    for net, sta, channel in index.get(
        (stats.network, stats.station, stats.location, stats.channel), ()
    ):
        nodes = (net, sta, channel)
        if all(
            (node.start_date is None or node.start_date <= stats.starttime)
            and (node.end_date is None or node.end_date >= stats.endtime)
            for node in nodes
        ):
            candidates.append((sta, channel))
    if len(candidates) != 1:
        raise ValueError(
            f"Expected one full-coverage metadata epoch for {trace.id}; found {len(candidates)}"
        )
    azimuth, dip = _directions(*candidates[0], False, False)
    if abs(dip) > 1:
        raise ValueError(f"Horizontal Dip exceeds 1 degree for {trace.id}: {dip}")
    return azimuth


def _compatible(first, second):
    a, b = first.stats, second.stats
    return (
        a.sampling_rate == b.sampling_rate
        and a.npts == b.npts
        and abs(a.starttime.ns - b.starttime.ns)
        <= max(_TIME_PRECISION_NS, 1e9 * _TIME_TOLERANCE_SAMPLES / a.sampling_rate)
    )


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _rotate_pair(task, index):
    paths, destinations, preflight_error = task
    outputs_complete = False
    deleting = False
    try:
        if preflight_error:
            raise ValueError(preflight_error)
        if any(p.exists() or p.is_symlink() for p in destinations):
            raise FileExistsError("N/E output already exists; originals retained")
        traces = [read_one(p) for p in paths]
        if not _compatible(*traces):
            raise ValueError("Components have different coverage or sample grids")
        angles = np.deg2rad([_direction(index, tr) for tr in traces])
        matrix = np.column_stack((np.cos(angles), np.sin(angles)))
        if np.linalg.cond(matrix) > 1e8:
            raise ValueError("Horizontal directions are singular or ill-conditioned")
        north, east = np.linalg.solve(matrix, np.vstack([tr.data for tr in traces]))
        start = traces[0].stats.starttime
        for tr, data, suffix, azimuth in zip(
            traces, (north, east), "NE", (0, 90), strict=True
        ):
            mark(tr, "orientation")
            tr.data = data
            tr.stats.starttime = start
            tr.stats.channel = tr.stats.channel[:2] + suffix
            tr.stats.sac.kcmpnm = tr.stats.channel
            tr.stats.sac.cmpaz = azimuth
            tr.stats.sac.cmpinc = 90
        write_group(traces, paths[0].parent, False, destinations=destinations)
        outputs_complete = True
        # Durability precedes removing either original. No rollback once deletion starts.
        for path in destinations:
            with path.open("rb") as handle:
                os.fsync(handle.fileno())
        _sync_directory(paths[0].parent)
        deleting = True
        for path in paths:
            path.unlink()
        _sync_directory(paths[0].parent)
        return None, False
    except Exception as exc:
        if outputs_complete and not deleting:
            for path in destinations:
                path.unlink(missing_ok=True)
        detail = f"{type(exc).__name__}: {exc}"
        if deleting:
            detail += (
                "; N/E outputs retained; original deletion/directory sync incomplete"
            )
        return EventRotationIssue(paths[0], detail), deleting


def rotate_event_horizontals(
    src: str | Path,
    inventory: str | Path | Inventory,
    *,
    pattern: str = "*.HH[12].*.sac",
    max_workers: int = 5,
    errors_csv: str | Path | None = None,
) -> EventRotationSummary:
    """Rotate numbered horizontal SAC pairs in place, then remove originals.

    Recursively match a basename glob under src. Pair by parent event directory,
    header network/station/location/channel prefix, sample rate, sample count,
    and start time (tolerance max(1 microsecond, 1e-5 samples)). Both axes
    must match pattern.
    Segments pair one-to-one; overlapping or ambiguous segments fail. No trim,
    interpolation, merging, or resampling is performed. Z and existing N/E
    are untouched. Filename channel tokens must agree with SAC headers.

    Inventory Azimuth/Dip must cover the entire trace; horizontal dip must be
    within 1 degree of zero. Metadata sample rate need not match waveform rate.
    N/E names replace the channel token in the respective 1/2 filenames.
    Existing N/E within a group is a conflict; outputs are never overwritten.

    All staged SAC outputs are verified and committed before deleting inputs.
    Ordinary write errors retain originals. Deletion errors retain completed
    outputs and are reported separately; a crash can leave old/new files together.
    Do not run concurrently with another writer in src. Optional errors_csv
    must live outside src, and is replaced with source,error columns.
    """
    source = Path(src).expanduser().resolve()
    if not source.is_dir():
        raise NotADirectoryError(source)
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers < 1
    ):
        raise ValueError("max_workers must be a positive integer")
    if not pattern or "/" in pattern or "\\" in pattern:
        raise ValueError("pattern must be a non-empty basename glob")
    report = Path(errors_csv).expanduser().resolve() if errors_csv is not None else None
    if report is not None and report.is_relative_to(source):
        raise ValueError("errors_csv must be outside src")
    inv = (
        inventory
        if isinstance(inventory, Inventory)
        else obspy.read_inventory(str(inventory))
    )
    index = _metadata_index(inv)
    groups = defaultdict(lambda: {"1": [], "2": []})
    issues = []
    invalid = unpaired = 0
    for path in sorted(source.rglob(pattern)):
        if not path.is_file():
            continue
        try:
            if path.is_symlink() or path.resolve() != path.absolute():
                raise ValueError("Symlinked inputs are not supported")
            trace = obspy.read(str(path), format="SAC", headonly=True)[0]
            s = trace.stats
            if len(s.channel) != 3 or s.channel[-1] not in "12":
                raise ValueError("Matched file is not a numbered horizontal component")
            _destination(path, s.channel, "N" if s.channel[-1] == "1" else "E")
            key = (path.parent, s.network, s.station, s.location, s.channel[:2])
            groups[key][s.channel[-1]].append((path, trace))
        except Exception as exc:
            invalid += 1
            issues.append(EventRotationIssue(path, f"{type(exc).__name__}: {exc}"))
    # List each event directory once, rather than once for every station group.
    geographic = defaultdict(set)
    for parent in {key[0] for key in groups}:
        for path in parent.iterdir():
            tokens = path.name.split(".")
            if len(tokens) >= 5 and len(tokens[3]) == 3 and tokens[3][-1] in "NE":
                network, station, location, channel = tokens[:4]
                geographic[parent].add(
                    (
                        network,
                        station,
                        "" if location == "--" else location,
                        channel[:2],
                    )
                )
    tasks = []
    for key, axes in groups.items():
        existing = key[1:] in geographic[key[0]]
        overlap = False
        for entries in axes.values():
            ordered = sorted(entries, key=lambda entry: entry[1].stats.starttime.ns)
            if any(
                a[1].stats.endtime >= b[1].stats.starttime
                for a, b in zip(ordered, ordered[1:], strict=False)
            ):
                overlap = True
        used = set()
        for first, one in axes["1"]:
            matches = [(p, tr) for p, tr in axes["2"] if _compatible(one, tr)]
            reverse_unique = (
                len(matches) == 1
                and sum(_compatible(tr, matches[0][1]) for _, tr in axes["1"]) == 1
            )
            if not reverse_unique:
                unpaired += 1
                issues.append(
                    EventRotationIssue(
                        first, "Missing, incompatible, or ambiguous component 2"
                    )
                )
                continue
            second, two = matches[0]
            used.add(second)
            destinations = (
                _destination(first, one.stats.channel, "N"),
                _destination(second, two.stats.channel, "E"),
            )
            error = (
                "Existing N/E files in this group"
                if existing
                else "Overlapping segments"
                if overlap
                else None
            )
            tasks.append(((first, second), destinations, error))
        for second, _ in axes["2"]:
            if second not in used:
                unpaired += 1
                issues.append(
                    EventRotationIssue(
                        second, "Missing, incompatible, or ambiguous component 1"
                    )
                )
    failed = deletion_failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for issue, deletion in pool.map(lambda task: _rotate_pair(task, index), tasks):
            if issue is not None:
                failed += 1
                deletion_failed += int(deletion)
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
    summary = EventRotationSummary(
        len(tasks),
        len(tasks) - failed,
        failed,
        unpaired,
        invalid,
        deletion_failed,
        tuple(issues[:5]),
        report,
    )
    print(
        f"Rotation: {summary.rotated_pairs}/{summary.total_pairs} pairs rotated, {failed} failed, {unpaired} unpaired files, {invalid} invalid files, {deletion_failed} deletion failures"
    )
    for issue in summary.issue_samples:
        print(f"{issue.source}: {issue.error}")
    return summary
