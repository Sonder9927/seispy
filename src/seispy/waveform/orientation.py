"""Inventory-driven, interpolation-free horizontal or three-dimensional rotation."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Sequence

import numpy as np
from obspy import Stream
from obspy.core.inventory import Inventory
from obspy.signal.rotate import rotate2zne

from seispy.archive import coverage_day
from seispy.waveform.integrity import merge_contiguous_segments
from seispy.waveform._correction import (
    CorrectionIssue,
    CorrectionSummary,
    epoch,
    failure,
    inputs,
    mark,
    logger,
    read_one,
    run,
    value,
    write_group,
)


def correct_orientation(
    source_dir: str | Path,
    inventory: str | Path | Inventory,
    *,
    output_dir: str | Path,
    components: Sequence[str] = ("BHZ", "BHN", "BHE"),
    apply_tilt: bool = False,
    output_prefix: str | None = None,
    pattern: str = "*.sac",
    max_workers: int = 4,
    files_per_pool: int | None = 1024,
    overwrite: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CorrectionSummary:
    """Rotate one explicit (vertical, horizontal-1, horizontal-2) channel set.

    Directions come from standard StationXML Azimuth/Dip, not channel suffixes
    or filenames. Defaults select only BHZ/BHN/BHE; BHH is ignored. Input files
    can be anywhere beneath source_dir. Groups are network/station/location and
    coverage day; counters count these groups, not individual output files.

    apply_tilt=False solves the horizontal two-axis basis and preserves an
    upward vertical axis (negates a downward axis). Dips are otherwise ignored.
    apply_tilt=True uses all three measured signed axis dips with rotate2zne;
    metadata explicitly marked nominal/unknown is rejected, never inferred
    from a scalar tilt magnitude. Missing directions or singular bases fail.

    Losslessly compatible segments merge. Only common coverage is written;
    gaps stay gaps, conflicting overlaps and noninteger sample offsets fail.
    No interpolation, padding or rate conversion is performed. Outputs are
    Z/N/E with updated cmpaz/cmpinc and reserved SAC kuser1='SPORI'.
    Existing outputs fail unless overwrite=True; completed files are not skipped.
    Source inventory describes raw axes and is never modified or relabelled
    as an output response. Do not reuse it to rotate corrected output again.
    """
    if (
        isinstance(components, str)
        or len(components) != 3
        or len(set(components)) != 3
        or any(len(c) != 3 for c in components)
    ):
        raise ValueError(
            "components must contain three distinct three-character channel codes in Z,H1,H2 order"
        )
    prefixes = {c[:2] for c in components}
    prefix = (
        output_prefix
        if output_prefix is not None
        else next(iter(prefixes))
        if len(prefixes) == 1
        else None
    )
    if not isinstance(prefix, str) or len(prefix) != 2 or not prefix.isalnum():
        raise ValueError(
            "Specify a two-character output_prefix for mixed channel prefixes"
        )
    if not isinstance(apply_tilt, bool):
        raise ValueError("apply_tilt must be a bool")
    _, output, files, inv = inputs(
        source_dir,
        output_dir,
        inventory,
        pattern,
        max_workers,
        files_per_pool,
        max_error_samples,
    )
    groups, issues, ignored = defaultdict(list), [], 0
    for path in files:
        try:
            tr = read_one(path, headonly=True)
            if tr.stats.channel not in components:
                ignored += 1
                continue
            key = (
                tr.stats.network,
                tr.stats.station,
                tr.stats.location,
                str(coverage_day(tr.stats)),
            )
            groups[key].append(path)
        except Exception as exc:
            issues.append(CorrectionIssue("", path, f"{type(exc).__name__}: {exc}"))
    tasks = [dict(paths=tuple(paths)) for _, paths in sorted(groups.items())]
    return run(
        tasks,
        _orientation_batch,
        (output, inv, tuple(components), apply_tilt, prefix, overwrite),
        operation="correct-orientation",
        output=output,
        max_workers=max_workers,
        files_per_pool=files_per_pool,
        max_error_samples=max_error_samples,
        ignored=ignored,
        initial_issues=issues,
        save_report=save_report,
        save_log=save_log,
    )


def _directions(sta, ch, apply_tilt, vertical):
    if str(value(sta, "orientation_correction_applied", "false")).lower() == "true":
        raise ValueError("Metadata already describes orientation-corrected data")
    if ch.azimuth is None or ch.dip is None:
        raise ValueError(f"Missing Azimuth/Dip for {sta.code}.{ch.code}")
    az, dip = float(ch.azimuth), float(ch.dip)
    if not np.isfinite([az, dip]).all() or not 0 <= az < 360 or not -90 <= dip <= 90:
        raise ValueError("Invalid axis direction")
    if apply_tilt:
        statuses = (str(value(sta, "dip_status", "")), str(value(ch, "dip_status", "")))
        if any("nominal" in s.lower() or "unknown" in s.lower() for s in statuses):
            raise ValueError(
                "apply_tilt=True requires measured signed axis dips; metadata is nominal/unknown"
            )
    elif vertical and dip == 0:
        raise ValueError("Vertical-axis polarity is unknown (Dip=0)")
    return az, dip


def _common_windows(segments):
    rates = {tr.stats.sampling_rate for stream in segments for tr in stream}
    if len(rates) != 1:
        raise ValueError(
            "Components have different sampling rates; interpolation is not performed"
        )
    for stream in segments:
        stream.sort(keys=["starttime"])
        for a, b in zip(stream, stream[1:], strict=False):
            if b.stats.starttime <= a.stats.endtime:
                raise ValueError("Conflicting overlapping segments")
    pointers = [0, 0, 0]
    while all(pointers[k] < len(segments[k]) for k in range(3)):
        traces = [segments[k][pointers[k]] for k in range(3)]
        start = max(t.stats.starttime for t in traces)
        end = min(t.stats.endtime for t in traces)
        if start <= end:
            delta = traces[0].stats.delta
            indices = [
                (start.ns - t.stats.starttime.ns) / (delta * 1e9) for t in traces
            ]
            if any(abs(i - round(i)) > 1e-5 for i in indices):
                raise ValueError(
                    "Components have fractional-sample time offsets; interpolation is not performed"
                )
            npts = round((end.ns - start.ns) / (delta * 1e9)) + 1
            window = []
            for tr, index in zip(traces, indices, strict=True):
                result = tr.copy()
                result.data = tr.data[round(index) : round(index) + npts].copy()
                result.stats.starttime = start
                if len(result) != npts:
                    raise ValueError("Inconsistent common sample grid")
                window.append(result)
            yield window
        earliest = min(t.stats.endtime for t in traces)
        for k, tr in enumerate(traces):
            if tr.stats.endtime == earliest:
                pointers[k] += 1


def _rotate(traces, directions, apply_tilt, prefix):
    if apply_tilt:
        matrix = []
        for az, dip in directions:
            az, dip = np.deg2rad([az, dip])
            matrix.append(
                [-np.sin(dip), np.cos(az) * np.cos(dip), np.sin(az) * np.cos(dip)]
            )
        if np.linalg.cond(matrix) > 1e8:
            raise ValueError("Axis directions are singular or ill-conditioned")
        data = rotate2zne(
            traces[0].data,
            *directions[0],
            traces[1].data,
            *directions[1],
            traces[2].data,
            *directions[2],
        )
    else:
        azimuths = np.deg2rad([directions[1][0], directions[2][0]])
        matrix = np.column_stack([np.cos(azimuths), np.sin(azimuths)])
        if np.linalg.cond(matrix) > 1e8:
            raise ValueError("Horizontal directions are singular or ill-conditioned")
        north, east = np.linalg.solve(
            matrix, np.vstack([traces[1].data, traces[2].data])
        )
        vertical = traces[0].data if directions[0][1] < 0 else -traces[0].data
        data = (vertical, north, east)
    for tr, samples, code, az, inc in zip(
        traces, data, "ZNE", (0.0, 0.0, 90.0), (0.0, 90.0, 90.0), strict=True
    ):
        tr.data = samples
        tr.stats.channel = prefix + code
        tr.stats.sac.cmpaz = az
        tr.stats.sac.cmpinc = inc
    return traces


def _orientation_batch(tasks, output, inv, components, apply_tilt, prefix, overwrite):
    results = []
    for task in tasks:
        try:
            streams = {c: Stream() for c in components}
            directions = {}
            for path in task["paths"]:
                tr = read_one(path)
                mark(tr, "orientation")
                sta, ch = epoch(inv, tr)
                direction = _directions(
                    sta, ch, apply_tilt, tr.stats.channel == components[0]
                )
                if (
                    tr.stats.channel in directions
                    and directions[tr.stats.channel] != direction
                ):
                    raise ValueError(
                        "Axis direction changes within a group; split inputs at the metadata epoch"
                    )
                directions[tr.stats.channel] = direction
                streams[tr.stats.channel].append(tr)
            if any(not streams[c] for c in components):
                raise ValueError(
                    "Missing required component(s): "
                    + ", ".join(c for c in components if not streams[c])
                )
            for stream in streams.values():
                merge_contiguous_segments(stream)
            ordered = [streams[c] for c in components]
            input_samples = sum(len(tr) for stream in ordered for tr in stream)
            traces = []
            for window in _common_windows(ordered):
                traces.extend(
                    _rotate(
                        window, [directions[c] for c in components], apply_tilt, prefix
                    )
                )
            if not traces:
                raise ValueError("Components have no common data coverage")
            logger.info(
                "Orientation correction %s: components=%s directions=%s apply_tilt=%s output_prefix=%s trimmed_samples=%d",
                task["paths"],
                components,
                directions,
                apply_tilt,
                prefix,
                input_samples - sum(len(tr) for tr in traces),
            )
            written = write_group(traces, output, overwrite)
            results.append(dict(status="succeeded", files_written=written))
        except Exception as exc:
            results.append(failure(task["paths"], exc))
    return results
