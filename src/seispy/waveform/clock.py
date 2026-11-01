"""Sample-preserving clock shifts from StationXML linear drift models."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Sequence

from obspy import UTCDateTime
from obspy.core.inventory import Inventory

from seispy.archive import WaveformIdentity

from seispy.waveform._correction import (
    CorrectionIssue,
    CorrectionSummary,
    epoch,
    failure,
    inputs,
    mark,
    provenance,
    read_one,
    resume,
    run,
    value,
    write_group,
)


def correct_clock_drift(
    source_dir: str | Path,
    inventory: str | Path | Inventory,
    *,
    output_dir: str | Path,
    channels: Sequence[str] = ("BHZ", "BHN", "BHE"),
    pattern: str = "*.sac",
    max_workers: int = 4,
    files_per_pool: int | None = 1024,
    overwrite: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CorrectionSummary:
    """Shift each complete record by its midpoint clock offset, without interpolation.

    The StationXML Station.extra fields are clock_reference_time,
    clock_end_time, clock_initial_offset_seconds, clock_rate_seconds_per_second,
    and clock_sign='observed_time_minus_GPS_time'. The model argument must be
    recorded time (clock_model='linear_recorded_timestamp'). Channel.extra may
    override these fields. Full-record epoch/model coverage is required.

    Samples, npts and sampling interval stay unchanged. No rounding to a sample
    interval is performed. Per-file provenance records contain the applied
    shift and maximum endpoint residual of this constant-shift approximation.
    This is not a full linear time-axis correction. Identical Z/N/E windows
    receive the same shift; rotation subsequently rejects misaligned grids.

    Sources and inventory are preserved. Outputs use canonical SAC paths and
    SAC kuser0='SPCLK'; this reserved field must be empty on first input.
    Reprocessing corrected input is rejected. overwrite=False resumes only
    outputs with matching input/settings fingerprints and valid output hashes.
    Counters count input files; unselected channels (including BHH by default)
    are counted as ignored. Use max_workers=1 for serial execution.
    """
    if isinstance(channels, str) or not channels or len(set(channels)) != len(channels):
        raise ValueError(
            "channels must be a nonempty sequence of distinct channel codes"
        )
    source, output, files, inv = inputs(
        source_dir,
        output_dir,
        inventory,
        pattern,
        max_workers,
        files_per_pool,
        max_error_samples,
    )
    tasks, issues, ignored = [], [], 0
    destinations = set()
    for path in files:
        try:
            tr = read_one(path, headonly=True)
            if tr.stats.channel not in channels:
                ignored += 1
                continue
            sta, ch = epoch(inv, tr)
            fields = {}
            for key in (
                "clock_model",
                "clock_reference_time",
                "clock_end_time",
                "clock_initial_offset_seconds",
                "clock_rate_seconds_per_second",
                "clock_sign",
                "clock_model_argument",
                "clock_correction_applied",
            ):
                fields[key] = value(ch, key, value(sta, key))
            model = _clock_model(fields)
            planned = tr.copy()
            planned.stats.starttime -= _offset(planned, model)
            target = WaveformIdentity.from_trace(planned).sac_path(output)
            if target in destinations:
                raise ValueError("Multiple inputs map to the same corrected SAC path")
            destinations.add(target)
            tasks.append(dict(paths=(path,), model=model))
        except Exception as exc:
            issues.append(CorrectionIssue("", path, f"{type(exc).__name__}: {exc}"))
    return run(
        tasks,
        _clock_batch,
        (source, output, overwrite),
        operation="correct-clock-drift",
        output=output,
        max_workers=max_workers,
        files_per_pool=files_per_pool,
        max_error_samples=max_error_samples,
        ignored=ignored,
        initial_issues=issues,
        save_report=save_report,
        save_log=save_log,
    )


def _clock_model(fields):
    if fields["clock_model"] != "linear_recorded_timestamp":
        raise ValueError("Expected clock_model=linear_recorded_timestamp")
    if fields["clock_sign"] != "observed_time_minus_GPS_time":
        raise ValueError("Unsupported or missing clock sign convention")
    if str(fields["clock_correction_applied"]).lower() == "true":
        raise ValueError("Metadata already describes clock-corrected data")
    argument = fields["clock_model_argument"]
    if argument is not None and not str(argument).startswith("recorded timestamp"):
        raise ValueError("Clock model argument must be recorded timestamp")
    # Missing values must not fall through UTCDateTime(None), which means now.
    if any(
        fields[k] is None
        for k in (
            "clock_reference_time",
            "clock_end_time",
            "clock_initial_offset_seconds",
            "clock_rate_seconds_per_second",
        )
    ):
        raise ValueError("Incomplete clock model")
    start, end = (
        UTCDateTime(fields["clock_reference_time"]),
        UTCDateTime(fields["clock_end_time"]),
    )
    offset, rate = (
        float(fields["clock_initial_offset_seconds"]),
        float(fields["clock_rate_seconds_per_second"]),
    )
    if (
        end <= start
        or not math.isfinite(offset)
        or not math.isfinite(rate)
        or abs(rate) >= 1
    ):
        raise ValueError("Invalid clock model bounds, offset or rate")
    return dict(
        reference_time=str(start), end_time=str(end), initial_offset=offset, rate=rate
    )


def _clock_batch(tasks, source, output, overwrite):
    results = []
    for task in tasks:
        try:
            (path,) = task["paths"]
            model = task["model"]
            payload, fingerprint = provenance(
                (path,), source, "clock", dict(method="midpoint_shift", **model)
            )
            if resume(output, fingerprint, overwrite):
                results.append(dict(status="skipped", files_written=0))
                continue
            tr = read_one(path)
            mark(tr, "clock")
            duration = tr.stats.endtime - tr.stats.starttime
            correction = _offset(tr, model)
            original_start = tr.stats.starttime
            tr.stats.starttime -= correction
            written = write_group(
                [tr],
                output,
                fingerprint,
                payload,
                dict(
                    id=tr.id,
                    original_start=str(original_start),
                    corrected_start=str(tr.stats.starttime),
                    clock_offset_seconds=correction,
                    applied_shift_seconds=-correction,
                    max_endpoint_residual_seconds=abs(model["rate"]) * duration / 2,
                    max_endpoint_residual_samples=abs(model["rate"])
                    * duration
                    * tr.stats.sampling_rate
                    / 2,
                    sampling_rate=tr.stats.sampling_rate,
                    interpolation=False,
                ),
                overwrite,
            )
            results.append(dict(status="succeeded", files_written=written))
        except Exception as exc:
            results.append(failure(task["paths"], exc))
    return results


def _offset(tr, model):
    reference, end = (
        UTCDateTime(model["reference_time"]),
        UTCDateTime(model["end_time"]),
    )
    if tr.stats.starttime < reference or tr.stats.endtime > end:
        raise ValueError("Complete trace must lie within clock model validity interval")
    midpoint = tr.stats.starttime + (tr.stats.endtime - tr.stats.starttime) / 2
    return model["initial_offset"] + model["rate"] * (midpoint - reference)
