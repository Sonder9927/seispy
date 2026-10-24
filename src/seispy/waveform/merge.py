"""Merge waveform segments by channel and day."""

import math
from dataclasses import dataclass
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from seispy._main_guard import require_reimport_safe_entry_point
from seispy.progress import call_with_warnings, progress_iter, resolve_worker_call

from seispy.archive import WaveformIdentity
from seispy.waveform.integrity import merge_contiguous_segments, merge_short_gaps
from seispy.workflow import (
    commit_output,
    resolve_separate_directory_trees,
    temporary_output_path,
)


@dataclass(frozen=True)
class DailyMergeSummary:
    """Channel-day outcomes and discovery/processing errors."""

    written: int
    skipped: int
    failed: int
    errors: tuple[str, ...]


def merge_waveforms_by_day(
    source_dir: str | Path,
    output_dir: str | Path,
    pattern: str = "*.SAC",
    *,
    output_format: str = "sac",
    max_workers: int = 5,
    overwrite: bool = False,
) -> DailyMergeSummary:
    """Merge archived waveforms by channel and day.

    MiniSEED mode splits at UTC midnight and preserves gaps as separate traces
    in one channel-day file. Conflicting overlaps fail the day. Partial days
    are allowed. SAC mode retains its legacy short-gap interpolation policy.
    Set output_format="mseed" and pattern="*.mseed" for MiniSEED archives.


    Args:
        source_dir: Canonical SAC archive searched recursively for input files.
        output_dir: Separate root for merged channel-day SAC files.
        pattern: Recursive input pattern; use "*.mseed" for MiniSEED.
        output_format: "sac" (legacy) or "mseed" (lossless daily archive).
        max_workers: Maximum number of parallel channel-day workers.
        overwrite: Rebuild existing outputs. MiniSEED otherwise verifies sample
            equality before skipping; changed inputs are reported as errors.

    Examples:
        ```python
        merge_waveforms_by_day("data/sac", "data/sac-merged", pattern="*.sac")
        ```
    """
    if output_format not in {"sac", "mseed"}:
        raise ValueError("output_format must be 'sac' or 'mseed'")
    if max_workers < 1:
        raise ValueError("max_workers must be positive")
    require_reimport_safe_entry_point("merge_waveforms_by_day")
    src_path, output_path = resolve_separate_directory_trees(source_dir, output_dir)
    if not src_path.is_dir():
        raise NotADirectoryError(src_path)
    output_path.mkdir(parents=True, exist_ok=True)
    if output_format == "mseed":
        return _merge_mseed_days(src_path, output_path, pattern, max_workers, overwrite)
    groups, errs = _group_targets(src_path, pattern)
    written_count = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(
                call_with_warnings, _merge_targets, targets, output_path, overwrite
            )
            for targets in groups.values()
        ]
        for future in progress_iter(
            as_completed(futures),
            total=len(futures),
            mininterval=2,
            desc="Merging channel-days",
            unit="day",
        ):
            error = resolve_worker_call(future.result())
            if error:
                errs.append(error)
            else:
                written_count += 1
    if errs:
        error_path = output_path / "merge-errors.txt"
        error_path.write_text("".join(errs))
        print(f"Check {error_path} for more information")
    else:
        print("All done with no errors.")
    return DailyMergeSummary(written_count, 0, len(errs), tuple(errs))


def _group_targets(src: Path, pattern: str):
    groups = defaultdict(list)
    errors = []
    for path in sorted(
        candidate
        for candidate in src.rglob(pattern)
        if candidate.is_file() and not candidate.name.lower().endswith(".merged.sac")
    ):
        try:
            stream = obspy.read(path, headonly=True)
            if len(stream) != 1:
                raise ValueError("SAC file must contain exactly one trace")
            identity = WaveformIdentity.from_trace(stream[0])
            if not identity.matches_sac_path(path, src):
                raise ValueError("filename or directory does not match the SAC header")
            groups[identity.day_key].append(path)
        except Exception as exc:
            errors.append(f"Errors in {path}:\n  {exc}\n")
    return dict(groups), errors


def _merge_targets(
    sacs: list[Path], output_dir: Path, overwrite: bool = False
) -> str | None:
    temporary = None
    try:
        st = obspy.Stream()
        for sac in sacs:
            st += obspy.read(sac)
        identities = [WaveformIdentity.from_trace(trace) for trace in st]
        if not identities or len({item.day_key for item in identities}) != 1:
            raise ValueError("SAC headers do not share one channel-day identity")
        st.sort()
        merge_short_gaps(st)
        if len(st) != 1:
            raise ValueError("SAC segments did not merge into one continuous trace")
        destination = identities[0].sac_path(output_dir, merged=True)
        if destination.exists() and not overwrite:
            raise FileExistsError(destination)
        temporary = temporary_output_path(destination)
        st[0].write(str(temporary), format="SAC")
        written = obspy.read(temporary, format="SAC", headonly=True)
        if len(written) != 1 or not WaveformIdentity.from_trace(
            written[0]
        ).matches_sac_path(destination, output_dir):
            raise ValueError("merged SAC output identity does not match its path")
        commit_output(temporary, destination, overwrite=overwrite)
        temporary = None

    except Exception as err:
        return f"Errors in {sacs[0] if sacs else output_dir}:\n  {err}\n"
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _utc_days(trace):
    day = obspy.UTCDateTime(trace.stats.starttime.date)
    while day <= trace.stats.endtime:
        yield day
        day += 86400


def _merge_mseed_days(source, output, pattern, max_workers, overwrite):
    from seispy.archive import matches_mseed_path

    groups = defaultdict(set)
    errors = []
    for path in sorted(source.rglob(pattern)):
        if not path.is_file():
            continue
        try:
            stream = obspy.read(path, format="MSEED", headonly=True)
            if not matches_mseed_path(path, source, stream):
                raise ValueError("path does not match MiniSEED headers")
            for trace in stream:
                for day in _utc_days(trace):
                    groups[(*trace.id.split("."), day.year, day.julday)].add(path)
        except Exception as exc:
            errors.append(f"{path}: {exc}")
    written = skipped = failed = 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_merge_mseed_day, key, sorted(paths), output, overwrite)
            for key, paths in groups.items()
        ]
        for future in progress_iter(
            as_completed(futures),
            total=len(futures),
            desc="Merging channel-days",
            unit="day",
        ):
            try:
                status, error = future.result()
            except Exception as exc:
                status, error = "failed", str(exc)
            written += status == "written"
            skipped += status == "skipped"
            failed += status == "failed"
            if error:
                errors.append(error)
    report = output / "merge-errors.txt"
    if errors:
        report.write_text("\n".join(errors) + "\n")
    else:
        report.unlink(missing_ok=True)
    return DailyMergeSummary(written, skipped, failed, tuple(errors))


def _merge_mseed_day(key, paths, output, overwrite):
    import numpy as np

    temporary = None
    try:
        network, station, location, channel, year, julday = key
        day = obspy.UTCDateTime(year=year, julday=julday)
        stream = obspy.Stream()
        for path in paths:
            for trace in obspy.read(path, format="MSEED").select(
                network=network, station=station, location=location, channel=channel
            ):
                # Half-open UTC windows: the midnight sample belongs only to the
                # following day, including on non-integer sample grids.
                rate = trace.stats.sampling_rate
                first = max(
                    0, math.ceil((day.ns - trace.stats.starttime.ns) * rate / 1e9)
                )
                stop = min(
                    trace.stats.npts,
                    math.ceil(
                        ((day + 86400).ns - trace.stats.starttime.ns) * rate / 1e9
                    ),
                )
                if stop > first:
                    part = trace.copy()
                    part.data = trace.data[first:stop].copy()
                    part.stats.starttime = trace.stats.starttime + first / rate
                    stream += part
        if not stream:
            raise ValueError("no samples in UTC day")
        if len({tr.stats.sampling_rate for tr in stream}) != 1:
            raise ValueError("inconsistent sampling rates")
        merge_contiguous_segments(stream)
        stream.sort()
        if any(
            right.stats.starttime <= left.stats.endtime
            for left, right in zip(stream, stream[1:], strict=False)
        ):
            raise ValueError("conflicting overlap or incompatible sample grids")
        destination = (
            output
            / network
            / station
            / str(year)
            / f"{network}.{station}.{location or '--'}.{channel}.{year}.{julday:03d}.mseed"
        )

        def same_samples(other):
            merge_contiguous_segments(other)
            other.sort()
            return len(other) == len(stream) and all(
                a.id == b.id
                and a.stats.starttime == b.stats.starttime
                and a.stats.sampling_rate == b.stats.sampling_rate
                and np.array_equal(a.data, b.data)
                for a, b in zip(stream, other, strict=True)
            )

        if destination.exists() and not overwrite:
            if not same_samples(obspy.read(destination, format="MSEED")):
                raise ValueError(
                    "existing daily output differs; use overwrite=True to rebuild"
                )
            return "skipped", None
        temporary = temporary_output_path(destination)
        stream.write(str(temporary), format="MSEED")
        if not same_samples(obspy.read(temporary, format="MSEED")):
            raise ValueError("written daily samples do not match input")
        commit_output(temporary, destination, overwrite=overwrite)
        return "written", None
    except Exception as exc:
        return "failed", f"{key}: {exc}"
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
