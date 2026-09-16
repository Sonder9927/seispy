"""Merge waveform segments by channel and day."""

from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from tqdm import tqdm

from seispy.archive import WaveformIdentity
from seispy.waveform.integrity import merge_short_gaps
from seispy.workflow import (
    commit_output,
    resolve_separate_directory_trees,
    temporary_output_path,
)


def merge_waveforms_by_day(
    source_dir: str | Path,
    output_dir: str | Path,
    pattern: str = "*.SAC",
) -> None:
    """Merge SAC traces sharing one header-derived channel-day identity.

    Args:
        source_dir: Canonical SAC archive searched recursively for input files.
        output_dir: Separate root for merged channel-day SAC files.
        pattern: File pattern used to discover SAC files.

    Examples:
        ```python
        merge_waveforms_by_day("data/sac", "data/sac-merged", pattern="*.sac")
        ```
    """
    src_path, output_path = resolve_separate_directory_trees(source_dir, output_dir)
    if not src_path.is_dir():
        raise NotADirectoryError(src_path)
    output_path.mkdir(parents=True, exist_ok=True)
    groups, errs = _group_targets(src_path, pattern)
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(_merge_targets, targets, output_path)
            for targets in groups.values()
        ]
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            mininterval=2,
            desc="Merging channel-days",
        ):
            error = future.result()
            if error:
                errs.append(error)
    if errs:
        error_path = output_path / "merge-errors.txt"
        error_path.write_text("".join(errs))
        print(f"Check {error_path} for more information")
    else:
        print("All done with no errors.")


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


def _merge_targets(sacs: list[Path], output_dir: Path) -> str | None:
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
        if destination.exists():
            raise FileExistsError(destination)
        temporary = temporary_output_path(destination)
        st[0].write(str(temporary), format="SAC")
        written = obspy.read(temporary, format="SAC", headonly=True)
        if len(written) != 1 or not WaveformIdentity.from_trace(
            written[0]
        ).matches_sac_path(destination, output_dir):
            raise ValueError("merged SAC output identity does not match its path")
        commit_output(temporary, destination)
        temporary = None

    except Exception as err:
        return f"Errors in {sacs[0] if sacs else output_dir}:\n  {err}\n"
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)
