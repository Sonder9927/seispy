import time
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from tqdm import tqdm

from seispy._archive import WaveformIdentity
from seispy._waveform import merge_short_gaps


def merge_by_day(
    src: str | Path, pattern: str = "*.SAC", remove_src: bool = True
) -> None:
    """Merge SAC traces sharing one header-derived channel-day identity.

    Args:
        src: Root directory searched recursively for SAC files.
        pattern: File pattern used to discover SAC files.
        remove_src: Remove source files after a successful merge.

    Examples:
        ```python
        merge_by_day("data/sorted", pattern="*.sac", remove_src=False)
        ```
    """
    src_path = Path(src)
    groups, errs = _group_targets(src_path, pattern)
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_merge_targets, targets, src_path, remove_src): key
            for key, targets in groups.items()
        }
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
        Path("errors.txt").write_text("".join(errs))
        print("Check errors.txt for more information")
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


def _merge_targets(sacs: list[Path], src: Path, remove_src: bool) -> str | None:
    try:
        st = obspy.Stream()
        for sac in sacs:
            st += obspy.read(sac)
        identities = [WaveformIdentity.from_trace(trace) for trace in st]
        if not identities or len({item.day_key for item in identities}) != 1:
            raise ValueError("SAC headers do not share one channel-day identity")
        st.sort()
        merge_short_gaps(st)
        destination = identities[0].sac_path(src, merged=True)
        if destination.exists():
            raise FileExistsError(destination)
        for tr in st:
            tr.write(str(destination), format="SAC")

    except Exception as err:
        return f"Errors in {sacs[0] if sacs else src}:\n  {err}\n"

    if remove_src:
        for sac in sacs:
            if sac != destination:
                sac.unlink()
    time.sleep(0.1)
