import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from rose import pather, write_errors
from tqdm import tqdm

from seispy._waveform import merge_short_gaps


def merge_by_day(
    src: str | Path, pattern: str = "*.SAC", remove_src: bool = True
) -> None:
    """Merge SAC traces in each leaf directory by day.

    Args:
        src: Root directory containing daily leaf directories.
        pattern: File pattern evaluated in each leaf directory.
        remove_src: Remove source files after a successful merge.

    Examples:
        >>> merge_by_day("data/sorted", pattern="*.sac", remove_src=False)
    """
    src_path = Path(src)
    days = pather.find_last_subdirs(src_path)
    errs = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_merge_targets, day, pattern, remove_src)
            for day in days
            if day.is_dir()
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            mininterval=2,
            desc="Merging at last subdirs",
        ):
            future = future.result()
            if future:
                errs.append(future)
    if errs:
        write_errors(errs)
    else:
        print("All done with no errors.")


def _merge_targets(day: Path, pattern, remove_src: bool) -> str | None:
    sacs = list(day.glob(pattern))
    try:
        st = obspy.Stream()
        for sac in sacs:
            st += obspy.read(sac)
        st.sort()
        merge_short_gaps(st)

        sac_parts = sac.stem.split(".")
        target_parts = sac_parts[:-1] + ["merged", "sac"]
        for tr in st:
            target_parts[3] = tr.stats.channel
            target_str = str(sac.parent / ".".join(target_parts))
            tr.write(target_str, format="SAC")

    except Exception as err:
        return f"Errors in {day}:\n  {err}"

    if remove_src:
        for sac in sacs:
            sac.unlink()
    time.sleep(0.1)
