import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from icecream import ic
from tqdm import tqdm

from rose import pather, write_errors


def merge_by_day(
    src: str | Path, pattern: str = "*.SAC", remove_src: bool = True
) -> None:
    """merge sac files by day

    Parameters:
        src: source directory.
        pattern: search pattern.
        remove_src: whether remove source files after mergeing.
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
    ic("All Merged with NO errors!")


def sort_to(src: str | Path, dest: str | Path, pattern: str = "*.SAC"):
    """copy source and sort structure of destnation

    Sort files from `mseed2sac` to the structure like `/net/sta/year/day`.
    Target file named like `net.sta..channel.D.year.day.hourminsec.SAC`.

    Parameters:
        src_dir: source directory
        dest_dir: destination directory
        pattern: search pattern of target files

    Examples:
        >>>import seispy
        >>>seispy.sort_to('/path/src', '/path/dest', '*.SAC')
    """
    src_path = Path(src)
    dest_path = Path(dest)
    targets = pather.glob(src_path, "rglob", [pattern])
    ltargets = len(targets)
    nbach = 10_000
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_copy_targets, targets[i : i + nbach], dest_path)
            for i in range(0, ltargets, nbach)
        }
        for future in tqdm(as_completed(futures), total=len(futures), mininterval=2):
            future.result()
    ic(f"All done. Sorted `{src}` to `{dest}`.")


def _copy_targets(targets: list[Path], dest_path: Path):
    for target in targets:
        # change these parts to parse filename.
        net, sta, _, _, _, year, day, _ = target.stem.split(".")
        dest_file = dest_path / net / sta / year / day / target.name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(target, dest_file)
    time.sleep(1)


def _merge_targets(day: Path, pattern, remove_src: bool) -> str | None:
    sacs = list(day.glob(pattern))
    try:
        st = obspy.Stream()
        for sac in sacs:
            st += obspy.read(sac)
        st.sort()
        st.merge(method=1, fill_value="interpolate")

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
    time.sleep(1)


if __name__ == "__main__":
    sort_to("data/sac_src", "data/sac_dest", "*.SAC")
    merge_by_day("data/sac_dest")
