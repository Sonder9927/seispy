import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from icecream import ic
from tqdm import tqdm

from rose import pather


def merge_by_day(src: str | Path, remove_src: bool = True) -> None:
    src_path = Path(src)
    days = pather.find_last_subdirs(src_path)
    errs = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_merge_targets, day, remove_src)
            for day in days if day
        }
        for future in as_completed(futures):
            future = future.result()
            if future:
                errs.append(future)
    ic(f"All done. Merged in `{src}`.")
    ic(errs)


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
    nbach = 2000
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_copy_targets, targets[i: i + nbach], dest_path)
            for i in range(0, len(targets), nbach)
        }
        for future in as_completed(futures):
            future.result()
    ic(f"All done. Sorted `{src}` to `{dest}`.")


def _copy_targets(targets: list[Path], dest_path: Path):
    for target in tqdm(targets):
        net, sta, _, _, _, year, day, _ = target.stem.split(".")
        dest_file = dest_path / net / sta / year / day / target.name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(target, dest_file)
    time.sleep(1)


def _merge_targets(day: Path, remove_src: bool) -> str | None:
    sacs = list(day.glob("*.SAC"))
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
