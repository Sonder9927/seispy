import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple

import obspy
from icecream import ic
from rose import get_logger
from tqdm import tqdm

_LOG_RESAMPLE = {
    "name": "resample",
    "file": "resample.log",
    "level": logging.INFO,
}


def resample_by_station(
    src_dir: str | Path,
    delta: float,
    method: str = "obspy",
    pattern: str = "*.sac",
    remove_src: bool = True,
    max_workers: int = 5,
) -> None:
    logger = get_logger(**_LOG_RESAMPLE)
    logger.info(f"Start resample with {method=} at {src_dir=}.")
    src_path = Path(src_dir)
    station_paths = list(src_path.glob("*/"))
    total = len(station_paths)
    logger.info(f"Found {total} stations.")

    # resample
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _resample_method(method), sta_path, pattern, delta, remove_src
            )
            for sta_path in station_paths
        }
        with tqdm(total=total, desc="Resampling...") as pbar:
            post = {"total": 0, "failed": 0}
            for future in as_completed(futures):
                batch_total, failed = future.result()
                post["total"] += batch_total
                post["failed"] += failed
                pbar.update(1)
                pbar.set_postfix(post)

    logger.info(f"Resample complete. With {post}.")
    print(f"Resample complete. Check {_LOG_RESAMPLE['file']} for details.")


def _resample_method(method):
    method = method.lower()
    if method == "obspy":
        ic("NOTE: using Obspy `resample` not `decimate`.")
        return obspy_resample_by_station
    if method == "sac":
        return sac_resample_by_station
    raise ValueError(f"Unknown method: {method}")


def obspy_resample_by_station(
    dir: Path, pattern: str, delta: float, remove_src: bool
) -> Tuple[int, int]:
    logger = get_logger(**_LOG_RESAMPLE)
    total = 0
    failed = 0
    for target in dir.rglob(pattern):
        total += 1
        try:
            st = obspy.read(target)
            st.resample(delta)
            dest_sac = target.with_suffix(f".{delta}Hz.sac")
            st.write(str(dest_sac), format="SAC")
            if remove_src:
                target.unlink()
            logger.debug(f"resampled {target.name} -> {dest_sac.name}")
        except Exception as e:
            failed += 1
            logger.error(f"Error occered at {target.parent} : {e}")

    time.sleep(0.1)
    return total, failed


def resample_to(
    src_dir: str | Path,
    dest_dir: str | Path,
    deltas: List[float],
    pattern: str = "*.sac",
    max_workers: int = 5,
):
    logger = get_logger(**_LOG_RESAMPLE)
    logger.info(f"Start resample from {src_dir} to {dest_dir} with {deltas=}.")
    src_path = Path(src_dir)
    dest_path = Path(dest_dir)
    sac_paths = list(src_path.rglob(pattern))
    bs = 2000
    batches = [sac_paths[i: i + bs] for i in range(0, len(sac_paths), bs)]
    total = len(batches)
    logger.info(f"Found {total} stations.")

    # resample
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(sac_resample_to, src_path, dest_path, bb, deltas)
            for bb in batches
        }
        with tqdm(total=total, desc="Resampling...") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)


def sac_resample_to(
    src_path: Path, dest_path: Path, sacs: List[Path], deltas: List[float]
):
    import os
    import subprocess

    cmd = ""
    for sac in sacs:
        dest_sac = dest_path / sac.relative_to(src_path)
        dest_sac.parent.mkdir(parents=True, exist_ok=True)

        cmd += f"r {sac}\n"
        for delta in deltas:
            cmd += f"decimate {delta} \n"
        cmd += f"w {dest_sac}\n"
    cmd += "q\n"

    os.putenv("SAC_DISPLAY_COPYRIGHT", "0")
    subprocess.Popen(["sac"], stdin=subprocess.PIPE).communicate(cmd.encode())

    time.sleep(0.1)


def sac_resample_by_station(
    dir: Path, pattern: str, delta: float, remove_src: bool
) -> Tuple[int, int]:
    import os
    import subprocess

    total = 0
    cmd = ""
    for target in dir.rglob(pattern):
        cmd += f"r {target}\n"
        cmd += f"decimate {delta} \n"
        if remove_src:
            cmd += "w over \n"
        else:
            cmd += f"w {target.with_suffix(f'.{delta}Hz.sac')}\n"
        total += 1
    cmd += "q\n"

    os.putenv("SAC_DISPLAY_COPYRIGHT", "0")
    subprocess.Popen(["sac"], stdin=subprocess.PIPE).communicate(cmd.encode())

    time.sleep(0.1)
    return total, 0
