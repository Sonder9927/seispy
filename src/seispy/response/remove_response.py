import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Callable

import obspy
from rose import get_logger
from tqdm import tqdm

_LOG_DECONVOLUTION = {
    "name": "deconvolution",
    "file": "deconvolution.log",
    "level": logging.INFO,
}


def deconvolution_by_station(
    src_dir: str | Path,
    resp: str,
    resample: float | None = None,
    method: str = "obspy",
    pattern: str = "*.sac",
    remove_src: bool = True,
    max_workers: int = 5,
) -> None:
    """deconvolution last subdirs (days)

    remove response by last directories

    Parameters:
        src_dir: source directory
        resp: response file
        resample: resample to given delta if it isn't None.
        method: method of data processing
        pattern: search pattern at last subdirectory
        remove_src: remove source file after deconvolution
        max_workers: max workers for parallel processing
    """
    logger = get_logger(**_LOG_DECONVOLUTION)
    logger.info(f"Deconvolution started with {method=} at {src_dir=}.")

    method = method.lower()
    src_path = Path(src_dir)
    station_paths = list(src_path.glob("*/"))
    total = len(station_paths)
    logger.info(f"Found {total} stations.")

    # read response if method is obspy
    inv = obspy.read_inventory(resp) if method == "obspy" else resp

    # remove response
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                deconv_by_method(method),
                sta_path,
                pattern,
                _get_response(method, inv, sta_path.name),
                resample,
                remove_src,
            )
            for sta_path in station_paths
        }
        with tqdm(total=total, desc="Processing stations") as pbar:
            post = {"total": 0, "failed": 0}
            for future in as_completed(futures):
                batch_total, failed = future.result()
                post["total"] += batch_total
                post["failed"] += failed
                pbar.update(1)
                pbar.set_postfix(post)

    logger.info(f"Deconvolution complete with {post}.")
    print(f"Deconvolution complete. Check {_LOG_DECONVOLUTION['file']} for details.")


def _get_response(method, resp, station):
    if method == "obspy":
        inv = resp.select(station=station)
        if len(inv):
            return inv
        raise ValueError(f"{station=} not found in the inventory.")
    if method == "sac":
        return resp
    raise ValueError(f"Unknown method: {method}")


def deconv_by_method(method) -> Callable:
    if method == "obspy":
        return obspy_deconv
    if method == "sac":
        return sac_deconv
    raise ValueError(f"Unknown method: {method}")


def obspy_deconv(dir: Path, pattern, inv, resample, remove_src: bool):
    logger = get_logger(**_LOG_DECONVOLUTION)
    total = 0
    failed = 0
    for target in dir.rglob(pattern):
        total += 1
        try:
            st = stream_removed_response(target, inv, resample)
            dest_sac = target.with_suffix(".deconv.sac")
            st.write(str(dest_sac), format="SAC")
            logger.debug(f"Deconvolution {target.name} -> {dest_sac.name}")
            if remove_src:
                target.unlink()
        except Exception as e:
            failed += 1
            logger.error(f"Error occered at {target} : {e}")

    time.sleep(0.1)
    return total, failed


def sac_deconv(dir: Path, pattern, pzs, resample, remove_src):
    import os
    import subprocess

    logger = get_logger(**_LOG_DECONVOLUTION)
    if resample is not None:
        logger.warning("resample is not supported by `sac` method")

    total = 0
    cmd = ""
    for target in dir.rglob(pattern):
        total += 1
        cmd += f"r {target}\n"
        cmd += "rmean; rtr; taper \n"
        cmd += f"trans from pol s {pzs} to none freq 0.003 0.006 1 2\n"
        cmd += "mul 1.0e9 \n"
        if remove_src:
            cmd += "w over \n"
        else:
            cmd += f"w {target.with_suffix('.deconv.sac')}\n"
    cmd += "q\n"

    os.putenv("SAC_DISPLAY_COPYRIGHT", "0")
    subprocess.Popen(["sac"], stdin=subprocess.PIPE).communicate(cmd.encode())

    time.sleep(0.1)
    return total, 0


def stream_removed_response(file: str | Path, inv, resample=None):
    """remove response from sac file

    Parameters:
        file: target file
        inv (Inventory): inventory
        resample: resample result of deconvolution.
    Returns:
        Stream: stream of deconvolution
    """
    st = obspy.read(file)
    st.merge(method=1, fill_value="interpolate")
    for tr in st:
        tr.detrend("demean")
        tr.detrend("linear")
        tr.taper(max_percentage=0.05, type="hann")
        tr.remove_response(
            inventory=inv,
            water_level=None,
            pre_filt=[0.003, 0.006, 1, 2],
            output="DISP",
            zero_mean=False,
            taper=False,
        )
        tr.data *= 1e9
        if resample is not None:
            tr.resample(resample)
    return st
