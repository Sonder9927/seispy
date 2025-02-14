"""
instrument responses from GEONET
"""

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from icecream import ic
from rose import pather, write_errors
from tqdm import tqdm


def deconvolution_last_subdirs(
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
    src_path = Path(src_dir)
    last_subdirs = pather.find_last_subdirs(src_path)

    # read response if method is obspy
    inv = obspy.read_inventory(resp) if method == "obspy" else resp

    # remove response
    ic("removing response ...")
    errs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                deconv_by_method(method),
                subdir,
                pattern,
                inv,
                resample,
                remove_src,
            )
            for subdir in last_subdirs
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            mininterval=2,
            desc="deconvolution at last subdirs",
        ):
            result = future.result()
            if result:
                errs.append(result)
    if errs:
        write_errors(errs)
    else:
        ic("All Removed Response with NO errors!")


def deconv_by_method(method):
    method = method.lower()
    if method == "obspy":
        return obspy_deconv
    if method == "sac":
        return sac_deconv
    raise ValueError(f"Unknown method: {method}")


def obspy_deconv(
    dir: Path, pattern: str, inv, resample, remove_src: bool
) -> str | None:
    try:
        for target in dir.glob(pattern):
            st = stream_removed_response(target, inv, resample)
            dest_sac = target.with_suffix(".deconv.sac")
            st.write(str(dest_sac), format="SAC")
            if remove_src:
                target.unlink()
    except Exception as e:
        return f"Error occered at {target.parent} : {e}\n"

    time.sleep(0.1)


def sac_deconv(
    dir: Path, pattern: str, pzs: str, resample, remove_src: bool
) -> str | None:
    import os
    import subprocess

    if resample is not None:
        ic("resample is not supported by `sac` method")

    cmd = ""
    for target in dir.glob(pattern):
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


def stream_removed_response(file: str | Path, inv, resample: float | None = None):
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
