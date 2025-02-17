import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from icecream import ic
from rose import pather, write_errors
from tqdm import tqdm


def resample_last_subdirs(
    src_dir: str | Path,
    delta: float,
    method: str = "obspy",
    pattern: str = "*.sac",
    remove_src: bool = True,
    max_workers: int = 5,
) -> None:
    src_path = Path(src_dir)
    last_subdirs = pather.find_last_subdirs(src_path)

    # resample
    ic("resampling ...")
    errs = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                resample_by_method(method), subdir, pattern, delta, remove_src
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


def resample_by_method(method):
    method = method.lower()
    if method == "obspy":
        ic("NOTE: using Obspy `resample` not `decimate`.")
        return obspy_resample
    if method == "sac":
        return sac_resample
    raise ValueError(f"Unknown method: {method}")


def obspy_resample(
    dir: Path, pattern: str, delta: float, remove_src: bool
) -> str | None:
    try:
        for target in dir.glob(pattern):
            st = obspy.read(target)
            st.resample(delta)
            dest_sac = target.with_suffix(f".{delta}Hz.sac")
            st.write(str(dest_sac), format="SAC")
            if remove_src:
                target.unlink()
    except Exception as e:
        return f"Error occered at {target.parent} : {e}\n"

    time.sleep(0.1)


def sac_resample(dir: Path, pattern: str, delta: float, remove_src: bool) -> str | None:
    import os
    import subprocess

    cmd = ""
    for target in dir.glob(pattern):
        cmd += f"r {target}\n"
        cmd += f"decimate {delta} \n"
        if remove_src:
            cmd += "w over \n"
        else:
            cmd += f"w {target.with_suffix(f'.{delta}Hz.sac')}\n"
    cmd += "q\n"

    os.putenv("SAC_DISPLAY_COPYRIGHT", "0")
    subprocess.Popen(["sac"], stdin=subprocess.PIPE).communicate(cmd.encode())

    time.sleep(0.1)
