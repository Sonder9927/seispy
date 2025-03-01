import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from icecream import ic
from rose import pather, write_errors
from tqdm import tqdm


def mseed_dir_to_sac(src: str | Path, dest: str | Path, pattern: str = "*.miniseed"):
    """trans mseed files to sac and sort structure of destnation

    Dest file name like `net.sta.khole.channel.D.year.day.sac`.

    Parameters:
        src_dir: source directory
        dest_dir: destination directory
        pattern: search pattern of target files

    Examples:
        >>>import seispy
        >>>seispy.mseed_dir_to_sac('/path/src', '/path/dest', '*.miniseed')
    """
    src_path = Path(src)
    dest_path = Path(dest)
    targets = pather.glob(src_path, "rglob", [pattern])
    ltargets = len(targets)
    nbach = 2000
    errs = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_mseeds_to_sac, targets[i: i + nbach], dest_path)
            for i in range(0, ltargets, nbach)
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            mininterval=2,
            desc="Mseeds to sac ...",
        ):
            future = future.result()
            if future:
                errs += future
    if errs:
        write_errors(errs)
    ic("All Merged with NO errors!")


def _mseeds_to_sac(targets: list[Path], dest_path: Path):
    errs = []
    for target in targets:
        try:
            _mseed2sac(target, dest_path)
        except Exception as err:
            errs.append(f"Errors in {target}:\n  {err}\n")

    if errs:
        return errs

    time.sleep(0.1)


def _mseed2sac(target: Path, dest_path: Path):
    """trans mseed file to sac

    Dest file name like `net.sta.khole.channel.D.year.day.sac`.

    Parameters:
        target: target file
        dest_dir: destination directory

    """
    # read stream
    st = obspy.read(target)
    st.merge(method=1, fill_value="interpolate")
    stats = st[0].stats
    # read stats
    starttime = stats.starttime
    year = str(starttime.year)
    day = f"{starttime.julday:03d}"
    # dest dir
    dest_dir = dest_path / stats.network / stats.station / year / day
    dest_dir.mkdir(parents=True, exist_ok=True)
    # dest file
    file_parts = [
        stats.network,
        stats.station,
        stats.location,
        stats.channel,
        stats.mseed.dataquality,
        year,
        day,
        starttime.strftime("%H%M%S"),
        "sac",
    ]
    dest_file = str(dest_dir / ".".join(file_parts))
    # save
    st.write(dest_file, format="SAC")


if __name__ == "__main__":
    mseed_dir_to_sac("data/perm_test", "data/perm_dest", "*HH*.D")
