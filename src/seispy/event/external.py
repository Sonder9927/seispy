"""Cut event windows with the external SAC toolchain."""

import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from tqdm import tqdm

from seispy.workflow import resolve_separate_directory_trees


def _bundled_command(command: str, bin_path: str = "bin") -> Path:
    """Resolve one command from the event-cutting toolchain."""
    path = Path(bin_path) / command
    if not path.exists():
        raise FileNotFoundError(f"{command=} not found in {bin_path}.")
    return path


def cut_events_binary(
    net_dir: str | Path,
    dest_dir: str | Path,
    event_file: str | Path,
    time_window: int = 10800,
    max_workers: int = 4,
) -> None:
    """Cut event windows with the external ``cutevent`` toolchain.

    Args:
        net_dir: One network directory containing continuous data by station.
        dest_dir: Destination root for event waveform files.
        event_file: Catalog created by :func:`write_event_catalog`.
        time_window: Window length after each origin, in seconds.
        max_workers: Maximum number of station worker processes.

    Raises:
        ValueError: If the input and output directory trees overlap.
        subprocess.CalledProcessError: If an external command fails.

    Examples:
        ```python
        cut_events_binary(
            "data/continuous", "data/events", "events.cat",
            time_window=10_800, max_workers=1,
        )
        ```
    """
    network_dir, output_dir = resolve_separate_directory_trees(net_dir, dest_dir)
    stations = sorted(path for path in network_dir.iterdir() if path.is_dir())
    mktraceiodb = _bundled_command("mktraceiodb")
    cutevent = _bundled_command("cutevent")
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _cut_station,
                index,
                station,
                mktraceiodb,
                cutevent,
                event_file,
                time_window,
                output_dir,
            )
            for index, station in enumerate(stations)
        }
        with tqdm(total=len(futures), desc="Processing stations") as bar:
            for future in as_completed(futures):
                future.result()
                bar.update(1)


def _cut_station(
    index, station, mktraceiodb, cutevent, event_file, time_window, dest_dir
):
    paths = [Path(f"data_z.{suffix}_{index}") for suffix in ("lst", "db", "done")]
    listing, database, completed_list = paths
    try:
        with listing.open("w") as file:
            for sac in station.rglob("*.sac"):
                file.write(f"{sac}\n")
                stream = obspy.read(sac)
                if not stream[0].stats.location:
                    stream[0].stats.location = "10"
                    stream.write(str(sac), format="SAC")
        commands = (
            f"{mktraceiodb} -L {completed_list} -O {database} -LIST {listing} -V\n"
            f"{cutevent} -V -ctlg {event_file} -tbl {database} -b +0 "
            f"-e +{time_window} -out {dest_dir}\n"
        )
        subprocess.run(["bash"], input=commands.encode(), check=True)
    finally:
        for path in paths:
            path.unlink(missing_ok=True)
