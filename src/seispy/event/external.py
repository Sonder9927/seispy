"""Cut event windows with the external SAC toolchain."""

import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tempfile import TemporaryDirectory

import obspy
from obspy.io.sac import SACTrace
from seispy.event.catalog import _integer_second_window
from seispy._main_guard import require_reimport_safe_entry_point
from seispy.progress import call_with_warnings, progress_bar, resolve_worker_call

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
        time_window: Window length beginning at the origin floored to an
            integer UTC second. The precise origin is retained in SAC ``o``.
        max_workers: Maximum number of station worker processes.

    Raises:
        ValueError: If the input and output directory trees overlap, or multiple
            events fall in the same integer second and would overwrite each other.
        subprocess.CalledProcessError: If an external command fails.

    Examples:
        ```python
        cut_events_binary(
            "data/continuous", "data/events", "events.cat",
            time_window=10_800, max_workers=1,
        )
        ```
    """
    require_reimport_safe_entry_point("cut_events_binary")
    network_dir, output_dir = resolve_separate_directory_trees(net_dir, dest_dir)
    stations = sorted(path for path in network_dir.iterdir() if path.is_dir())
    mktraceiodb = _bundled_command("mktraceiodb")
    cutevent = _bundled_command("cutevent")
    with TemporaryDirectory(prefix="seispy-events-") as temporary:
        catalog = Path(temporary) / "events.cat"
        origins = _integer_second_catalog(event_file, catalog)
        _cut_network(
            stations,
            mktraceiodb,
            cutevent,
            catalog,
            time_window,
            output_dir,
            max_workers,
            origins,
        )


def _integer_second_catalog(source, destination):
    """Normalize only the disposable binary catalog, preserving all columns."""
    origins = {}
    rows = []
    for line in Path(source).read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        columns = line.split()
        origin = obspy.UTCDateTime(columns[0].replace("/", "-").replace(",", "T"))
        start = _integer_second_window({"start": origin, "end": origin})["start"]
        if start.ns in origins:
            raise ValueError(
                "Binary cutter cannot represent multiple events in one integer second"
            )
        origins[start.ns] = origin
        columns[0] = start.strftime("%Y/%m/%d,%H:%M:%S.000000")
        rows.append(" ".join(columns) + "\n")
    Path(destination).write_text("".join(rows))
    return origins


def _cut_network(
    stations,
    mktraceiodb,
    cutevent,
    event_file,
    time_window,
    output_dir,
    max_workers,
    origins,
):
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                call_with_warnings,
                _cut_station,
                index,
                station,
                mktraceiodb,
                cutevent,
                event_file,
                time_window,
                output_dir,
                origins,
            )
            for index, station in enumerate(stations)
        }
        with progress_bar(
            total=len(futures), desc="Processing stations", unit="station"
        ) as bar:
            for future in as_completed(futures):
                resolve_worker_call(future.result())
                bar.update(1)


def _cut_station(
    index,
    station,
    mktraceiodb,
    cutevent,
    event_file,
    time_window,
    dest_dir,
    origins,
):
    paths = [Path(f"data_z.{suffix}_{index}") for suffix in ("lst", "db", "done")]
    listing, database, completed_list = paths
    identities = set()
    try:
        with listing.open("w") as file:
            for sac in station.rglob("*.sac"):
                file.write(f"{sac}\n")
                stream = obspy.read(sac)
                identities.add((stream[0].stats.network, stream[0].stats.station))
                if not stream[0].stats.location:
                    stream[0].stats.location = "10"
                    stream.write(str(sac), format="SAC")
        commands = (
            f"{mktraceiodb} -L {completed_list} -O {database} -LIST {listing} -V\n"
            f"{cutevent} -V -ctlg {event_file} -tbl {database} -b +0 "
            f"-e +{time_window} -out {dest_dir}\n"
        )
        subprocess.run(["bash"], input=commands.encode(), check=True)
        # The legacy C writer takes its reference and origin from the temporary
        # whole-second catalog. Restore the real origin without changing samples.
        for epoch, origin in origins.items():
            reference = obspy.UTCDateTime(ns=epoch)
            if origin == reference:
                continue
            directory = Path(dest_dir) / reference.strftime("%Y%m%d%H%M%S")
            for network, name in identities:
                for output in directory.glob(f"{network}.{name}.*.sac"):
                    header = SACTrace.read(str(output), headonly=True)
                    header.o = origin - reference
                    header.iztype = "iunkn"
                    header.write(str(output), headonly=True)
    finally:
        for path in paths:
            path.unlink(missing_ok=True)
