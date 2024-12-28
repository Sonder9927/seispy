"""
instrument responses from GEONET
"""

import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from icecream import ic
from obspy import UTCDateTime
from obspy.clients.fdsn import Client
from tqdm import tqdm

from rose import pather, write_errors


def download_response():
    client = Client("GEONET")
    starttime = UTCDateTime(2023, 1, 1)
    endtime = UTCDateTime(2024, 12, 12)

    client.get_stations(
        network="NZ",
        starttime=starttime,
        endtime=endtime,
        level="response",
        filename="response.xml",
        format="xml",
        minlatitude=-45,
        maxlatitude=-30,
        minlongitude=170,
        maxlongitude=180,
    )


def combine_responses(
    responses: list[str | Path], outfile=None, starttime=(2023, 1, 1)
):
    """combine respons files

    Args:
        responses: response files
        outfile (_type_): output file
        starttime (tuple, optional): shift stream's starttime.

    Raises:
        ValueError: No response found

    Returns:
        _type_: stream
    """
    combined_inv = None
    for resp in responses:
        inv = obspy.read_inventory(resp)

        if combined_inv is None:
            combined_inv = inv
        else:
            combined_inv += inv
    if combined_inv is None:
        raise ValueError("no response found")
    if starttime:
        combined_inv = _shift_starttime(combined_inv, starttime)
    if outfile:
        combined_inv.write(outfile, format="STATIONXML")
    return combined_inv


def simple_response(resp_all, outfile, sta_list: list[str]):
    inv = obspy.read_inventory(resp_all)
    simple_inv = obspy.core.inventory.inventory.Inventory(
        networks=[], source=inv.source
    )

    net = inv[0]
    new_net = obspy.core.inventory.network.Network(
        code=net.code,
        description=net.description,
        start_date=net.start_date,
        end_date=net.end_date,
        total_number_of_stations=net.total_number_of_stations,
        selected_number_of_stations=net.selected_number_of_stations,
    )

    for sta in net:
        if sta.code in sta_list:
            # sta.channels=[
            #     cha for cha in sta.channels if not cha.code.startswith("LH")
            # ]
            new_net.stations.append(sta)

    if new_net.stations:
        simple_inv.networks.append(new_net)
    simple_inv.write(outfile, format="STATIONXML")


def deconvolution_last_subdirs(
    src_dir: str | Path,
    resp: str,
    resample: float | None = None,
    pattern: str = "*.sac",
    remove_src: bool = True,
) -> None:
    """deconvolution last subdirs (days)

    remove response by last directories

    Parameters:
        src_dir: source directory
        resp: response file
        resample: resample to given delta if it isn't None.
        pattern: search pattern at last subdirectory
        remove_src: remove source file after deconvolution
    """
    src_path = Path(src_dir)
    last_subdirs = pather.find_last_subdirs(src_path)

    # remove response
    ic("reading response ...")
    inv = obspy.read_inventory(resp)
    ic("removing response ...")
    errs = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(
                targets_remove_response,
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


def targets_remove_response(
    dir: Path, pattern, inv, resample, remove_src
) -> str | None:
    try:
        for target in dir.glob(pattern):
            st = stream_removed_response(target, inv, resample)
            dest_sac = target.with_suffix(".deconv.sac")
            st.write(str(dest_sac), format="SAC")
            if remove_src:
                target.unlink()
    except Exception as e:
        return f"Error occered at {target} : {e}\n"

    time.sleep(1)


def stream_removed_response(file: str | Path, inv, resample: float | None = None):
    """remove response from sac file

    Args:
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


def _shift_starttime(inv, starttime):
    for net in inv:
        for sta in net:
            for cha in sta:
                cha.start_date = obspy.UTCDateTime(*starttime)
    return inv
