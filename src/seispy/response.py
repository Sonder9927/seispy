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

from rose import pather


def download_resp():
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


def combine_resp(responses, outfile, starttime=(2023, 1, 1)):
    combined_inv = None
    for resp in responses:
        inv = obspy.read_inventory(resp)
        for nt in inv:
            for st in nt:
                for cha in st:
                    if not cha.location_code:
                        cha.location_code = "01"

        if combined_inv is None:
            combined_inv = inv
        else:
            combined_inv += inv
    if combined_inv is None:
        raise ValueError("no response found")
    if starttime:
        combined_inv = _shift_starttime(combined_inv, starttime)
    combined_inv.write(outfile, format="STATIONXML")


def simple_resp(resp_all, outfile, sta_list: list[str]):
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


def remove_response(src_dir, dest_dir, resp, target_patterns=["*.sac"]):
    src_path = Path(src_dir)
    dest_path = Path(dest_dir)

    # copy structure
    ic("copying structure")
    pather.copy_structure(src_path, dest_path)

    # remove response
    ic("removing response")
    errs = []
    inv = obspy.read_inventory(resp)
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(
                subdir_remove_response,
                idir,
                inv,
                target_patterns,
                pather.path_relative(src_path, idir, dest_path),
            )
            for idir in src_path.iterdir()
            if idir.is_dir()
        }
        for future in as_completed(futures):
            result = future.result()
            errs += result


def subdir_remove_response(dir, inv, patterns, dest_dir: Path):
    # remove response
    ic(f"removing response in {dir} ...")
    targets = pather.glob(dir, "rglob", patterns, exclude_parts=["soh"])
    errs = []
    for target in tqdm(targets):
        dir_to_target = target.relative_to(dir)
        dest_file = dest_dir / dir_to_target
        dest_sac = dest_file.with_suffix(".sac")
        try:
            st = stream_removed_response(target, inv)
            st.remove_response(inventory=inv)
            st.write(str(dest_sac), format="SAC")
        except Exception as e:
            errs.append((dest_file.name, e))
            ic(f"Error occered at {target}")
    time.sleep(2)
    return errs


def stream_removed_response(file, inv):
    st = obspy.read(file)
    st.merge(method=1, fill_value="interpolate")
    for tr in st:
        tr.reamove_response(inventory=inv, pre_filt=[0.003, 0.006, 1, 2], output="DISP")
        tr.data *= 1e9
    return st


def _shift_starttime(inv, starttime):
    for net in inv:
        for sta in net:
            for cha in sta:
                cha.start_date = obspy.UTCDateTime(*starttime)
    return inv
