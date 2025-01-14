"""
instrument responses from GEONET
"""

from pathlib import Path

import obspy
from obspy import UTCDateTime
from obspy.clients.fdsn import Client


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
    combined_inv = obspy.Inventory()
    for resp in responses:
        inv = obspy.read_inventory(resp)
        combined_inv += inv
    if not len(combined_inv):
        raise ValueError("no response found")

    if starttime:
        combined_inv = _shift_starttime(combined_inv, starttime)

    if outfile:
        combined_inv.write(outfile, format="STATIONXML")

    return combined_inv


def simple_response(resp_all, sta_list: list[str], outfile=None):
    inv = obspy.read_inventory(resp_all)
    # simple_inv = obspy.core.inventory.inventory.Inventory(
    simple_inv = obspy.Inventory(networks=[], source=inv.source)

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
    if outfile:
        simple_inv.write(outfile, format="STATIONXML")

    return simple_inv


def _shift_starttime(inv, starttime):
    for net in inv:
        for sta in net:
            for cha in sta:
                cha.start_date = obspy.UTCDateTime(*starttime)
    return inv
