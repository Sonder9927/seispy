"""
instrument responses from GEONET
"""

from pathlib import Path

import obspy
from obspy import UTCDateTime
from obspy.clients.fdsn import Client


def download_response(filename, client="GEONET", network="NZ", **kwargs):
    client = Client(client)
    start = kwargs.get("starttime") or (2023, 1, 1)
    starttime = UTCDateTime(*start)
    end = kwargs.get("endtime") or (2024, 12, 12)
    endtime = UTCDateTime(*end)

    client.get_stations(
        network=network,
        starttime=starttime,
        endtime=endtime,
        level="response",
        filename=filename,
        format="xml",
        minlatitude=kwargs.get("minlatitude") or -50,
        maxlatitude=kwargs.get("maxlatitude") or -30,
        minlongitude=kwargs.get("minlongitude") or 160,
        maxlongitude=kwargs.get("maxlongitude") or 180,
    )


def combine_responses(
    responses: list[str | Path], outfile=None, starttime=(2023, 1, 1)
):
    """combine response files

    Parameters:
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
