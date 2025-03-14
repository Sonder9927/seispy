"""
instrument responses from GEONET
"""

from pathlib import Path

import obspy
from obspy import UTCDateTime
from obspy.clients.fdsn import Client


def download(filename, client="GEONET", network="NZ", **kwargs):
    client = Client(client)
    starttime = kwargs.get("starttime") or UTCDateTime(2023, 1, 1)
    endtime = kwargs.get("endtime") or UTCDateTime(2025, 1, 1)

    inv = client.get_stations(
        network=network,
        starttime=starttime,
        endtime=endtime,
        level="response",
        minlatitude=kwargs.get("minlatitude") or -50,
        maxlatitude=kwargs.get("maxlatitude") or -30,
        minlongitude=kwargs.get("minlongitude") or 160,
        maxlongitude=kwargs.get("maxlongitude") or 180,
    )
    inv.write(filename, format="STATIONXML")


def combine(responses: list[str | Path], outfile=None, starttime=(2023, 1, 1)):
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


def filter(resp, station_list: list[str], channel_list=[], outfile=None):
    """
    从 resp 文件中过滤特定台站和通道。

    :param resp: 原始 RESP/StationXML 文件路径
    :param station_list: 需要保留的台站列表 (e.g. ["WEL", "KHZ"])
    :param channel_list: 需要保留的通道列表 (e.g. ["HHZ", "HHE"])
    :param outfile: 过滤后的 XML 输出路径
    """
    from obspy.core.inventory import Inventory

    # 读取原始 resp
    inv = obspy.read_inventory(resp)

    new_inv = Inventory(networks=[], source=inv.source)
    for net in inv:
        new_net = net.copy()
        new_net.stations = []
        for sta in net:
            if sta.code not in station_list:
                continue
            new_sta = sta.copy()
            # 筛选该台站的通道
            if channel_list:
                new_sta.channels = [
                    ch for ch in sta.channels if ch.code in channel_list
                ]
            else:
                new_sta.channels = sta.channels

            # 只保留有有效通道的台站
            if new_sta.channels:
                new_net.stations.append(new_sta)

        # 只保留有有效台站的网络
        if new_net.stations:
            new_inv.networks.append(new_net)

    if outfile:
        new_inv.write(outfile, format="STATIONXML")
    return new_inv


def extract(resp_all, sta_list: list[str], outfile=None):
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
