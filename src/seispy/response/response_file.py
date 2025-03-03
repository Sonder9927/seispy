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


def filter(resp, sta_list: list[str], cha_list=[], outfile=None):
    """
    从 resp 文件中过滤特定台站和通道。

    :param resp: 原始 RESP/StationXML 文件路径
    :param stationlist: 需要保留的台站列表（如 ["ST01", "ST02"]）
    :param channels: 需要保留的通道列表（如 ["HHZ", "BHZ"]）
    :param outfile: 过滤后的 XML 输出路径
    """
    from obspy.core.inventory import Inventory, Network, Station

    # 读取原始 XML
    inv = obspy.read_inventory(resp)
    # 创建新的 Inventory 容器
    filtered_networks = []

    for net in inv:
        filtered_stations = []
        for sta in net:
            if sta.code in sta_list:
                if cha_list:
                    # 筛选该台站的通道
                    filtered_channels = [ch for ch in sta if ch.code in cha_list]
                else:
                    filtered_channels = sta.channels

                # 只有当台站包含需要的通道时才添加
                if filtered_channels:
                    new_station = Station(
                        code=sta.code,
                        latitude=sta.latitude,
                        longitude=sta.longitude,
                        elevation=sta.elevation,
                        site=sta.site,
                        channels=filtered_channels,
                    )
                    filtered_stations.append(new_station)

        if filtered_stations:
            new_network = Network(code=net.code, stations=filtered_stations)
            filtered_networks.append(new_network)

    # 生成新的 Inventory
    filtered_inventory = Inventory(networks=filtered_networks, source=inv.source)

    if outfile:
        filtered_inventory.write(outfile, format="STATIONXML")
    return filtered_inventory


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
