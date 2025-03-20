import datetime
import logging
import subprocess
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import obspy
import pandas as pd
from obspy import UTCDateTime
from rose import get_logger, pather
from tqdm import tqdm

_LOG_CUTEVENT = {
    "file": "cutevent.log",
    "name": "cut_event",
    "level": logging.INFO,
}


def cut_events(src_dir, dest_dir, event_csv, station_csv=None, time_window=10800):
    logger = get_logger(**_LOG_CUTEVENT)
    logger.info("Start cutting events...")

    # load events and stations
    events = _load_events(event_csv, time_window)
    stations = _load_stations(src_dir, station_csv)
    logger.info(f"Found {len(stations)} stations and {len(events)} events.")

    total = len(events) * len(stations)
    with tqdm(total=total, desc="Processing...") as pbar:
        for station in stations:
            for event in events:
                cut_event_station(event, station, src_dir, dest_dir)
                pbar.update(1)
    logger.info("Cut events complete.")
    print(f"Cut events complete. Check {_LOG_CUTEVENT['file']} for details.")


def cut_event_station(event, station, src_dir, dest_dir):
    """处理单个事件-台站组合"""
    logger = get_logger(**_LOG_CUTEVENT)
    # 生成时间覆盖范围
    year_jdays = _calculate_julian_dates(event["start"], event["end"])

    # 获取所有可能相关的SAC文件路径
    station_name = station["station"]
    sac_files = _target_paths(src_dir, station_name, year_jdays)
    if not sac_files:
        logger.warning(
            f"No SAC files found for {station_name=} {event['start']=}."
            "Expect sac name like `*.{year}.{jday}.*.sac`."
        )

    # collect all channels data
    channel_data = {}

    for sac_path in sac_files:
        try:
            # 精确读取所需数据段
            st = obspy.read(sac_path)
            tr = st[0]

            sta = tr.stats.station
            # check station
            if sta != station["station"]:
                logger.error(f"File name mismatch: {sac_path}.")
                continue

            channel = tr.stats.channel

            # 合并通道数据
            if channel in channel_data:
                channel_data[channel] += st
            else:
                channel_data[channel] = st

        except Exception as e:
            logger.error(f"Error processing file {sac_path}: {e}")

    # save channel data
    if channel_data:
        event_name = event["start"].strftime("%Y%m%d%H%M%S")
        event_dir = Path(dest_dir) / event_name
        event_dir.mkdir(parents=True, exist_ok=True)

        for channel, stream in channel_data.items():
            try:
                merged_st = stream.merge(method=1, fill_value="interpolate")
                merged_tr = merged_st[0]
                trimed_tr = _trimed_trace(merged_tr, event, station)
                out_name = f"{event_name}.{station_name}.{channel}.sac"
                trimed_tr.write(str(event_dir / out_name), format="SAC")
            except Exception as e:
                logger.error(f"{event_name} {channel} failed: {e}")


def _trimed_trace(merged_tr, event, station):
    # delta
    delta = np.float16(merged_tr.stats.delta)
    merged_tr.stats.delta = delta
    # trim to event time window
    trimed_tr = merged_tr.trim(event["start"], event["end"], nearest_sample=True)

    # header information
    data = trimed_tr.data
    npts = len(data)
    delta = trimed_tr.stats.delta
    if npts == 0:
        raise ValueError("Empty data after trim")
    start = trimed_tr.stats.starttime
    time_offset = start - event["start"]

    # update header
    if not trimed_tr.stats.location:
        trimed_tr.stats.location = "10"  # khole
    header_updates = {
        "delta": delta,
        "b": float(time_offset),
        "e": float(time_offset) + (npts - 1) * delta,
        "depmin": np.min(data),
        "depmax": np.max(data),
        "depmen": np.mean(data),
        "nzyear": start.year,
        "nzjday": start.julday,
        "nzhour": start.hour,
        "nzmin": start.minute,
        "nzsec": start.second,
        "nzmsec": start.microsecond // 1000,
        # necessary info of event
        "evla": event["latitude"],
        "evlo": event["longitude"],
        "evdp": event["depth"],
        "mag": event["mag"],
        "lcalda": 1,
        # optional info of station
        "stla": station.get("latitude", -12345),
        "stlo": station.get("longitude", -12345),
        "stel": station.get("elevation", -12345),
        "stdp": station.get("depth", -12345),
        # 参考时间 o 等
        # "o": 0.0,
    }

    trimed_tr.stats.sac.update(header_updates)
    return trimed_tr


def _load_events(catalog, time_window):
    df = pd.read_csv(
        catalog,
        parse_dates=["time"],
        dtype={
            "latitude": float,
            "longitude": float,
            "depth": float,
            "mag": float,
        },
    )

    events = []
    for _, row in df.iterrows():
        starttime = UTCDateTime(int(row["time"].timestamp()))  # 对齐到整数秒

        events.append(
            {
                "start": starttime,
                "end": starttime + time_window,
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "depth": float(row["depth"]),
                "mag": float(row["mag"]),
            }
        )
    return events


def _load_stations(src_dir, station_csv) -> list[dict]:
    """
    加载并验证台站信息

    :param src_dir: 数据目录，子目录名为台站名
    :param station_csv: 可选台站元数据CSV文件
    :return: 台站信息字典列表
    :raises ValueError: 当CSV与目录台站不匹配时
    """
    # get all stations under src_dir
    target_stations = {d.name for d in Path(src_dir).iterdir() if d.is_dir()}
    if not station_csv:
        return [{"station": s} for s in target_stations]

    df = pd.read_csv(station_csv)
    csv_stations = set(df["station"])
    miss_in_csv = target_stations - csv_stations
    if miss_in_csv:
        raise ValueError(
            f"{len(miss_in_csv)} stations missiong in CSV:"
            f"{sorted(miss_in_csv)[:5]}{'...' if len(miss_in_csv) > 5 else ''}"
        )

    return df[df["station"].isin(target_stations)].to_dict("records")


def _calculate_julian_dates(start, end):
    """计算时间范围内包含的所有儒略日"""
    dates = set()
    current = start.datetime
    end = end.datetime

    while current <= end:
        year = current.year
        jday = current.timetuple().tm_yday
        dates.add((year, f"{jday:03d}"))
        current += datetime.timedelta(days=1)

    return sorted(dates)


def _target_paths(sac_base, station, year_jdays):
    """构建预测的SAC文件路径"""
    valid_paths = []
    for year, jday in year_jdays:
        dir_path = Path(sac_base) / station / str(year) / jday
        if not dir_path.exists():
            continue

        # 预期文件名模式：*.{year}.{jday}.*.sac
        pattern = f"*.{year}.{jday}.*.sac"
        valid_paths.extend(dir_path.glob(pattern))

    return valid_paths


def cut_events_bin(
    src_dir: str,
    dest_dir: str,
    evtf: str,
    time_window: int = 10800,
    max_workers: int = 4,
) -> None:
    src_path = Path(src_dir)
    station_paths = list(src_path.glob("*/"))
    total = len(station_paths)

    # binarary
    mktraceiodb = pather.binuse("mktraceiodb")
    cutevent = pather.binuse("cutevent")

    # cut events con
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                cut_events_per_station,
                id,
                sta_path,
                mktraceiodb,
                cutevent,
                evtf,
                time_window,
                dest_dir,
            )
            for id, sta_path in enumerate(station_paths)
        }
        with tqdm(total=total, desc="Processing stations") as pbar:
            for future in as_completed(futures):
                future.result()
                pbar.update(1)

    print("Cut events complete.")


def cut_events_per_station(
    id: int,
    sta_path: Path,
    mktraceiodb,
    cutevent,
    evtf: str,
    time_window: int,
    dest_dir: str,
) -> None:
    lst = Path(f"data_z.lst_{id}")
    db = Path(f"data_z.db_{id}")
    done_lst = Path(f"done_z.lst_{id}")

    with lst.open("w") as f:
        for sac in sta_path.rglob("*.sac"):
            f.write(str(sac) + "\n")
            st = obspy.read(sac)
            stats = st[0].stats
            if not stats.location:
                stats.location = "10"
                st.write(str(sac), format="SAC")

    cmd_str = "echo shell start\n"
    cmd_str += f"{mktraceiodb} -L {done_lst} -O {db} -LIST {lst} -V\n"
    cmd_str += f"{cutevent} -V -ctlg {evtf} -tbl {db} -b +0 -e +{time_window} -out {dest_dir}\n"
    cmd_str += "echo shell end\n"
    subprocess.Popen(["bash"], stdin=subprocess.PIPE).communicate(cmd_str.encode())

    lst.unlink()
    db.unlink()
    done_lst.unlink()


if __name__ == "__main__":
    cut_events(
        "data/events.csv",
        "data/1A",
        "data/events",
        10800,
    )
