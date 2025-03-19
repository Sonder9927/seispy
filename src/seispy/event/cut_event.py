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


def cut_events(src_dir, dest_dir, catalog, time_window=10800):
    logger = get_logger(**_LOG_CUTEVENT)
    logger.info("Start cutting events...")

    # load events
    events = _load_events(catalog, time_window)
    # get all stations under src_dir
    stations = [d.name for d in Path(src_dir).iterdir() if d.is_dir()]
    logger.info(f"Found {len(stations)} stations and {len(events)} events.")

    total = len(events) * len(stations)
    with tqdm(total=total, desc="Processing...") as pbar:
        for event in events:
            for station in stations:
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
    sac_files = _target_paths(src_dir, station, year_jdays)
    if not sac_files:
        logger.warning(
            f"No SAC files found for {station=} {event['start']=}."
            "Expect sac name like `*.{year}.{jday}.*.sac`."
        )

    # 通道数据容器
    channel_data = {}

    for sac_path in sac_files:
        try:
            # 精确读取所需数据段
            st = obspy.read(sac_path)
            tr = st[0]

            sta = tr.stats.station
            # check station
            if sta != station:
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

    # 保存结果
    if channel_data:
        event_name = event["start"].strftime("%Y%m%d%H%M%S")
        event_dir = Path(dest_dir) / event_name
        event_dir.mkdir(parents=True, exist_ok=True)

        for channel, stream in channel_data.items():
            try:
                merged_st = stream.merge(method=1, fill_value="interpolate")
                merged_tr = merged_st[0]
                trimed_tr = _trimed_trace(merged_tr, event)
                out_name = f"{event_name}.{station}.{channel}.sac"
                trimed_tr.write(str(event_dir / out_name), format="SAC")
            except Exception as e:
                logger.error(f"{event_name} {channel} failed: {e}")


def _trimed_trace(merged_tr, event):
    # trim to event time window
    trimed_tr = merged_tr.trim(event["start"], event["end"], nearest_sample=True)
    delta = np.float32(trimed_tr.stats.delta)
    merged_tr.stats.delta = delta

    # header information
    data = trimed_tr.data
    npts = len(data)
    if npts == 0:
        raise ValueError("Empty data after trim")
    start = trimed_tr.stats.starttime
    time_offset = start - event["start"]

    # update header
    header_updates = {
        "delta": delta,
        "b": float(time_offset),
        "e": float(time_offset) + (npts - 1) * delta,
        "depmin": np.min(data),
        "depmax": np.max(data),
        "depmen": np.mean(data),
        "evla": event["lat"],
        "evlo": event["lon"],
        "evdp": event["depth"],
        "mag": event["mag"],
        "lcalda": 1,
        "nzyear": start.year,
        "nzjday": start.julday,
        "nzhour": start.hour,
        "nzmin": start.minute,
        "nzsec": start.second,
        "nzmsec": start.microsecond // 1000,
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
                "lat": float(row["latitude"]),
                "lon": float(row["longitude"]),
                "depth": float(row["depth"]),
                "mag": float(row["mag"]),
            }
        )
    return events


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
