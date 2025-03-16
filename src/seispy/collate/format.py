import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from pathlib import Path

import obspy
import pandas as pd
from rose import get_logger
from tqdm import tqdm

_LOG_FORMAT = {
    "file": "format.log",
    "name": "format",
    "level": logging.INFO,
}


def format_per_event(event_data, src_path, dest_path, pattern, stations_dict):
    """
    处理单个事件目录下的所有匹配文件
    :param event_data: 包含 event_dir 和 event_info 的字典
    :param pattern: 文件匹配模式 (如 "*.LHZ.sac")
    """
    logger = get_logger(**_LOG_FORMAT)

    event_dir = event_data["event_dir"]
    event_info = event_data["event_info"]

    src_event_dir = src_path / event_dir

    # search sac files in event_dir
    sac_files = list(src_event_dir.glob(pattern))
    if not sac_files:
        logger.warning(f"No files found in {event_dir} with {pattern=}.")

    # make dest event dir
    dest_event_dir = dest_path / event_dir
    dest_event_dir.mkdir(parents=True, exist_ok=True)

    post = {"success": 0, "failed": 0}
    # process all sac files found by pattern
    for sac_file in sac_files:
        try:
            parts = sac_file.stem.split(".")
            if len(parts) < 3:
                logger.error(
                    f"Invalid filename: {sac_file.name}. "
                    "Should be `event.station.channel.sac`."
                )
                continue

            station = parts[1]
            channel = parts[2]

            # 获取台站信息
            station_info = stations_dict.get(station)
            if not station_info:
                logger.error(f"No station info for {sac_file.name}")
                continue

            # update header info
            tr = obspy.read(str(sac_file))[0]
            # change stats
            stats = tr.stats
            stats.station = station
            stats.channel = channel
            if not stats.location:
                stats.location = "10"  # khole
            # update sac
            sac = stats.sac
            # station info
            sac.stla = station_info["latitude"]
            sac.stlo = station_info["longitude"]
            if station_info.get("elevation"):
                sac.stel = station_info["elevation"]
            if station_info.get("depth"):
                sac.stdp = station_info["depth"]
            # event info
            sac.evla = event_info["latitude"]
            sac.evlo = event_info["longitude"]
            if event_info.get("elevation"):
                sac.evel = event_info["elevation"]
            if event_info.get("depth"):
                sac.evdp = event_info["depth"]
            sac.mag = event_info["mag"]
            sac.lcalda = 1  # 0=FALSE, 1=TRUE

            dest_file = dest_event_dir / f"{event_dir}.{station}.{channel}.sac"
            tr.write(str(dest_file), format="SAC")
            post["success"] += 1

        except Exception as e:
            logger.error(f"failed format: {sac_file.name}: {str(e)}")
            # 如果写入失败，清理不完整文件
            if dest_file.exists():
                dest_file.unlink()
            post["failed"] += 1
    return post


def format_head(
    src_dir: str,
    dest_dir: str,
    events_csv: str,
    stations_csv: str,
    pattern: str = "*.sac",
    max_workers: int = 4,
):
    """
    :param src_dir: 源数据根目录
    :param dest_dir: 目标数据根目录
    :param pattern: 文件匹配模式 (如 "*.HHZ.sac")
    """
    src_path = Path(src_dir)
    dest_path = Path(dest_dir)

    # 读取事件数据
    events_df = pd.read_csv(events_csv, parse_dates=["time"])
    events_df["time"] = pd.to_datetime(events_df["time"], utc=True)
    events_df["event_dir"] = events_df["time"].dt.strftime("%Y%m%d%H%M%S")
    events_dict = events_df.set_index("event_dir").to_dict("index")

    # 读取台站数据
    stations_df = pd.read_csv(stations_csv)
    stations_dict = stations_df.set_index("station").to_dict("index")

    # 准备事件任务列表
    event_tasks = []
    for event_dir in [d.name for d in src_path.iterdir() if d.is_dir()]:
        event_info = events_dict.get(event_dir)
        if not event_info:
            raise ValueError(f"Warning: No event info found for {event_dir}")
        event_tasks.append({"event_dir": event_dir, "event_info": event_info})

    logger = get_logger(**_LOG_FORMAT)
    logger.info("=" * 60)
    logger.info(f"Starting SAC head format from {src_dir} to {dest_dir}.")
    logger.info(f"Search pattern: {pattern}.")
    logger.info(f"Found {len(event_tasks)} events to process")
    logger.info(
        "**Notice: this will replace station and channel in head "
        "with name in the sac file.**"
    )

    # 创建处理函数的部分应用
    processor = partial(
        format_per_event,
        src_path=src_path,
        dest_path=dest_path,
        pattern=pattern,
        stations_dict=stations_dict,
    )

    # 使用进程池处理
    post = {"success": 0, "failed": 0}
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(processor, task): task for task in event_tasks}

        with tqdm(total=len(futures), desc="Formating events...") as pbar:
            for future in as_completed(futures):
                ipost = future.result()
                post["success"] += ipost.get("success", 0)
                post["failed"] += ipost.get("failed", 0)
                pbar.set_postfix(post)
                pbar.update()
    logger.info(f"Format complete with {post}.")
    logger.info("=" * 60 + "\n")


if __name__ == "__main__":
    format_head(
        src_dir="path/to/source",
        dest_dir="path/to/destination",
        events_csv="events.csv",
        stations_csv="stations.csv",
        pattern="*.HHZ.sac",
    )
