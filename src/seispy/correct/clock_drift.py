import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
from obspy import UTCDateTime
import pandas as pd
from rose import get_logger
from tqdm import tqdm

_LOG_DRIFT = {
    "name": "correct",
    "file": "correct.log",
    "level": logging.INFO,
}


def correct_clock_drift(src_dir: str, dest_dir:str, drift_csv: str, max_workers: int = 4):
    """correct clocke drift

    Args:
        src_dir (str): SAC数据根目录路径
        src_dir (str): 输出修正后数据目录
        drift_csv (str): 钟漂校正参数CSV文件路径
        max_workers (int, optional): 最大并行工作进程数. Defaults to None (自动设置).
    """

    logger = get_logger(**_LOG_DRIFT)
    logger.info(f"Correct clock drift for {src_dir} with {drift_csv}")

    # 加载钟漂数据
    drift_data = _load_drift_data(drift_csv)
    logger.info(f"Loaded drift data for {len(drift_data)} stations")
    # 筛选出存在于数据目录中的台站
    valid_stations = _get_valid_stations(src_dir, drift_data.keys())
    logger.info(f"Found {len(valid_stations)} valid stations with drift data")

    # 并行处理每个台站
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_station = {
            executor.submit(
                _process_station_drift_correction,
                station, drift_data[station], src_dir, dest_dir
            ): station for station in valid_stations
        }
        
        with tqdm(total=len(valid_stations), desc="Processing stations") as pbar:
            for future in as_completed(future_to_station):
                station = future_to_station[future]
                try:
                    future.result()
                    pbar.update(1)
                    pbar.set_postfix_str(f"Completed: {station}")
                except Exception as e:
                    logger.error(f"Failed to process station {station}: {str(e)}")
                    pbar.update(1)
    
    logger.info("Drift correction complete!")


def _load_drift_data(drift_csv):
    df = pd.read_csv(
        drift_csv,
        parse_dates=["starttime", "endtime"],
        dtype={
            "station": str,
            "drift": float,
            "drift_rate": float,
        }
    )
    
    return {
        row["station"]: {
            "reference_time": UTCDateTime(row["starttime"]),
            "drift": float(row["drift"]),
            "drift_rate": float(row["drift_rate"]),
            "starttime": UTCDateTime(row["starttime"]),
            "endtime": UTCDateTime(row["endtime"]),
        } for _, row in df.iterrows()
    }


def _get_valid_stations(src_dir, drift_stations):
    """获取存在于数据目录中的台站"""
    src_path = Path(src_dir)
    return [sta for sta in drift_stations if (src_path / sta).exists()]


def _process_station_drift_correction(station_name, station_drift, src_dir, dest_dir):
    """处理单个台站的钟漂修正"""
    logger = get_logger(**_LOG_DRIFT)
    
    # 获取该台站的所有SAC文件
    station_path = Path(src_dir) / station_name
    
    processed_count = 0
    # 处理每个SAC文件
    for sac_file in station_path.rglob("*.sac"):
        try:
            st = obspy.read(sac_file)
            
            # 检查文件时间是否在钟漂数据时间范围内
            sac_time = st[0].stats.starttime
            if not (station_drift["starttime"] <= sac_time <= station_drift["endtime"]):
                continue
            
            # 应用钟漂修正
            _apply_drift_correction(st, station_drift)
            # 保存修正后的文件
            _save_corrected_file(st, sac_file, src_dir, dest_dir)

            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing {sac_file}: {str(e)}")

    logger.info(f"{station_name} complete, total {processed_count} files processed.")
    

def _apply_drift_correction(stream, station_drift):
    drift_rate = station_drift["drift_rate"]
    reference_time = station_drift["reference_time"]
    
    for tr in stream:
        # 计算中间时间和钟漂修正量
        duration = tr.stats.endtime - tr.stats.starttime
        mid_time = tr.stats.starttime + duration / 2
        correction = drift_rate * (mid_time - reference_time)
        
        # 直接平移整个trace的时间轴
        tr.stats.starttime -= correction


def _save_corrected_file(stream, original_file, src_dir, dest_dir):
    """save the corrected file, keep original structure"""
    src_path = Path(src_dir)
    dest_path = Path(dest_dir)
    
    relative_path = original_file.relative_to(src_path)
    target_dir = dest_path / relative_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)
    
    stream.write(str(target_dir / original_file.name), format="SAC")


if __name__ == "__main__":
    src_directory = "/path/to/your/data"  
    dest_directory = "/path/to/corrected/data" 
    drift_file = "/path/to/drift.csv"  
    
    correct_clock_drift(
        src_dir=src_directory,
        dest_dir=dest_directory,
        drift_csv=drift_file,
        max_workers=4
    )


