import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
import pandas as pd
from obspy.signal.rotate import rotate2zne
from rose import get_logger
from tqdm import tqdm

_LOG_DRIFT = {
    "name": "correct",
    "file": "correct.log",
    "level": logging.INFO,
}


def orientation(src_dir: str, dest_dir: str, cor_csv: str, max_workers: int = 4):
    """correct clocke drift

    Args:
        src_dir (str): SAC数据根目录路径
        src_dir (str): 输出修正后数据目录
        cor_csv (str): 校正参数CSV文件路径
        max_workers (int, optional): 最大并行工作进程数. Defaults to None (自动设置).
    """

    logger = get_logger(**_LOG_DRIFT)
    logger.info(f"\nCorrect orientation for {src_dir} with {cor_csv}")

    # 加载钟漂数据
    cor_data = _load_cor_data(cor_csv)
    logger.info(f"Loaded cor data for {len(cor_data)} stations")
    # 筛选出存在于数据目录中的台站
    valid_stations = _get_valid_stations(src_dir, cor_data.keys())
    logger.info(
        f"Found {len(valid_stations)} valid stations in corret info and given dir."
    )

    # 并行处理每个台站
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交所有任务
        future_to_station = {
            executor.submit(
                _process_station_orientation,
                station,
                cor_data[station],
                src_dir,
                dest_dir,
            ): station
            for station in valid_stations
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

    logger.info("Orientation correction complete!\n")


def _load_cor_data(cor_csv):
    df = pd.read_csv(
        cor_csv, dtype={"station": str, "orientation": float, "tilt": float}
    )

    return {
        row["station"]: {"azimuth": row["orientation"], "dip": row["tilt"]}
        for _, row in df.iterrows()
    }


def _get_valid_stations(src_dir, cor_stations):
    """获取存在于数据目录中的台站"""
    src_path = Path(src_dir)
    return [sta for sta in cor_stations if (src_path / sta).exists()]


def _process_station_orientation(station_name, station_cor, src_dir, dest_dir):
    """处理单个台站的钟漂修正"""
    logger = get_logger(**_LOG_DRIFT)

    # 获取该台站的所有SAC文件
    station_path = Path(src_dir) / station_name

    processed_count = 0
    pattern = "*.BHZ.*sac"
    # 处理每个SAC文件
    for zsac in station_path.rglob(pattern):
        nsac = zsac.with_name(zsac.name.replace(".BHZ.", ".BHN."))
        esac = zsac.with_name(zsac.name.replace(".BHZ.", ".BHE."))
        try:
            # 应用钟漂修正
            ztr, ntr, etr = _apply_rotate2zne(
                zsac, nsac, esac, station_cor["azimuth"], station_cor["dip"]
            )
            # 保存修正后的文件
            _save_corrected_file(ztr, zsac, ntr, nsac, etr, esac, src_dir, dest_dir)

            processed_count += 1

        except Exception as e:
            logger.error(f"Error processing {nsac}: {str(e)}")

    if processed_count == 0:
        logger.warning(
            f"No files processed for {station_name} by searching pattern `{pattern}`."
        )
    else:
        logger.info(
            f"{station_name} complete, total {processed_count}X2 files processed."
        )


def _apply_rotate2zne(zsac, nsac, esac, theta, dip=0):
    ztr = obspy.read(zsac)[0]
    ntr = obspy.read(nsac)[0]
    etr = obspy.read(esac)[0]

    ztr.data, ntr.data, etr.data = rotate2zne(
        ztr.data,
        0,
        -90 + dip,
        ntr.data,
        0 + theta,
        dip,
        etr.data,
        (90 + theta) % 360,
        dip,
    )
    return ztr, ntr, etr


def _save_corrected_file(ztr, og_zsac, ntr, og_nsac, etr, og_esac, src_dir, dest_dir):
    """save the corrected file, keep original structure"""
    src_path = Path(src_dir)
    dest_path = Path(dest_dir)

    relative_path = og_zsac.relative_to(src_path)
    target_dir = dest_path / relative_path.parent
    target_dir.mkdir(parents=True, exist_ok=True)

    # ztr.write(str(target_dir / og_zsac.name), format="SAC")
    ntr.write(str(target_dir / og_nsac.name), format="SAC")
    etr.write(str(target_dir / og_esac.name), format="SAC")


if __name__ == "__main__":
    src_directory = "/path/to/your/data"
    dest_directory = "/path/to/corrected/data"
    cor_csv = "/path/to/cor.csv"

    orientation(
        src_dir=src_directory, dest_dir=dest_directory, cor_csv=cor_csv, max_workers=4
    )
