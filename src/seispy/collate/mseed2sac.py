import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Union

import obspy
from rose import batch_generator, get_logger
from tqdm import tqdm

_LOG_MSEED2SAC = {
    "name": "mseed2sac",
    "file": "mseed2sac.log",
    "level": logging.INFO,
}


def mseed2sac_dir(
    src_dir: Union[Path, str],
    dest_dir: Union[Path, str],
    pattern: str = "*.miniseed",
    batch_size: int = 2000,
    max_workers: int = 5,
) -> None:
    """MiniSEED 转 SAC 主函数

    Args:
        src_dir: 源目录路径
        dest_dir: 目标目录路径
        pattern: 文件匹配模式
        batch_size: 每批处理文件数
        max_workers: 最大并行进程数
        log_file: 自定义日志文件路径（可选）
    """

    logger = get_logger(**_LOG_MSEED2SAC)
    logger.info("Mseed2sac started")

    src_path = Path(src_dir)
    dest_base = Path(dest_dir)
    dest_base.mkdir(parents=True, exist_ok=True)

    # 获取文件列表
    file_paths = list(src_path.rglob(pattern))
    total_files = len(file_paths)
    logger.info(f"Found {total_files} files")

    # 多进程处理
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_batch, batch, dest_base)
            for batch in batch_generator(file_paths, batch_size)
        }

        with tqdm(total=total_files, desc="Processing") as pbar:
            success = 0
            for future in as_completed(futures):
                isuccess = future.result()
                success += isuccess
                pbar.update(isuccess)
                pbar.set_postfix({"success": success})

    logger.info(f"Mseed2sac complete. Success: {success}/{total_files}")
    print(f"All done! Check {_LOG_MSEED2SAC['file']}")


def _process_batch(file_paths: list[Path], dest_dir: Path) -> int:
    """处理单个批次"""
    logger = get_logger(**_LOG_MSEED2SAC)
    success = 0
    for path in file_paths:
        try:
            mseed2sac(path, dest_dir)
            success += 1
        except Exception as e:
            logger.error(f"Failed {path.name}: {str(e)}", exc_info=True)
    return success


def mseed2sac(mseed_path: Path, dest_base: Path) -> None:
    """转换单个文件"""
    logger = get_logger(**_LOG_MSEED2SAC)

    stream = obspy.read(mseed_path)
    stream.merge(method=1, fill_value="interpolate")

    for trace in stream:
        stats = trace.stats
        start = stats.starttime

        dest_path = build_destination_path(
            network=stats.network,
            station=stats.station,
            year=start.year,
            julday=start.julday,
            location=stats.location,
            channel=stats.channel,
            data_quality=stats.mseed.dataquality,
            dest_base=dest_base,
        )

        trace.write(str(dest_path), format="SAC")
    logger.debug(f"Converted: {mseed_path.name} -> {dest_path.parent}")


def build_destination_path(
    network: str,
    station: str,
    year: int,
    julday: int,
    location: str,
    channel: str,
    data_quality: str,
    dest_base: Path,
) -> Path:
    """构建文件存储路径

    Parameters:
    network: 台网代码 (如"XB")
    station: 台站代码 (如"CD01")
    year: 年份 (完整四位数)
    julday: 儒略日 (1-366)
    location: 位置标识 (如"00")
    channel: 通道代码 (如"HHZ")
    data_quality: 数据质量标识
    dest_base: 基础存储路径

    Return:
    Path: 完整文件路径

    Examples:
    >>> path = build_destination_path(
            Network("XB"), Station("CD01"), 2023, 123,
            Location("00"), Channel("HHZ"), DataQuality("D"),
            Path("/data"), ".sac"
        )
    >>> print(path)
    /data/XB/CD01/2023/123/XB.CD01.00.HHZ.D.2023.123.sac
    """
    dir_path = dest_base / f"{network}/{station}/{year:04d}/{julday:03d}"
    dir_path.mkdir(parents=True, exist_ok=True)

    filename = (
        f"{network}.{station}.{location}.{channel}."
        f"{data_quality}.{year:04d}.{julday:03d}.sac"
    )
    return dir_path / filename


if __name__ == "__main__":
    mseed2sac_dir("data/perm_test", "data/perm_dest", pattern="*HH*.D")
