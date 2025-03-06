import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

import obspy
from icecream import ic
from rose import batch_generator, logger, pather, write_errors
from tqdm import tqdm


def mseed2sac_dir(
    src_dir: Path | str,
    dest_dir: Path | str,
    pattern: str = "*.miniseed",
    batch_size: int = 2000,
    max_workers: int = 5,
) -> None:
    """主转换函数"""
    _logger = logger(name="mseed2sac", log_file=Path("mseed2sac.log"))
    _logger.info("Conversion process started")
    dest_base = Path(dest_dir)
    # 获取源目录下所有符合pattern的文件路径
    file_paths = pather.glob(src_dir, "rglob", [pattern])
    # 获取文件总数
    total_files = len(file_paths)
    _logger.debug(f"Total files to process: {total_files}")

    # 使用ProcessPoolExecutor创建一个进程池，最大进程数为max_workers
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交任务到进程池，每个任务处理一个batch
        futures = {
            executor.submit(process_batch, batch, dest_base, _logger)
            for batch in batch_generator(file_paths, batch_size)
        }

        # 使用tqdm创建一个进度条，显示总文件数和当前进度
        with tqdm(total=total_files, desc="Processing files") as progress:
            # 遍历任务，获取任务结果并更新进度条
            for future in as_completed(futures):
                processed = future.result()
                progress.update(processed)

    # 打印转换完成信息
    _logger.info("Conversion process completed")
    ic("All conversions finished")


def process_batch(file_paths: Iterable[Path], dest_dir: Path, _logger) -> int:
    """处理文件批次并返回成功处理数量"""
    success_count = 0
    for path in file_paths:
        try:
            mseed2sac(path, dest_dir, _logger)
            success_count += 1
        except Exception as e:
            _logger.error(f"Failed {path.name}: {str(e)}", exc_info=True)
    return success_count


def mseed2sac(mseed_path: Path, dest_base: Path, _logger=None):
    stream = obspy.read(mseed_path)
    stream.merge(method=1, fill_value="interpolate")

    for trace in stream:
        stats = trace.stats
        start_time = stats.starttime

        dest_path = build_destination_path(
            network=stats.network,
            station=stats.station,
            year=start_time.year,
            julday=start_time.julday,
            location=stats.location,
            channel=stats.channel,
            data_quality=stats.mseed.dataquality,
            dest_base=dest_base,
            suffix=".sac",
        )

        trace.write(str(dest_path), format="SAC")
    if _logger:
        _logger.debug(f"Converted: {mseed_path} -> {dest_path}")


def build_destination_path(
    network: str,
    station: str,
    year: int,
    julday: int,
    location: str,
    channel: str,
    data_quality: str,
    dest_base: Path,
    suffix: str = ".sac",
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
    suffix: 文件扩展名 (默认.sac)

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
    year_str = f"{year:04d}"
    day_str = f"{julday:03d}"
    dir_path = dest_base / network / station / year_str / day_str
    dir_path.mkdir(parents=True, exist_ok=True)

    filename = (
        f"{network}.{station}.{location}.{channel}."
        f"{data_quality}.{year_str}.{day_str}{suffix}"
    )

    return dir_path / filename


def mseed_dir_to_sac(src: str | Path, dest: str | Path, pattern: str = "*.mseed"):
    """trans mseed files to sac and sort structure of destnation

    Dest file name like `net.sta.khole.channel.D.year.day.sac`.

    Parameters:
        src_dir: source directory
        dest_dir: destination directory
        pattern: search pattern of target files

    Examples:
        >>>import seispy
        >>>seispy.mseed_dir_to_sac('/path/src', '/path/dest', '*.miniseed')
    """
    src_path = Path(src)
    dest_path = Path(dest)
    targets = pather.glob(src_path, "rglob", [pattern])
    ltargets = len(targets)
    nbach = 2000
    errs = []
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_mseeds_to_sac, targets[i : i + nbach], dest_path)
            for i in range(0, ltargets, nbach)
        }
        for future in tqdm(
            as_completed(futures),
            total=len(futures),
            mininterval=2,
            desc="Mseeds to sac ...",
        ):
            future = future.result()
            if future:
                errs += future
    if errs:
        write_errors(errs)
    ic("All Converted with NO errors!")


def _mseeds_to_sac(targets: list[Path], dest_path: Path):
    errs = []
    for target in targets:
        try:
            _mseed2sac(target, dest_path)
        except Exception as err:
            errs.append(f"Errors in {target}:\n  {err}\n")

    if errs:
        return errs

    time.sleep(0.1)


def _mseed2sac(target: Path, dest_path: Path):
    """trans mseed file to sac

    Dest file name like `net.sta.khole.channel.D.year.day.sac`.

    Parameters:
        target: target file
        dest_dir: destination directory

    """
    # read stream
    st = obspy.read(target)
    st.merge(method=1, fill_value="interpolate")
    stats = st[0].stats
    # read stats
    starttime = stats.starttime
    year = str(starttime.year)
    day = f"{starttime.julday:03d}"
    # dest dir
    dest_dir = dest_path / stats.network / stats.station / year / day
    dest_dir.mkdir(parents=True, exist_ok=True)
    # dest file
    file_parts = [
        stats.network,
        stats.station,
        stats.location,
        stats.channel,
        stats.mseed.dataquality,
        year,
        day,
        starttime.strftime("%H%M%S"),
        "sac",
    ]
    dest_file = str(dest_dir / ".".join(file_parts))
    # save
    st.write(dest_file, format="SAC")


if __name__ == "__main__":
    # mseed_dir_to_sac("data/perm_test", "data/perm_dest", "*HH*.D")
    mseed2sac_dir("data/perm_test", "data/perm_dest", pattern="*HH*.D")
