import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
import pandas as pd
from obspy import UTCDateTime
from seispy.workflow import BatchRun, new_run_id
from tqdm import tqdm

from seispy.correct.summary import CorrectionCounts, CorrectionIssue, CorrectionSummary

logger = logging.getLogger(__name__)


def correct_clock_drift(
    src_dir: str | Path,
    dest_dir: str | Path,
    drift_csv: str | Path,
    max_workers: int = 4,
    *,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CorrectionSummary:
    """Correct SAC trace times from station clock-drift metadata.

    Args:
        src_dir: Root directory of input SAC files grouped by station.
        dest_dir: Destination root preserving the input directory structure.
        drift_csv: CSV containing station, drift rate, and validity times.
        max_workers: Maximum number of station worker processes.
        max_error_samples: Maximum number of representative failures in the summary.
        save_report: Write a durable JSON run report. Enabled by default.
        save_log: Write a run log beside the report. Enabled by default.

    Returns:
        A common batch summary, including partial progress when failures occur.

    Examples:
        ```python
        correct_clock_drift(
            "data/sac", "data/drift-corrected", "clock-drift.csv",
            max_workers=1,
        )
        ```
    """

    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    src_path = Path(src_dir).expanduser().resolve()
    output_path = Path(dest_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {src_path}")
    drift_data = _load_drift_data(drift_csv)
    valid_stations = _get_valid_stations(src_path, drift_data.keys())
    station_totals = {
        station: sum(1 for _ in (src_path / station).rglob("*.sac"))
        for station in valid_stations
    }
    total = sum(station_totals.values())
    run_id = new_run_id()
    with BatchRun(
        "correct-clock-drift",
        output_path,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(total=total, succeeded=0, failed=0, skipped=0, issue_samples=())
        results = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _process_station_drift_correction,
                    station,
                    drift_data[station],
                    src_path,
                    output_path,
                    max_error_samples,
                ): station
                for station in valid_stations
            }
            with tqdm(total=len(futures), desc="Correcting clock drift") as bar:
                for future in as_completed(futures):
                    station = futures[future]
                    try:
                        results.append(future.result())
                    except Exception as exc:
                        run.error("station=%s error=%s", station, exc)
                        station_total = station_totals[station]
                        results.append(
                            CorrectionCounts(
                                total=station_total,
                                failed=station_total,
                                issue_samples=(
                                    CorrectionIssue(
                                        station,
                                        src_path / station,
                                        f"{type(exc).__name__}: {exc}",
                                    ),
                                ),
                            )
                        )
                    succeeded = sum(item.succeeded for item in results)
                    failed = sum(item.failed for item in results)
                    skipped = sum(item.skipped for item in results)
                    issues = tuple(
                        issue for item in results for issue in item.issue_samples
                    )[:max_error_samples]
                    run.checkpoint(
                        total=total,
                        completed=succeeded + failed + skipped,
                        succeeded=succeeded,
                        failed=failed,
                        skipped=skipped,
                        issue_samples=issues,
                    )
                    bar.update(1)
        succeeded = sum(item.succeeded for item in results)
        failed = sum(item.failed for item in results)
        skipped = sum(item.skipped for item in results)
        issues = tuple(issue for item in results for issue in item.issue_samples)[
            :max_error_samples
        ]
        return run.complete(
            CorrectionSummary(
                run_id=run_id,
                duration_seconds=0,
                total=total,
                succeeded=succeeded,
                failed=failed,
                skipped=skipped,
                issue_samples=issues,
                output_dir=output_path,
            )
        )


def _load_drift_data(drift_csv):
    df = pd.read_csv(
        drift_csv,
        parse_dates=["starttime", "endtime"],
        dtype={
            "station": str,
            "drift": float,
            "drift_rate": float,
        },
    )

    return {
        row["station"]: {
            "reference_time": UTCDateTime(row["starttime"]),
            "drift": float(row["drift"]),
            "drift_rate": float(row["drift_rate"]),
            "starttime": UTCDateTime(row["starttime"]),
            "endtime": UTCDateTime(row["endtime"]),
        }
        for _, row in df.iterrows()
    }


def _get_valid_stations(src_dir, drift_stations):
    """获取存在于数据目录中的台站"""
    src_path = Path(src_dir)
    return [sta for sta in drift_stations if (src_path / sta).exists()]


def _process_station_drift_correction(
    station_name, station_drift, src_dir, dest_dir, max_error_samples=20
):
    """处理单个台站的钟漂修正"""

    # 获取该台站的所有SAC文件
    station_path = Path(src_dir) / station_name

    processed_count = failed = skipped = total = 0
    issues = []
    # 处理每个SAC文件
    for sac_file in station_path.rglob("*.sac"):
        total += 1
        try:
            st = obspy.read(sac_file)

            # 检查文件时间是否在钟漂数据时间范围内
            sac_time = st[0].stats.starttime
            if not (station_drift["starttime"] <= sac_time <= station_drift["endtime"]):
                skipped += 1
                continue

            # 应用钟漂修正
            _apply_drift_correction(st, station_drift)
            # 保存修正后的文件
            _save_corrected_file(st, sac_file, src_dir, dest_dir)

            processed_count += 1

        except Exception as e:
            logger.error(f"Error processing {sac_file}: {str(e)}")
            failed += 1
            if len(issues) < max_error_samples:
                issues.append(
                    CorrectionIssue(station_name, sac_file, f"{type(e).__name__}: {e}")
                )

    logger.info(f"{station_name} complete, total {processed_count} files processed.")
    return CorrectionCounts(total, processed_count, failed, skipped, tuple(issues))


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
    drift_file = "/path/to/cor.csv"

    correct_clock_drift(
        src_dir=src_directory,
        dest_dir=dest_directory,
        drift_csv=drift_file,
        max_workers=4,
    )
