import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import obspy
import pandas as pd
from obspy.signal.rotate import rotate2zne
from seispy.workflow import BatchRun, new_run_id
from seispy.progress import call_with_warnings, progress_bar, resolve_worker_call

from seispy.correct.summary import CorrectionCounts, CorrectionIssue, CorrectionSummary

logger = logging.getLogger(__name__)


def correct_orientation(
    net_dir: str | Path,
    dest_dir: str | Path,
    cor_csv: str | Path,
    max_workers: int = 4,
    *,
    max_error_samples: int = 20,
    save_report: bool | None = True,
    save_log: bool = True,
) -> CorrectionSummary:
    """Rotate three-component SAC data to correct sensor orientation.

    Args:
        net_dir: One network directory of input SAC files grouped by station.
        dest_dir: Destination root preserving the input directory structure.
        cor_csv: CSV containing station orientation and tilt values in degrees.
        max_workers: Maximum number of station worker processes.
        max_error_samples: Maximum number of representative failures in the summary.
        save_report: Write a durable JSON run report. Enabled by default.
        save_log: Write a run log beside the report. Enabled by default.

    Returns:
        A common batch summary, including partial progress when failures occur.

    Examples:
        ```python
        correct_orientation(
            "data/sac/NZ", "data/orientation-corrected/NZ", "orientation.csv",
            max_workers=1,
        )
        ```
    """

    if max_workers < 1:
        raise ValueError("max_workers must be at least 1")
    if max_error_samples < 0:
        raise ValueError("max_error_samples cannot be negative")
    src_path = Path(net_dir).expanduser().resolve()
    output_path = Path(dest_dir).expanduser().resolve()
    if not src_path.is_dir():
        raise NotADirectoryError(f"Source directory does not exist: {src_path}")
    cor_data = _load_cor_data(cor_csv)
    valid_stations = _get_valid_stations(src_path, cor_data.keys())
    pattern = "*.BHZ.*sac"
    station_totals = {
        station: sum(1 for _ in (src_path / station).rglob(pattern))
        for station in valid_stations
    }
    total = sum(station_totals.values())
    run_id = new_run_id()
    with BatchRun(
        "correct-orientation",
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
                    call_with_warnings,
                    _process_station_orientation,
                    station,
                    cor_data[station],
                    src_path,
                    output_path,
                    max_error_samples,
                ): station
                for station in valid_stations
            }
            with progress_bar(
                total=len(futures), desc="Correcting orientation", unit="station"
            ) as bar:
                for future in as_completed(futures):
                    station = futures[future]
                    try:
                        results.append(resolve_worker_call(future.result()))
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
                    issues = tuple(
                        issue for item in results for issue in item.issue_samples
                    )[:max_error_samples]
                    run.checkpoint(
                        total=total,
                        completed=succeeded + failed,
                        succeeded=succeeded,
                        failed=failed,
                        skipped=0,
                        issue_samples=issues,
                    )
                    bar.update(1)
        succeeded = sum(item.succeeded for item in results)
        failed = sum(item.failed for item in results)
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
                issue_samples=issues,
                output_dir=output_path,
            )
        )


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


def _process_station_orientation(
    station_name, station_cor, src_dir, dest_dir, max_error_samples=20
):
    """处理单个台站的钟漂修正"""

    # 获取该台站的所有SAC文件
    station_path = Path(src_dir) / station_name

    processed_count = failed = total = 0
    issues = []
    pattern = "*.BHZ.*sac"
    # 处理每个SAC文件
    for zsac in station_path.rglob(pattern):
        total += 1
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
            failed += 1
            if len(issues) < max_error_samples:
                issues.append(
                    CorrectionIssue(station_name, nsac, f"{type(e).__name__}: {e}")
                )

    if processed_count == 0:
        logger.warning(
            f"No files processed for {station_name} by searching pattern `{pattern}`."
        )
    else:
        logger.info(
            f"{station_name} complete, total {processed_count}X2 files processed."
        )
    return CorrectionCounts(total, processed_count, failed, 0, tuple(issues))


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

    correct_orientation(
        net_dir=src_directory, dest_dir=dest_directory, cor_csv=cor_csv, max_workers=4
    )
