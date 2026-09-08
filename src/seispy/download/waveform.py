import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from obspy import UTCDateTime
from obspy.clients.fdsn.header import FDSNNoDataException
from rose import get_logger
from rose.batch import (
    ReportMixin,
    auto_save_report,
    cleanup_outputs,
    commit_output,
    create_run_id,
    temporary_output_path,
)
from tqdm import tqdm

from seispy.download.inventory import EARTHSCOPE_URL, _client

_LOG = {"name": "waveform_download", "file": "waveform-download.log", "level": logging.INFO}


@dataclass(frozen=True)
class WaveformDownloadError:
    """Describe one sampled waveform download failure.

    Attributes:
        station: Station code associated with the failure.
        day: Requested UTC day in ISO format.
        error: Exception type and message.
    """

    station: str
    day: str
    error: str


@dataclass(frozen=True)
class _Counts:
    total: int = 0
    downloaded: int = 0
    skipped: int = 0
    no_data: int = 0
    failed: int = 0
    files_written: int = 0
    samples: tuple[WaveformDownloadError, ...] = ()


@dataclass(frozen=True)
class WaveformDownloadSummary(ReportMixin):
    """Summarize a waveform download run without retaining every task.

    This class is returned by :func:`download_waveforms`; applications normally
    do not instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of requested station-day tasks.
        downloaded: Number of station-days downloaded successfully.
        skipped: Number skipped because output already existed.
        no_data: Number for which the FDSN service returned no data.
        failed: Number of unexpected failures.
        files_written: Number of waveform files committed to disk.
        error_samples: Bounded sample of unexpected download errors.
        output_dir: Root directory containing the downloaded files.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.

    Examples:
        >>> summary = download_waveforms(...)
        >>> print(summary.downloaded, summary.failed)
        >>> if summary.report_path:
        ...     print(summary.report_path)
    """

    run_id: str
    total: int
    downloaded: int
    skipped: int
    no_data: int
    failed: int
    files_written: int
    error_samples: tuple[WaveformDownloadError, ...]
    output_dir: Path
    duration_seconds: float
    report_path: Path | None = None


def download_waveforms(
    output_dir: str | Path,
    network: str,
    starttime: Any,
    endtime: Any,
    *,
    station: str | list[str] = "*",
    location: str = "*",
    channel: str = "*",
    client: str = EARTHSCOPE_URL,
    username: str | None = None,
    password: str | None = None,
    max_workers: int = 5,
    overwrite: bool = False,
    max_error_samples: int = 20,
    save_report: bool | None = None,
    output_format: Literal["mseed", "sac"] = "mseed",
) -> WaveformDownloadSummary:
    """Download daily waveform files for one network.

    Args:
        output_dir: Root directory for the downloaded waveform tree.
        network: FDSN network code.
        starttime: Inclusive start time accepted by ``UTCDateTime``.
        endtime: Exclusive end time accepted by ``UTCDateTime``.
        station: Station wildcard, comma-separated codes, or a list of codes.
        location: FDSN location selector.
        channel: FDSN channel selector.
        client: ObsPy FDSN client name or service URL.
        username: Username for restricted data.
        password: Password for restricted data.
        max_workers: Maximum number of concurrent station downloads.
        overwrite: Whether existing destination files may be replaced.
        max_error_samples: Maximum number of failures retained in the summary.
        save_report: Force JSON report creation on or off. ``None`` writes a
            report only when issues occur.
        output_format: Output format, either ``"mseed"`` or ``"sac"``.

    Returns:
        Counts, sampled failures, output location, and run duration.

    Raises:
        ValueError: If arguments, credentials, or the time range are invalid.

    Examples:
        >>> summary = download_waveforms(
        ...     "waveforms", "NZ", "2025-01-01", "2025-01-03",
        ...     station=["WEL"], channel="BH?", max_workers=1,
        ... )
        >>> summary.output_dir.name
        'waveforms'
    """
    started = time.monotonic()
    run_id = create_run_id()
    logger = get_logger(**_LOG)
    if max_workers < 1 or max_error_samples < 0:
        raise ValueError("max_workers must be positive and max_error_samples non-negative")
    output_format = output_format.lower()
    if output_format not in {"mseed", "sac"}:
        raise ValueError("output_format must be 'mseed' or 'sac'")
    _client(client, username, password)  # validate credentials and endpoint early
    start, end = UTCDateTime(starttime), UTCDateTime(endtime)
    if start >= end:
        raise ValueError("starttime must be earlier than endtime")
    stations = _station_codes(client, username, password, network, station, start, end)
    days = []
    current = UTCDateTime(start.date)
    while current < end:
        days.append(current)
        current += 86400
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    logger.info(
        "run_id=%s started client=%s network=%s stations=%d days=%d channel=%s format=%s",
        run_id, client, network, len(stations), len(days), channel, output_format,
    )
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _download_station, client, username, password, output, network,
                code, location, channel, days, end, overwrite,
                min(max_error_samples, 1), output_format,
            ): code
            for code in stations
        }
        counts = []
        with tqdm(total=len(stations) * len(days), desc="Downloading waveforms") as bar:
            for future in as_completed(futures):
                result = future.result()
                counts.append(result)
                bar.update(result.total)
    combined = _combine(counts, max_error_samples)
    summary = WaveformDownloadSummary(
        run_id, combined.total, combined.downloaded, combined.skipped,
        combined.no_data, combined.failed, combined.files_written,
        combined.samples, output,
        round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(summary, "waveform-download",
                               summary.failed + summary.no_data > 0, save_report)
    logger.info(
        "run_id=%s completed total=%d downloaded=%d skipped=%d no_data=%d "
        "failed=%d files_written=%d duration=%.3f report=%s",
        run_id, summary.total, summary.downloaded, summary.skipped,
        summary.no_data, summary.failed, summary.files_written,
        summary.duration_seconds, summary.report_path,
    )
    for item in summary.error_samples:
        logger.error("run_id=%s station=%s day=%s error=%s",
                     run_id, item.station, item.day, item.error)
    return summary


def _station_codes(client, username, password, network, station, start, end):
    if isinstance(station, list):
        return station
    if "," in station and "*" not in station and "?" not in station:
        return [value.strip() for value in station.split(",") if value.strip()]
    inventory = _client(client, username, password).get_stations(
        network=network, station=station, starttime=start, endtime=end, level="station"
    )
    return sorted({item.code for net in inventory for item in net.stations})


def _download_station(base_url, username, password, output, network, station,
                      location, channel, days, end, overwrite, sample_limit,
                      output_format="mseed"):
    client = _client(base_url, username, password)
    downloaded = skipped = no_data = failed = 0
    samples = []
    files_written = 0
    for day in days:
        try:
            stream = client.get_waveforms(
                network=network, station=station, location=location, channel=channel,
                starttime=day, endtime=min(day + 86400, end),
            )
            if not len(stream):
                raise FDSNNoDataException("empty stream")
            written, was_skipped = _write_waveforms(
                stream, output, network, station, day, output_format, overwrite
            )
            if was_skipped:
                skipped += 1
                continue
            files_written += written
            downloaded += 1
        except FDSNNoDataException:
            no_data += 1
        except Exception as exc:
            failed += 1
            if len(samples) < sample_limit:
                samples.append(WaveformDownloadError(
                    station, day.strftime("%Y-%m-%d"), f"{type(exc).__name__}: {exc}"
                ))
    return _Counts(
        len(days), downloaded, skipped, no_data, failed, files_written, tuple(samples)
    )


def _write_waveforms(stream, output, network, station, day, output_format, overwrite):
    directory = output / network / station / str(day.year) / f"{day.julday:03d}"
    if output_format == "mseed":
        destination = directory / f"{network}.{station}.{day.year}.{day.julday:03d}.mseed"
        if destination.exists() and not overwrite:
            return 0, True
        temporary = temporary_output_path(destination)
        try:
            stream.write(str(temporary), format="MSEED")
            commit_output(temporary, destination, overwrite=overwrite)
        except Exception:
            temporary.unlink(missing_ok=True)
            raise
        return 1, False

    stream.merge(method=1, fill_value="interpolate")
    destinations = [_sac_destination(trace, directory) for trace in stream]
    if not overwrite and any(path.exists() for path in destinations):
        return 0, True
    temporary_paths = []
    created = []
    try:
        for trace, destination in zip(stream, destinations, strict=True):
            temporary = temporary_output_path(destination)
            temporary_paths.append(temporary)
            trace.write(str(temporary), format="SAC")
            commit_output(temporary, destination, overwrite=overwrite)
            temporary_paths.remove(temporary)
            created.append(destination)
    except Exception:
        cleanup_outputs(temporary_paths)
        if not overwrite:
            cleanup_outputs(created)
        raise
    return len(created), False


def _sac_destination(trace, directory):
    stats = trace.stats
    try:
        quality = stats.mseed.dataquality
    except (AttributeError, KeyError):
        quality = "D"
    start = stats.starttime
    filename = (
        f"{stats.network}.{stats.station}.{stats.location}.{stats.channel}.{quality}."
        f"{start.year}.{start.julday:03d}.{start.strftime('%H%M%S')}.sac"
    )
    return directory / filename


def _combine(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[:max(0, limit - len(samples))])
    return _Counts(
        sum(x.total for x in items), sum(x.downloaded for x in items),
        sum(x.skipped for x in items), sum(x.no_data for x in items),
        sum(x.failed for x in items), sum(x.files_written for x in items),
        tuple(samples),
    )
