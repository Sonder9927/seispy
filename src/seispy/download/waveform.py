import fnmatch
import logging
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from obspy import UTCDateTime, read_inventory
from obspy.clients.fdsn.header import (
    FDSNBadGatewayException,
    FDSNInternalServerException,
    FDSNNoDataException,
    FDSNServiceUnavailableException,
    FDSNTimeoutException,
    FDSNTooManyRequestsException,
)
from obspy.core.inventory import Inventory
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

from seispy._waveform import merge_short_gaps
from seispy.download.inventory import EARTHSCOPE_URL, _client

_LOG = {
    "name": "waveform_download",
    "file": "waveform-download.log",
    "level": logging.INFO,
}
_THREAD_STATE = threading.local()
_RETRYABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    FDSNBadGatewayException,
    FDSNInternalServerException,
    FDSNServiceUnavailableException,
    FDSNTimeoutException,
    FDSNTooManyRequestsException,
)


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
    max_retries: int = 2,
    retry_backoff: float = 1.0,
    save_report: bool | None = None,
    output_format: Literal["mseed", "sac"] = "mseed",
    inventory: str | Path | Inventory | None = None,
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
        max_workers: Maximum number of concurrent station-day requests.
        overwrite: Whether existing destination files may be replaced.
        max_error_samples: Maximum number of failures retained in the summary.
        max_retries: Number of retries after a transient waveform request failure.
        retry_backoff: Initial retry delay in seconds. Later delays increase
            exponentially and include a small random jitter.
        save_report: Force JSON report creation on or off. ``None`` writes a
            report only when issues occur.
        output_format: Output format, either ``"mseed"`` or ``"sac"``.
        inventory: Optional StationXML path or ObsPy inventory used as the
            waveform download manifest. It replaces the remote station lookup
            and excludes station-days without matching active metadata.

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
    if max_workers < 1 or max_error_samples < 0 or max_retries < 0 or retry_backoff < 0:
        raise ValueError(
            "max_workers must be positive; sample/retry counts and backoff "
            "must be non-negative"
        )
    output_format = output_format.lower()
    if output_format not in {"mseed", "sac"}:
        raise ValueError("output_format must be 'mseed' or 'sac'")
    _client(client, username, password)  # validate credentials and endpoint early
    start, end = UTCDateTime(starttime), UTCDateTime(endtime)
    if start >= end:
        raise ValueError("starttime must be earlier than endtime")
    days = tuple(_iter_days(start, end))
    if inventory is None:
        stations = _station_codes(
            client, username, password, network, station, start, end
        )
        task_items = tuple((code, day) for code in stations for day in days)
    else:
        manifest = (
            inventory
            if isinstance(inventory, Inventory)
            else read_inventory(str(inventory), format="STATIONXML")
        )
        task_items = _inventory_tasks(
            manifest, network, station, location, channel, days, end
        )
        stations = sorted({code for code, _ in task_items})
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    logger.info(
        "run_id=%s started client=%s network=%s stations=%d days=%d channel=%s format=%s",
        run_id,
        client,
        network,
        len(stations),
        len(days),
        channel,
        output_format,
    )
    tasks = iter(task_items)
    aggregate = {
        "total": 0,
        "downloaded": 0,
        "skipped": 0,
        "no_data": 0,
        "failed": 0,
        "files_written": 0,
    }
    error_samples = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(total=len(task_items), desc="Downloading waveforms") as bar:
            pending = set()
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < max_workers * 3:
                    try:
                        code, day = next(tasks)
                    except StopIteration:
                        exhausted = True
                        break
                    pending.add(
                        executor.submit(
                            _download_day,
                            client,
                            username,
                            password,
                            output,
                            network,
                            code,
                            location,
                            channel,
                            day,
                            min(day + 86400, end),
                            overwrite,
                            min(max_error_samples, 1),
                            output_format,
                            max_retries,
                            retry_backoff,
                        )
                    )
                if pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        result = future.result()
                        for name in aggregate:
                            aggregate[name] += getattr(result, name)
                        error_samples.extend(
                            result.samples[
                                : max(0, max_error_samples - len(error_samples))
                            ]
                        )
                        bar.update(1)
    combined = _Counts(**aggregate, samples=tuple(error_samples))
    summary = WaveformDownloadSummary(
        run_id,
        combined.total,
        combined.downloaded,
        combined.skipped,
        combined.no_data,
        combined.failed,
        combined.files_written,
        combined.samples,
        output,
        round(time.monotonic() - started, 3),
    )
    summary = auto_save_report(
        summary, "waveform-download", summary.failed + summary.no_data > 0, save_report
    )
    logger.info(
        "run_id=%s completed total=%d downloaded=%d skipped=%d no_data=%d "
        "failed=%d files_written=%d duration=%.3f report=%s",
        run_id,
        summary.total,
        summary.downloaded,
        summary.skipped,
        summary.no_data,
        summary.failed,
        summary.files_written,
        summary.duration_seconds,
        summary.report_path,
    )
    for item in summary.error_samples:
        logger.error(
            "run_id=%s station=%s day=%s error=%s",
            run_id,
            item.station,
            item.day,
            item.error,
        )
    return summary


def _station_codes(client, username, password, network, station, start, end):
    if isinstance(station, list):
        return list(dict.fromkeys(station))
    if "," in station and "*" not in station and "?" not in station:
        return list(
            dict.fromkeys(
                value.strip() for value in station.split(",") if value.strip()
            )
        )
    inventory = _client(client, username, password).get_stations(
        network=network, station=station, starttime=start, endtime=end, level="station"
    )
    return sorted({item.code for net in inventory for item in net.stations})


def _inventory_tasks(inventory, network, station, location, channel, days, end):
    """Return station-days backed by matching, active StationXML channels."""
    station_selectors = station if isinstance(station, list) else station.split(",")
    station_selectors = [value.strip() for value in station_selectors if value.strip()]
    tasks = []
    for day in days:
        request_end = min(day + 86400, end)
        selected = inventory.select(
            network=network,
            location=location,
            channel=channel,
            starttime=day,
            endtime=request_end,
        )
        codes = {
            item.code
            for net in selected
            for item in net.stations
            if any(
                fnmatch.fnmatchcase(item.code, pattern) for pattern in station_selectors
            )
        }
        tasks.extend((code, day) for code in sorted(codes))
    return tuple(tasks)


def _iter_days(start, end):
    current = UTCDateTime(start.date)
    while current < end:
        yield current
        current += 86400


def _thread_client(base_url, username, password):
    key = (base_url, username, password)
    if getattr(_THREAD_STATE, "key", None) != key:
        _THREAD_STATE.client = _client(base_url, username, password)
        _THREAD_STATE.key = key
    return _THREAD_STATE.client


def _download_station(
    base_url,
    username,
    password,
    output,
    network,
    station,
    location,
    channel,
    days,
    end,
    overwrite,
    sample_limit,
    output_format="mseed",
    max_retries=0,
    retry_backoff=0,
):
    # Compatibility wrapper used by callers of the former station-level worker.
    _THREAD_STATE.key = None
    return _combine(
        [
            _download_day(
                base_url,
                username,
                password,
                output,
                network,
                station,
                location,
                channel,
                day,
                min(day + 86400, end),
                overwrite,
                sample_limit,
                output_format,
                max_retries,
                retry_backoff,
            )
            for day in days
        ],
        sample_limit,
    )


def _download_day(
    base_url,
    username,
    password,
    output,
    network,
    station,
    location,
    channel,
    day,
    request_end,
    overwrite,
    sample_limit,
    output_format,
    max_retries,
    retry_backoff,
):
    if not overwrite and _day_is_complete(
        output, network, station, day, output_format, location, channel
    ):
        return _Counts(total=1, skipped=1)
    try:
        stream = _fetch_waveforms(
            base_url,
            username,
            password,
            network,
            station,
            location,
            channel,
            day,
            request_end,
            max_retries,
            retry_backoff,
        )
        written, was_skipped = _write_waveforms(
            stream,
            output,
            network,
            station,
            day,
            output_format,
            overwrite,
            location,
            channel,
        )
        return _Counts(
            total=1,
            downloaded=int(not was_skipped),
            skipped=int(was_skipped),
            files_written=written,
        )
    except FDSNNoDataException:
        return _Counts(total=1, no_data=1)
    except Exception as exc:
        samples = ()
        if sample_limit:
            samples = (
                WaveformDownloadError(
                    station, day.strftime("%Y-%m-%d"), f"{type(exc).__name__}: {exc}"
                ),
            )
        return _Counts(total=1, failed=1, samples=samples)


def _fetch_waveforms(
    base_url,
    username,
    password,
    network,
    station,
    location,
    channel,
    start,
    end,
    max_retries,
    retry_backoff,
):
    client = _thread_client(base_url, username, password)
    for attempt in range(max_retries + 1):
        try:
            stream = client.get_waveforms(
                network=network,
                station=station,
                location=location,
                channel=channel,
                starttime=start,
                endtime=end,
            )
            if not len(stream):
                raise FDSNNoDataException("empty stream")
            return stream
        except FDSNNoDataException:
            raise
        except _RETRYABLE_ERRORS:
            if attempt == max_retries:
                raise
            delay = retry_backoff * (2**attempt)
            time.sleep(delay + random.uniform(0, delay * 0.1))


def _write_waveforms(
    stream,
    output,
    network,
    station,
    day,
    output_format,
    overwrite,
    location="*",
    channel="*",
):
    directory = output / network / station / str(day.year) / f"{day.julday:03d}"
    if output_format == "mseed":
        destination = (
            directory / f"{network}.{station}.{day.year}.{day.julday:03d}.mseed"
        )
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

    merge_short_gaps(stream)
    destinations = [_sac_destination(trace, directory) for trace in stream]
    temporary_paths = []
    created = []
    try:
        for trace, destination in zip(stream, destinations, strict=True):
            if destination.exists() and not overwrite:
                continue
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
    return len(created), not created


def _day_is_complete(output, network, station, day, output_format, location, channel):
    directory = output / network / station / str(day.year) / f"{day.julday:03d}"
    if output_format == "mseed":
        path = directory / f"{network}.{station}.{day.year}.{day.julday:03d}.mseed"
        return path.is_file() and path.stat().st_size > 0
    records = []
    for path in directory.glob("*.sac"):
        parts = path.name.split(".")
        if len(parts) != 9 or path.stat().st_size == 0:
            continue
        file_network, file_station, file_location, file_channel = parts[:4]
        if file_network == network and file_station == station:
            records.append((file_location, file_channel))
    return _selectors_are_complete(records, location, channel)


def _selectors_are_complete(records, location, channel):
    if not records:
        return False
    locations = [value.strip() for value in location.split(",") if value.strip()]
    channels = [value.strip() for value in channel.split(",") if value.strip()]
    matching = [
        (file_location, file_channel)
        for file_location, file_channel in records
        if any(fnmatch.fnmatchcase(file_location, item) for item in locations)
        and any(fnmatch.fnmatchcase(file_channel, item) for item in channels)
    ]
    if not matching:
        return False
    explicit_channels = [
        item for item in channels if "*" not in item and "?" not in item
    ]
    return all(
        any(file_channel == item for _, file_channel in matching)
        for item in explicit_channels
    )


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
        samples.extend(item.samples[: max(0, limit - len(samples))])
    return _Counts(
        sum(x.total for x in items),
        sum(x.downloaded for x in items),
        sum(x.skipped for x in items),
        sum(x.no_data for x in items),
        sum(x.failed for x in items),
        sum(x.files_written for x in items),
        tuple(samples),
    )
