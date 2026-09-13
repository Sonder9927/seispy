"""Download waveform data from FDSN providers."""

import fnmatch
import logging
import random
import threading
import time
import warnings
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from obspy import UTCDateTime, read, read_inventory
from obspy.clients.fdsn.header import (
    FDSNBadGatewayException,
    FDSNInternalServerException,
    FDSNNoDataException,
    FDSNServiceUnavailableException,
    FDSNTimeoutException,
    FDSNTooManyRequestsException,
)
from obspy.core.inventory import Inventory
from obspy.io.mseed import InternalMSEEDWarning
from seispy.archive import (
    WaveformIdentity,
    channel_mseed_path,
    mseed_path,
    preserve_sac_quality,
    stream_day_identity,
)
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    cleanup_outputs,
    commit_output,
    new_run_id,
    temporary_output_path,
)
from tqdm import tqdm

from seispy.waveform.integrity import merge_short_gaps
from seispy.download.stations import EARTHSCOPE_URL, _client
from seispy.inventory import analyze_inventory

logger = logging.getLogger(__name__)
_THREAD_STATE = threading.local()
_RETRYABLE_ERRORS = (
    ConnectionError,
    TimeoutError,
    FDSNBadGatewayException,
    FDSNInternalServerException,
    FDSNServiceUnavailableException,
    FDSNTimeoutException,
    FDSNTooManyRequestsException,
    InternalMSEEDWarning,
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
    succeeded: int = 0
    skipped: int = 0
    no_data: int = 0
    failed: int = 0
    files_written: int = 0
    samples: tuple[WaveformDownloadError, ...] = ()


@dataclass(frozen=True, order=True)
class _InventoryTask:
    """One exact StationXML-backed waveform request."""

    network: str
    station: str
    location: str
    channel: str
    sample_rate: float
    starttime: UTCDateTime
    endtime: UTCDateTime


@dataclass(frozen=True)
class WaveformDownloadSummary(BatchSummary):
    """Summarize a waveform download run without retaining every task.

    This class is returned by :func:`download_waveforms`; applications normally
    do not instantiate it directly.

    Attributes:
        run_id: Unique identifier for the processing run.
        total: Number of requested station-day or XML-guided NSLC-day tasks.
        succeeded: Number of tasks succeeded successfully.
        skipped: Number skipped because output already existed.
        no_data: Number for which the FDSN service returned no data.
        failed: Number of unexpected failures.
        files_written: Number of waveform files committed to disk.
        issue_samples: Bounded sample of unexpected download errors.
        output_dir: Root directory containing the succeeded files.
        duration_seconds: Total elapsed wall-clock time.
        report_path: JSON report path when a report was generated.
        log_path: Persistent text log path when logging was enabled.
        status: Run state: running, completed, interrupted, or failed.

    Examples:
        ```python
        summary = download_waveforms(...)
        print(summary.succeeded, summary.failed)
        if summary.report_path:
            print(summary.report_path)
        ```
    """

    total: int
    succeeded: int
    skipped: int
    no_data: int
    failed: int
    files_written: int
    issue_samples: tuple[WaveformDownloadError, ...]
    output_dir: Path

    @property
    def has_issues(self) -> bool:
        return bool(self.failed or self.no_data)


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
    save_report: bool | None = True,
    save_log: bool = True,
    output_format: Literal["mseed", "sac"] = "mseed",
    inventory: str | Path | Inventory | None = None,
) -> WaveformDownloadSummary:
    """Download daily waveform files for one network.

    Args:
        output_dir: Root directory for the succeeded waveform tree.
        network: FDSN network code.
        starttime: Inclusive start time accepted by ``UTCDateTime``.
        endtime: Exclusive end time accepted by ``UTCDateTime``.
        station: Station wildcard, comma-separated codes, or a list of codes.
        location: FDSN location selector.
        channel: FDSN channel selector.
        client: ObsPy FDSN client name or service URL.
        username: Username for restricted data.
        password: Password for restricted data.
        max_workers: Maximum number of concurrent waveform requests.
        overwrite: Whether existing destination files may be replaced.
        max_error_samples: Maximum number of failures retained in the summary.
        max_retries: Number of retries after a transient waveform request failure.
        retry_backoff: Initial retry delay in seconds. Later delays increase
            exponentially and include a small random jitter.
        save_report: Write a continuously updated JSON report. Defaults to
            ``True``. ``None`` retains the report only when issues occur.
        save_log: Write a persistent, human-readable run log. Defaults to
            ``True``.
        output_format: Output format, either ``"mseed"`` or ``"sac"``.
        inventory: Optional StationXML path or ObsPy inventory used as an exact
            waveform manifest. Matching NSLC epochs are clipped to the request
            interval and split at UTC-day boundaries. Remote station discovery
            is not performed.

    Returns:
        Counts, sampled failures, output location, and run duration.

    Raises:
        ValueError: If arguments, credentials, or the time range are invalid.

    Examples:
        ```python
        summary = download_waveforms(
            "waveforms", "NZ", "2025-01-01", "2025-01-03",
            station=["WEL"], channel="BH?", max_workers=1,
        )
        summary.output_dir.name
        # => 'waveforms'
        ```
    """
    run_id = new_run_id()
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
        inventory_guided = False
    else:
        manifest = (
            inventory
            if isinstance(inventory, Inventory)
            else read_inventory(str(inventory), format="STATIONXML")
        )
        # ``read_inventory`` always returns Inventory in production.  The type
        # guard also keeps custom inventory-like test adapters usable.
        if isinstance(manifest, Inventory):
            inventory_analysis = analyze_inventory(
                manifest, starttime=start, endtime=end
            )
            inventory_analysis.download_suitability.require_safe("waveform download")
        task_items = _inventory_tasks(
            manifest, network, station, location, channel, start, end
        )
        if not task_items:
            raise ValueError(
                "inventory and selectors produced no waveform request in the "
                "requested time window"
            )
        stations = sorted({task.station for task in task_items})
        inventory_guided = True
    output = Path(output_dir).expanduser().resolve()
    output.mkdir(parents=True, exist_ok=True)
    aggregate = {
        "total": 0,
        "succeeded": 0,
        "skipped": 0,
        "no_data": 0,
        "failed": 0,
        "files_written": 0,
    }
    issue_samples = []
    with BatchRun(
        "waveform-download",
        output,
        run_id=run_id,
        save_report=save_report,
        save_log=save_log,
        logger=logger,
    ) as run:
        run.start(
            total=len(task_items),
            succeeded=0,
            skipped=0,
            no_data=0,
            failed=0,
            files_written=0,
            issue_samples=(),
            output_dir=output,
        )
        run.info(
            "run_id=%s request client=%s network=%s stations=%d days=%d "
            "channel=%s format=%s",
            run_id,
            client,
            network,
            len(stations),
            len(days),
            channel,
            output_format,
        )
        tasks = iter(task_items)
        with (
            warnings.catch_warnings(),
            ThreadPoolExecutor(max_workers=max_workers) as executor,
            tqdm(total=len(task_items), desc="Downloading waveforms") as bar,
        ):
            # Keep one filter active for the lifetime of the worker pool. Warning
            # filter contexts are process-wide on Python 3.12, so entering one in
            # every worker would race when concurrent requests finish.
            warnings.simplefilter("error", InternalMSEEDWarning)
            pending = set()
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < max_workers * 3:
                    try:
                        task = next(tasks)
                    except StopIteration:
                        exhausted = True
                        break
                    if inventory_guided:
                        future = executor.submit(
                            _download_inventory_task,
                            client,
                            username,
                            password,
                            output,
                            task,
                            overwrite,
                            min(max_error_samples, 1),
                            output_format,
                            max_retries,
                            retry_backoff,
                            run,
                        )
                    else:
                        code, day = task
                        future = executor.submit(
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
                            run,
                        )
                    pending.add(future)
                if pending:
                    done, pending = wait(pending, return_when=FIRST_COMPLETED)
                    for future in done:
                        result = future.result()
                        for name in aggregate:
                            aggregate[name] += getattr(result, name)
                        issue_samples.extend(
                            result.samples[
                                : max(0, max_error_samples - len(issue_samples))
                            ]
                        )
                        bar.update(1)
                        run.checkpoint(
                            completed=aggregate["total"],
                            total=len(task_items),
                            succeeded=aggregate["succeeded"],
                            skipped=aggregate["skipped"],
                            no_data=aggregate["no_data"],
                            failed=aggregate["failed"],
                            files_written=aggregate["files_written"],
                            issue_samples=tuple(issue_samples),
                            output_dir=output,
                        )
        summary = run.complete(
            WaveformDownloadSummary(
                run_id=run_id,
                total=aggregate["total"],
                succeeded=aggregate["succeeded"],
                skipped=aggregate["skipped"],
                no_data=aggregate["no_data"],
                failed=aggregate["failed"],
                files_written=aggregate["files_written"],
                issue_samples=tuple(issue_samples),
                output_dir=output,
                duration_seconds=0,
            )
        )
        for item in summary.issue_samples:
            run.error(
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


def _inventory_tasks(inventory, network, station, location, channel, start, end):
    """Return exact NSLC-day requests backed by StationXML channel epochs."""
    station_patterns = _patterns(station)
    location_patterns = _patterns(location, normalize_empty_location=True)
    channel_patterns = _patterns(channel)
    intervals = {}
    for net in inventory:
        if not fnmatch.fnmatchcase(net.code, network):
            continue
        for sta in net:
            if not _matches_any(sta.code, station_patterns):
                continue
            for item in sta:
                item_location = item.location_code or ""
                if not _matches_any(item_location, location_patterns):
                    continue
                if not _matches_any(item.code, channel_patterns):
                    continue
                epoch_start = max(start, item.start_date or start)
                epoch_end = min(end, item.end_date or end)
                if epoch_start >= epoch_end:
                    continue
                key = (
                    net.code,
                    sta.code,
                    item_location,
                    item.code,
                    float(item.sample_rate),
                )
                intervals.setdefault(key, []).append((epoch_start, epoch_end))

    tasks = []
    for key, epochs in sorted(intervals.items()):
        merged = []
        for epoch_start, epoch_end in sorted(epochs):
            if merged and epoch_start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(merged[-1][1], epoch_end))
            else:
                merged.append((epoch_start, epoch_end))
        for epoch_start, epoch_end in merged:
            day = UTCDateTime(epoch_start.date)
            while day < epoch_end:
                task_start = max(epoch_start, day)
                task_end = min(epoch_end, day + 86400)
                if task_start < task_end:
                    tasks.append(_InventoryTask(*key, task_start, task_end))
                day += 86400
    return tuple(sorted(tasks))


def _patterns(value, *, normalize_empty_location=False):
    values = value if isinstance(value, list) else value.split(",")
    patterns = [item.strip() for item in values if item.strip()]
    if normalize_empty_location:
        patterns = ["" if item == "--" else item for item in patterns]
    return patterns


def _matches_any(value, patterns):
    return any(fnmatch.fnmatchcase(value, pattern) for pattern in patterns)


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
    with warnings.catch_warnings():
        warnings.simplefilter("error", InternalMSEEDWarning)
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


def _download_inventory_task(
    base_url,
    username,
    password,
    output,
    task,
    overwrite,
    sample_limit,
    output_format,
    max_retries,
    retry_backoff,
    journal=None,
):
    if not overwrite and _inventory_task_is_complete(output, task, output_format):
        return _Counts(total=1, skipped=1)
    try:
        stream = _fetch_waveforms(
            base_url,
            username,
            password,
            task.network,
            task.station,
            task.location or "--",
            task.channel,
            task.starttime,
            task.endtime,
            max_retries,
            retry_backoff,
            journal,
        )
        _validate_inventory_stream(stream, task)
        if output_format == "mseed":
            destination = _inventory_mseed_path(output, task)
            if destination.exists() and not overwrite:
                raise FileExistsError(
                    f"existing MiniSEED is invalid for its XML task: {destination}; "
                    "use overwrite=True to replace it"
                )
            temporary = temporary_output_path(destination)
            try:
                stream.write(str(temporary), format="MSEED")
                commit_output(temporary, destination, overwrite=overwrite)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
            written, was_skipped = 1, False
        else:
            written, was_skipped = _write_waveforms(
                stream,
                output,
                task.network,
                task.station,
                UTCDateTime(task.starttime.date),
                output_format,
                overwrite,
                task.location or "--",
                task.channel,
            )
        return _Counts(
            total=1,
            succeeded=int(not was_skipped),
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
                    task.station,
                    task.starttime.strftime("%Y-%m-%d"),
                    f"{type(exc).__name__}: {exc}",
                ),
            )
        return _Counts(total=1, failed=1, samples=samples)


def _inventory_mseed_path(output, task):
    return channel_mseed_path(
        output,
        task.network,
        task.station,
        task.location,
        task.channel,
        task.starttime,
        task.endtime,
    )


def _validate_inventory_stream(stream, task):
    if not len(stream):
        raise ValueError("waveform stream is empty")
    expected = (task.network, task.station, task.location, task.channel)
    for trace in stream:
        identity = WaveformIdentity.from_trace(trace)
        actual = (
            identity.network,
            identity.station,
            identity.location,
            identity.channel,
        )
        if actual != expected:
            raise ValueError(
                f"succeeded trace identity {actual!r} does not match XML task "
                f"{expected!r}"
            )
        if abs(float(trace.stats.sampling_rate) - task.sample_rate) > 1e-6:
            raise ValueError(
                f"succeeded sample rate {trace.stats.sampling_rate} does not "
                f"match XML task {task.sample_rate}"
            )


def _inventory_task_is_complete(output, task, output_format):
    if output_format == "mseed":
        path = _inventory_mseed_path(output, task)
        if not path.is_file() or path.stat().st_size == 0:
            return False
        try:
            stream = read(path, headonly=True)
            _validate_inventory_stream(stream, task)
            return True
        except Exception as exc:
            logger.warning(
                "ignoring invalid XML-guided MiniSEED file %s: %s", path, exc
            )
        return False
    directory = output / task.network / task.station / str(task.starttime.year)
    location = task.location or "--"
    pattern = f"{task.network}.{task.station}.{location}.{task.channel}.*.sac"
    for path in directory.glob(pattern):
        try:
            stream = read(path, headonly=True)
            _validate_inventory_stream(stream, task)
            return True
        except Exception:
            continue
    return False


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
    journal=None,
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
            journal,
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
            succeeded=int(not was_skipped),
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
    journal=None,
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
        except _RETRYABLE_ERRORS as exc:
            if attempt == max_retries:
                if journal is not None:
                    journal.error(
                        "request failed after %d attempts nslc=%s.%s.%s.%s "
                        "start=%s end=%s error=%s: %s",
                        attempt + 1,
                        network,
                        station,
                        location,
                        channel,
                        start,
                        end,
                        type(exc).__name__,
                        exc,
                    )
                raise
            delay = retry_backoff * (2**attempt)
            if journal is not None:
                journal.warning(
                    "retrying request attempt=%d/%d delay=%.3fs "
                    "nslc=%s.%s.%s.%s start=%s end=%s error=%s: %s",
                    attempt + 2,
                    max_retries + 1,
                    delay,
                    network,
                    station,
                    location,
                    channel,
                    start,
                    end,
                    type(exc).__name__,
                    exc,
                )
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
    if output_format == "mseed":
        actual = stream_day_identity(stream)
        expected = (network, station, int(day.year), int(day.julday))
        if actual != expected:
            raise ValueError(
                f"succeeded stream identity {actual!r} does not match request "
                f"{expected!r}"
            )
        destination = mseed_path(output, stream)
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
    destinations = []
    for trace in stream:
        preserve_sac_quality(trace)
        identity = WaveformIdentity.from_trace(trace)
        expected = (network, station, int(day.year), int(day.julday))
        actual = (identity.network, identity.station, identity.year, identity.julday)
        if actual != expected:
            raise ValueError(
                f"succeeded trace identity {actual!r} does not match request "
                f"{expected!r}"
            )
        destinations.append(identity.sac_path(output))
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
    directory = output / network / station / str(day.year)
    expected = (network, station, int(day.year), int(day.julday))
    if output_format == "mseed":
        path = directory / f"{network}.{station}.{day.year}.{day.julday:03d}.mseed"
        if not path.is_file() or path.stat().st_size == 0:
            return False
        try:
            return stream_day_identity(read(path, headonly=True)) == expected
        except Exception as exc:
            logger.warning("ignoring invalid MiniSEED file %s: %s", path, exc)
            return False
    records = []
    for path in directory.glob("*.sac"):
        if path.stat().st_size == 0:
            continue
        try:
            traces = read(path, headonly=True)
            if len(traces) != 1:
                raise ValueError("SAC file must contain exactly one trace")
            identity = WaveformIdentity.from_trace(traces[0])
            actual = (
                identity.network,
                identity.station,
                identity.year,
                identity.julday,
            )
            if actual == expected and identity.matches_sac_path(path, output):
                records.append((identity.location, identity.channel))
        except Exception as exc:
            logger.warning("ignoring invalid SAC file %s: %s", path, exc)
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
    return WaveformIdentity.from_trace(trace).sac_path(directory)


def _combine(items, limit):
    samples = []
    for item in items:
        samples.extend(item.samples[: max(0, limit - len(samples))])
    return _Counts(
        sum(x.total for x in items),
        sum(x.succeeded for x in items),
        sum(x.skipped for x in items),
        sum(x.no_data for x in items),
        sum(x.failed for x in items),
        sum(x.files_written for x in items),
        tuple(samples),
    )
