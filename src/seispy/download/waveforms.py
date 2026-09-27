"""Download unmodified waveform responses from FDSN providers."""

import fnmatch
import logging
import random
import threading
import time
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
from obspy.io.mseed.util import get_record_information
from seispy.progress import progress_bar

from seispy.archive import channel_mseed_path
from seispy.download.stations import EARTHSCOPE_URL, _client
from seispy.inventory import analyze_inventory
from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    new_run_id,
    temporary_output_path,
)

logger = logging.getLogger(__name__)
_THREAD_STATE = threading.local()


class InvalidWaveformResponse(ValueError):
    """A staged response that is not complete miniSEED framing (retryable)."""


class DiscontinuousWaveformResponse(ValueError):
    """A response that covers too little of its span to be trusted (final)."""


_RETRYABLE_ERRORS = (
    InvalidWaveformResponse,
    ConnectionError,
    TimeoutError,
    FDSNBadGatewayException,
    FDSNInternalServerException,
    FDSNServiceUnavailableException,
    FDSNTimeoutException,
    FDSNTooManyRequestsException,
)

DEFAULT_MAX_GAP_RATIO = 0.5
DEFAULT_MAX_SEGMENTS = 1000


@dataclass(frozen=True)
class WaveformDownloadError:
    """Describe one sampled raw waveform download failure."""

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
    network: str
    station: str
    location: str
    channel: str
    sample_rate: float
    starttime: UTCDateTime
    endtime: UTCDateTime


@dataclass(frozen=True)
class WaveformDownloadSummary(BatchSummary):
    """Summarize downloads of unverified ``.mseed.raw`` responses."""

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
    network_workers: int = 10,
    overwrite: bool = False,
    max_error_samples: int = 20,
    max_retries: int = 2,
    retry_backoff: float = 1.0,
    max_gap_ratio: float | None = DEFAULT_MAX_GAP_RATIO,
    max_segments: int | None = DEFAULT_MAX_SEGMENTS,
    save_report: bool | None = True,
    save_log: bool = True,
    inventory: str | Path | Inventory | None = None,
) -> WaveformDownloadSummary:
    """Download FDSN responses byte-for-byte without decoding sample values.

    The output is a staging area, and every body is checked for complete
    miniSEED framing. Files use the ``.mseed.raw``
    suffix and must be passed to :func:`seispy.waveform.archive_waveforms`
    before normal processing.

    Args:
        output_dir: Root directory for raw waveform responses.
        network: FDSN network code.
        starttime: Inclusive request start time.
        endtime: Exclusive request end time.
        station: Station wildcard, comma-separated codes, or explicit list.
        location: FDSN location selector.
        channel: FDSN channel selector.
        client: ObsPy FDSN client name or service URL.
        username: Username for restricted data.
        password: Password for restricted data.
        network_workers: Maximum concurrent network transfers. Defaults to 10.
        overwrite: Replace existing non-empty raw responses.
        max_error_samples: Maximum sampled failures retained in the summary.
        max_retries: Retries after a transient request failure.
        retry_backoff: Initial exponential retry delay in seconds.
        max_gap_ratio: Reject a response whose positive gaps exceed this
            fraction of its span. Defaults to 0.5; pass None to accept
            discontinuous data.
        max_segments: Reject a response that splits into more traces than this.
            Defaults to 1000; pass None to accept any segmentation.
        save_report: Persist the continuously updated JSON run report.
        save_log: Persist the human-readable run log.
        inventory: Optional StationXML path or Inventory used as an exact NSLC
            request manifest. It is not used to validate downloaded bytes.

    Returns:
        Download counts and paths. ``succeeded`` means transport succeeded; it
        now also passed framing and continuity checks.
    """
    if (
        network_workers < 1
        or max_error_samples < 0
        or max_retries < 0
        or retry_backoff < 0
    ):
        raise ValueError(
            "network_workers must be positive; sample/retry counts and backoff "
            "must be non-negative"
        )
    if max_gap_ratio is not None and not 0.0 <= max_gap_ratio <= 1.0:
        raise ValueError("max_gap_ratio must be within [0, 1] or None")
    if max_segments is not None and max_segments < 0:
        raise ValueError("max_segments must be non-negative or None")
    _client(client, username, password)
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
        if isinstance(manifest, Inventory):
            analysis = analyze_inventory(manifest, starttime=start, endtime=end)
            analysis.download_suitability.require_safe("waveform download")
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
    run_id = new_run_id()
    aggregate = dict.fromkeys(
        ("total", "succeeded", "skipped", "no_data", "failed", "files_written"),
        0,
    )
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
            "channel=%s network_workers=%d raw_only=true",
            run_id,
            client,
            network,
            len(stations),
            len(days),
            channel,
            network_workers,
        )
        tasks = iter(task_items)
        with (
            ThreadPoolExecutor(max_workers=network_workers) as executor,
            progress_bar(
                total=len(task_items), desc="Downloading", unit="request"
            ) as bar,
        ):
            pending = set()
            exhausted = False
            while pending or not exhausted:
                while not exhausted and len(pending) < network_workers * 3:
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
                            max_retries,
                            retry_backoff,
                            run,
                            max_gap_ratio=max_gap_ratio,
                            max_segments=max_segments,
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
                            max_retries,
                            retry_backoff,
                            run,
                            max_gap_ratio=max_gap_ratio,
                            max_segments=max_segments,
                        )
                    pending.add(future)
                if not pending:
                    continue
                done, pending = wait(pending, return_when=FIRST_COMPLETED)
                for future in done:
                    result = future.result()
                    for name in aggregate:
                        aggregate[name] += getattr(result, name)
                    issue_samples.extend(
                        result.samples[: max(0, max_error_samples - len(issue_samples))]
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


def _download_inventory_task(
    base_url,
    username,
    password,
    output,
    task,
    overwrite,
    sample_limit,
    max_retries,
    retry_backoff,
    journal=None,
    *,
    max_gap_ratio=DEFAULT_MAX_GAP_RATIO,
    max_segments=DEFAULT_MAX_SEGMENTS,
):
    destination = _inventory_raw_path(output, task)
    return _download_raw_response(
        base_url,
        username,
        password,
        destination,
        task.network,
        task.station,
        task.location or "--",
        task.channel,
        task.starttime,
        task.endtime,
        overwrite,
        sample_limit,
        max_retries,
        retry_backoff,
        journal,
        max_gap_ratio=max_gap_ratio,
        max_segments=max_segments,
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
    max_retries,
    retry_backoff,
    journal=None,
    *,
    max_gap_ratio=DEFAULT_MAX_GAP_RATIO,
    max_segments=DEFAULT_MAX_SEGMENTS,
):
    destination = _station_day_raw_path(output, network, station, day)
    return _download_raw_response(
        base_url,
        username,
        password,
        destination,
        network,
        station,
        location,
        channel,
        day,
        request_end,
        overwrite,
        sample_limit,
        max_retries,
        retry_backoff,
        journal,
        max_gap_ratio=max_gap_ratio,
        max_segments=max_segments,
    )


def _download_raw_response(
    base_url,
    username,
    password,
    destination,
    network,
    station,
    location,
    channel,
    start,
    end,
    overwrite,
    sample_limit,
    max_retries,
    retry_backoff,
    journal,
    *,
    max_gap_ratio=DEFAULT_MAX_GAP_RATIO,
    max_segments=DEFAULT_MAX_SEGMENTS,
):
    if destination.is_file() and destination.stat().st_size > 0 and not overwrite:
        return _Counts(total=1, skipped=1)
    temporary = temporary_output_path(destination)
    try:
        _fetch_waveform_file(
            base_url,
            username,
            password,
            network,
            station,
            location,
            channel,
            start,
            end,
            temporary,
            max_retries,
            retry_backoff,
            journal,
            max_gap_ratio=max_gap_ratio,
            max_segments=max_segments,
        )
        commit_output(
            temporary,
            destination,
            overwrite=overwrite or destination.exists(),
        )
        return _Counts(total=1, succeeded=1, files_written=1)
    except FDSNNoDataException:
        return _Counts(total=1, no_data=1)
    except Exception as exc:
        samples = ()
        if sample_limit:
            samples = (
                WaveformDownloadError(
                    station,
                    start.strftime("%Y-%m-%d"),
                    f"{type(exc).__name__}: {exc}",
                ),
            )
        return _Counts(total=1, failed=1, samples=samples)
    finally:
        temporary.unlink(missing_ok=True)


def _inventory_raw_path(output, task):
    archive = channel_mseed_path(
        output,
        task.network,
        task.station,
        task.location,
        task.channel,
        task.starttime,
        task.endtime,
    )
    return archive.with_suffix(f"{archive.suffix}.raw")


def _station_day_raw_path(output, network, station, day):
    directory = output / network / station / str(day.year)
    return directory / f"{network}.{station}.{day.year}.{day.julday:03d}.mseed.raw"


def _validate_mseed_response(
    path: Path,
    *,
    max_gap_ratio: float | None = DEFAULT_MAX_GAP_RATIO,
    max_segments: int | None = DEFAULT_MAX_SEGMENTS,
) -> None:
    """Reject a staged response that is not complete, continuous miniSEED.

    A provider can answer with a valid first record followed by an HTTP error
    page, with a plain text/HTML body, or with a response that only covers a
    fraction of the requested window. Framing and coverage are inspected here
    without decoding sample values; the archive stays responsible for sample
    validation and recovery.
    """
    size = path.stat().st_size
    try:
        length = int(get_record_information(str(path))["record_length"])
    except Exception as exc:
        raise InvalidWaveformResponse(f"response is not miniSEED: {exc}") from exc
    if length <= 0 or size % length != 0:
        raise InvalidWaveformResponse(
            f"response is not complete miniSEED framing ({size} bytes)"
        )
    if max_gap_ratio is None and max_segments is None:
        return
    try:
        stream = read(str(path), headonly=True)
    except Exception as exc:
        raise InvalidWaveformResponse(
            f"response is not readable miniSEED: {exc}"
        ) from exc
    if not stream:
        raise InvalidWaveformResponse("response contains no miniSEED traces")
    if max_segments is not None and len(stream) > max_segments:
        raise DiscontinuousWaveformResponse(
            f"response fragments into {len(stream)} segments "
            f"(limit {max_segments}); likely a partial or discontinuous response"
        )
    if max_gap_ratio is None:
        return
    sampling_rate = float(stream[0].stats.sampling_rate)
    if sampling_rate <= 0:
        return
    data_seconds = sum(trace.stats.npts for trace in stream) / sampling_rate
    span = float(stream[-1].stats.endtime - stream[0].stats.starttime)
    if span <= 0:
        return
    gap_ratio = max(0.0, (span - data_seconds) / span)
    if gap_ratio > max_gap_ratio:
        raise DiscontinuousWaveformResponse(
            f"response has a {gap_ratio:.0%} gap ratio "
            f"(limit {max_gap_ratio:.0%}); likely a partial or discontinuous response"
        )


def _fetch_waveform_file(
    base_url,
    username,
    password,
    network,
    station,
    location,
    channel,
    start,
    end,
    destination,
    max_retries,
    retry_backoff,
    journal=None,
    *,
    max_gap_ratio=DEFAULT_MAX_GAP_RATIO,
    max_segments=DEFAULT_MAX_SEGMENTS,
):
    """Download response bytes without asking ObsPy to decode MiniSEED."""
    client = _thread_client(base_url, username, password)
    destination = Path(destination)
    for attempt in range(max_retries + 1):
        destination.unlink(missing_ok=True)
        try:
            client.get_waveforms(
                network=network,
                station=station,
                location=location,
                channel=channel,
                starttime=start,
                endtime=end,
                filename=str(destination),
            )
            if not destination.is_file() or destination.stat().st_size == 0:
                raise FDSNNoDataException("empty raw waveform response")
            _validate_mseed_response(
                destination,
                max_gap_ratio=max_gap_ratio,
                max_segments=max_segments,
            )
            return
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
