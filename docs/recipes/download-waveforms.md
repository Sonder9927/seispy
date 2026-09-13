---
title: Download waveforms
description: Download daily MiniSEED or SAC waveform files from FDSN.
---

# Download waveforms

**Use this when:** you want a daily waveform archive grouped by network,
station, and year, with Julian day retained in each filename.

## Example

```python
from seispy import download

summary = download.download_waveforms(
    "data/waveforms",
    network="NZ",
    starttime="2025-01-01",
    endtime="2025-01-03",
    station=["WEL", "KHZ"],
    channel="BH?",
    output_format="mseed",
    max_workers=2,
    max_retries=2,
    retry_backoff=1.0,
    overwrite=False,
    inventory="data/metadata/stations.xml",
)

print(f"Downloaded: {summary.succeeded}/{summary.total}")
print(f"No data: {summary.no_data}; failed: {summary.failed}")
print(f"Report: {summary.report_path}")
print(f"Log: {summary.log_path}")
```

## Download restricted data

Pass the same FDSN web-service credentials when downloading response metadata
and waveforms. The following EarthScope example downloads restricted MiniSEED,
checks daily availability, and removes the instrument response with ObsPy:

```python
from seispy import download, response


NET = "1U"
OUTDIR = "data"

inventory = download.download_inventory(
    "data/1U_inventory.xml",
    client="https://service.earthscope.org",
    username="username",
    password="password",
    network=NET,
    starttime="2023-08-01",
    endtime="2025-05-01",
    level="response",
)

download_summary = download.download_waveforms(
    OUTDIR,
    client="https://service.earthscope.org",
    network=NET,
    starttime="2023-08-01",
    endtime="2025-05-01",
    output_format="mseed",
    max_workers=10,
    username="username",
    password="password",
    inventory=inventory,
)

availability = download.download_status(
    f"{OUTDIR}/{NET}",
    start_date="2023-08-01",
    end_date="2025-04-30",
)

response_summary = response.remove_instrument_response(
    f"{OUTDIR}/{NET}",
    resp=inventory,
    backend="obspy",
    pattern="*.mseed",
    max_workers=18,
    decimate_factors=4,
)

print(
    f"Downloaded: {download_summary.succeeded}/{download_summary.total}; "
    f"failed: {download_summary.failed}"
)
print(
    f"Response removed: {response_summary.succeeded}/{response_summary.total}; "
    f"failed: {response_summary.failed}"
)
```

The placeholder values `username="username"` and `password="password"` must
be replaced with credentials issued by the provider. For EarthScope, sign in
to the [EarthScope user profile](https://www.earthscope.org/user), open the
**Credentials** tab, and click **REVEAL MY CREDENTIALS**. If credentials have
not yet been issued, click **CREATE FDSNWS CREDENTIALS** first. Access to the
requested restricted network must already have been granted.

!!! warning "Keep credentials private"

    Never commit real credentials to source control or include them in shared
    notebooks, documentation, screenshots, or logs. For reusable scripts,
    load them from environment variables or a secret manager instead of
    writing them directly in Python.

`download_waveforms` treats `endtime` as exclusive, whereas `download_status`
treats `end_date` as inclusive. Consequently, the example downloads through
2025-04-30 and uses that date as the final availability day.

## Result

Files are written below `data/waveforms/<network>/<station>/<year>/`.
The returned summary distinguishes succeeded, existing, no-data, and failed
requests.

## Reports, logs, and interrupted runs

Reports and persistent logs are enabled by default. They are written below the
waveform output directory:

```text
waveforms/logs/waveform-download-<run_id>.log
waveforms/logs/reports/waveform-download-<run_id>.json
```

The JSON report is created with `status: "running"` before waveform workers
start and tracks completed tasks with periodic atomic flushes. A normal run finishes with
`status: "completed"`; a caught keyboard or system interruption records
`status: "interrupted"`. If the process is forcibly terminated and cannot run
cleanup code, the last atomic report remains marked `running`, showing how far
the run progressed. The text log provides a human-readable start, progress,
error, and completion history using the same `run_id`.

Set `save_report=False` or `save_log=False` to disable either artifact. Passing
`save_report=None` preserves the former issue-only behavior: a checkpoint is
maintained while the command runs but removed after a clean completion.

When `inventory` is a StationXML path or an ObsPy `Inventory`, it is an exact
download manifest. Matching channel epochs are read locally and converted into
NSLC requests, clipped to both the metadata epoch and the requested time range,
then split at UTC-day boundaries. The station service is not queried again.
The `network`, `station`, `location`, and `channel` selectors are applied to
the manifest before tasks are created.

XML-guided MiniSEED uses one collision-free file per exact channel request:

```text
10.HHZ.001.mseed
```

The filename records network, station, location, channel, start day/time, and
end time. A channel epoch beginning or ending during a UTC day produces a
partial-day filename with its exact boundary. Downloaded trace identity and
sample rate are validated against StationXML before the file is committed.

Without `inventory`, the original station-day behavior and daily MiniSEED name
are retained for compatibility. Existing outputs are checked before a network
request is made, so rerunning the same command resumes an interrupted archive.

## Experimental bulk downloader

See the separate [MassDownloader guide](mass-download-waveforms.md) for
provider discovery, bulk requests, StationXML filtering, concurrency settings,
and a comparison with this stable downloader.

!!! tip "Learn with a short interval"

    Start with one station and one or two days. Increase the interval and
    `max_workers` only after confirming the service and selectors.

!!! note "Be considerate of public FDSN services"

    More workers are not always faster. Start with 2–5 workers and follow the
    data provider's usage policy. Temporary request failures are retried with
    exponential backoff; no-data responses are not retried.

[See all parameters →](../api/download.md#download-waveforms)
