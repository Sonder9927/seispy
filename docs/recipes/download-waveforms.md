---
title: Download and archive waveforms
description: Stage raw FDSN responses, then validate them into MiniSEED or SAC archives.
---

# Download and archive waveforms

Waveform acquisition has two explicit stages:

1. `download_waveforms` transfers FDSN response bytes into an untrusted staging
   directory. It never invokes ObsPy's MiniSEED decoder.
2. `archive_waveforms` validates those responses in isolated processes and
   commits trusted MiniSEED or SAC files.

This boundary keeps network concurrency independent from CPU-heavy native
decoding and prevents concurrent libmseed calls in download threads.

## Download raw responses

```python
from seispy import download

download_summary = download.download_waveforms(
    "data/waveform-staging",
    network="NZ",
    starttime="2025-01-01",
    endtime="2025-01-03",  # exclusive
    station=["WEL", "KHZ"],
    channel="BH?",
    inventory="data/metadata/stations.xml",
    network_workers=10,
    max_retries=2,
    retry_backoff=1.0,
)
```

Successful responses are stored byte-for-byte with a `.mseed.raw` suffix.
This suffix deliberately marks them as unverified and keeps ordinary MiniSEED
file scans from treating them as trusted data. With StationXML, filenames
include network, station, location, channel, UTC day, and a partial-day start
time when needed:

```text
data/waveform-staging/NZ/WEL/2025/NZ.WEL.10.HHZ.2025.001.mseed.raw
data/waveform-staging/NZ/WEL/2025/NZ.WEL.10.HHZ.2025.001.060000.mseed.raw
```

StationXML is an exact request manifest: requests are clipped to channel epochs
and UTC-day boundaries. It is not used to validate bytes during download.

## Archive as MiniSEED

```python
from seispy import waveform

archive_summary = waveform.archive_waveforms(
    "data/waveform-staging",
    "data/mseed",
    output_format="mseed",
    inventory="data/metadata/stations.xml",
    max_workers=5,
    remove_original=False,
)
```

Valid MiniSEED is copied without re-encoding. Headers, archive identity, and—if
StationXML is supplied—channel epoch and sample rate are checked first.
`max_workers` controls isolated archive processes and defaults to 5. A large
server may raise it independently of the downloader's `network_workers`; for
example, keep network transfers at 10 and use 40 archive workers if memory and
storage throughput permit.

## Archive as SAC

SAC uses the same staging input and integrity checks. Each resulting trace is
written to its canonical SAC path:

```python
sac_summary = waveform.archive_waveforms(
    "data/waveform-staging",
    "data/sac",
    output_format="sac",
    inventory="data/metadata/stations.xml",
    max_workers=5,
)
```

This replaces “download SAC directly”: an FDSN dataselect response is normally
MiniSEED, so SAC creation belongs to local validation and archival rather than
network transport.

## Original-file policy and damaged records

`remove_original=False` is the safe default. With `remove_original=True`, a raw
response is removed only after every output derived from it has been validated
and committed. Failed sources remain in staging.

By default, an integrity warning triggers record-level recovery. Independently
valid MiniSEED records may still be archived, but the damaged original is
always retained as evidence—even when `remove_original=True`. Set
`discard_corrupt_records=False` to reject the whole response instead.

## Restricted data

Pass provider credentials only to the network stage:

```python
download.download_waveforms(
    "data/waveform-staging",
    client="https://service.earthscope.org",
    username="username",
    password="password",
    network="1U",
    starttime="2023-08-01",
    endtime="2025-05-01",
    inventory="data/metadata/1U_inventory.xml",
)
```

Load real credentials from environment variables or a secret manager; do not
commit them to a script.

## Reports, logs, and interrupted runs

Both stages enable JSON reports and text logs by default under their respective
output directories:

```text
<root>/logs/waveform-download-<run_id>.log
<root>/logs/reports/waveform-download-<run_id>.json
<root>/logs/waveform-archive-<run_id>.log
<root>/logs/reports/waveform-archive-<run_id>.json
```

A report starts with `status: "running"` and is updated atomically. Normal,
caught interruption, and exception exits become `completed`, `interrupted`,
and `failed`. If the operating system terminates the process before cleanup,
the last report remains `running`, preserving the last checkpoint. Set
`save_report=False` or `save_log=False` to disable either artifact.

## Choosing concurrency

- `network_workers` is I/O concurrency and defaults to 10. Respect provider
  connection and rate limits.
- archive `max_workers` is process concurrency and defaults to 5. Size it for
  available memory, CPU, and disk bandwidth.
- The two pools no longer impose backpressure on each other. Run the stages
  sequentially for the simplest workflow, or schedule archival independently
  using only raw files whose download has already committed.

[See download parameters →](../api/download.md#download-waveforms)
[See archive parameters →](../api/waveform.md#archive-raw-waveform-responses)
