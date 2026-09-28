---
title: Download and validate known-station waveforms
description: Download known stations and archive the results as MiniSEED or SAC.
---

# Download known stations

<a id="interfaces"></a>

**Input:** response-level [StationXML](download-inventory.md#example) at
`data/metadata/stations.xml`, covering the requested stations and dates.

**Output:** a validated MiniSEED or SAC archive. Change the paths, provider,
station selectors, and dates in the example to match your dataset.

## Download raw responses

Choose an output format, then copy the complete download-and-archive example.
The download writes unverified `.mseed.raw` files; archival validates them.

=== "MiniSEED"

    ```python
    from seispy import download

    download_summary = download.download_waveforms(
        "data/waveform-staging",
        client="https://service.geonet.org.nz",
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

    from seispy import waveform
    from seispy.waveform import TraceFilter

    archive_summary = waveform.archive_waveforms(
        "data/waveform-staging",
        "data/mseed",
        output_format="mseed",
        inventory="data/metadata/stations.xml",
        max_workers=5,
        trace_filter=TraceFilter(min_duration_seconds=60),
    )

    print(download_summary.succeeded, download_summary.failed)
    print(archive_summary.succeeded, archive_summary.failed)
    ```

=== "SAC"

    ```python
    from seispy import download

    download_summary = download.download_waveforms(
        "data/waveform-staging",
        client="https://service.geonet.org.nz",
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

    from seispy import waveform
    from seispy.waveform import TraceFilter

    sac_summary = waveform.archive_waveforms(
        "data/waveform-staging",
        "data/sac",
        output_format="sac",
        inventory="data/metadata/stations.xml",
        max_workers=5,
        trace_filter=TraceFilter(min_duration_seconds=60),
    )

    print(download_summary.succeeded, download_summary.failed)
    print(sac_summary.succeeded, sac_summary.failed)
    ```

## Output and common changes

- Raw responses: `data/waveform-staging/<network>/<station>/<year>/`.
- Trusted output: `data/mseed/` or `data/sac/` with the same station hierarchy.
- Use `network_workers` for download concurrency and `max_workers` for archival.
- Set `TraceFilter(min_duration_seconds=...)` to your minimum usable duration.
- Source files are preserved. Inspect `summary.issue_samples` if failures occur.

## Download details



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



Valid single-identity MiniSEED is copied without re-encoding. Zero-sample
boundary traces do not determine archive identity. When a response contains
multiple non-empty station-day groups, each valid group is written to its own
header-derived path; failure in one group does not discard the others. Headers,
archive identity, and—if StationXML is supplied—channel epoch and sample rate
are checked first. Constant (flat-line) and non-finite traces are rejected as
unusable, so a response that decodes cleanly but carries no signal is never
committed. Both examples pass
`trace_filter=TraceFilter(min_duration_seconds=60)`, which drops traces shorter
than a minute before they are written; omit it to archive every decodable trace,
or see [Filter waveforms before processing](filter-waveforms.md) to tune the
policy.
`max_workers` controls isolated archive processes and defaults to 5. A large
server may raise it independently of the downloader's `network_workers`; for
example, keep network transfers at 10 and use 40 archive workers if memory and
storage throughput permit.

## Archive as SAC

SAC uses the same staging input and integrity checks. Each resulting trace is
written to its canonical SAC path:



This replaces “download SAC directly”: an FDSN dataselect response is normally
MiniSEED, so SAC creation belongs to local validation and archival rather than
network transport.

## Source-file policy and damaged records

Archival never deletes or modifies staged responses. A source is successful if
at least one non-empty trace is preserved; it fails only when no usable trace
can be archived. By default, an integrity warning triggers record-level
recovery. Independently valid MiniSEED records and trace groups may still be
archived, while the damaged original remains available as evidence. Set
`discard_corrupt_records=False` to reject a response whose full read reports an
integrity warning.

Use `traces_total`, `traces_written`, `traces_existing`,
`traces_ignored_empty`, and `traces_failed` on the archive summary to audit
partial recovery. `recovered` counts source files that required filtering,
splitting, or omission while still preserving at least one trace.

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
[See archive parameters →](../api/waveform.md#archive-waveform-files)
