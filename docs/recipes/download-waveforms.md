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

print(f"Downloaded: {summary.downloaded}/{summary.total}")
print(f"No data: {summary.no_data}; failed: {summary.failed}")
```

## Result

Files are written below `data/waveforms/<network>/<station>/<year>/`.
The returned summary distinguishes downloaded, existing, no-data, and failed
requests.

When `inventory` is a StationXML path or an ObsPy `Inventory`, it is an exact
download manifest. Matching channel epochs are read locally and converted into
NSLC requests, clipped to both the metadata epoch and the requested time range,
then split at UTC-day boundaries. The station service is not queried again.
The `network`, `station`, `location`, and `channel` selectors are applied to
the manifest before tasks are created.

XML-guided MiniSEED uses one collision-free file per exact channel request:

```text
NZ.WEL.10.HHZ.2025.001.000000-2025002T000000.mseed
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
