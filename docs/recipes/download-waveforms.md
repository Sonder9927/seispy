---
title: Download waveforms
description: Download daily MiniSEED or SAC waveform files from FDSN.
---

# Download waveforms

**Use this when:** you want a daily waveform archive grouped by network,
station, year, and Julian day.

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
)

print(f"Downloaded: {summary.downloaded}/{summary.total}")
print(f"No data: {summary.no_data}; failed: {summary.failed}")
```

## Result

Files are written below `data/waveforms/<network>/<station>/<year>/<day>/`.
The returned summary distinguishes downloaded, existing, no-data, and failed
requests.

Existing days are checked before a network request is made. MiniSEED uses its
deterministic output filename. SAC files are matched directly by network,
station, location, channel, and date, so no bookkeeping files are added to the
waveform archive. This makes rerunning the same command an efficient way to
resume an interrupted archive.

!!! tip "Learn with a short interval"

    Start with one station and one or two days. Increase the interval and
    `max_workers` only after confirming the service and selectors.

!!! note "Be considerate of public FDSN services"

    More workers are not always faster. Start with 2–5 workers and follow the
    data provider's usage policy. Temporary request failures are retried with
    exponential backoff; no-data responses are not retried.

[See all parameters →](../api/download.md#download-waveforms)
