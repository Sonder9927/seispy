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
    overwrite=False,
)

print(f"Downloaded: {summary.downloaded}/{summary.total}")
print(f"No data: {summary.no_data}; failed: {summary.failed}")
```

## Result

Files are written below `data/waveforms/<network>/<station>/<year>/<day>/`.
The returned summary distinguishes downloaded, existing, no-data, and failed
requests.

!!! tip "Learn with a short interval"

    Start with one station and one or two days. Increase the interval and
    `max_workers` only after confirming the service and selectors.

[See all parameters →](../api/download.md#download-waveforms)
