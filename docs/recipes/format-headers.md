---
title: Populate event SAC headers
description: Populate event and station metadata in SAC headers.
---

# Populate event SAC headers

## Interface

`waveform.format_sac_headers(src_dir, dest_dir, event_csv, station_csv, ...)`

**Input:** an event waveform tree plus event and station CSV files.<br>
**Output:** a copied event tree with consistent station and event metadata in
each SAC header.

## Required tables

`events.csv` needs `time`, `latitude`, `longitude`, and `mag`. `stations.csv`
needs `station`, `latitude`, and `longitude`. Optional depth and elevation
columns are used when present.

## Example

```python
from seispy import waveform

summary = waveform.format_sac_headers(
    "data/events/raw",
    "data/events/formatted",
    "data/catalog/events.csv",
    "data/metadata/stations.csv",
    pattern="*.sac",
    max_workers=2,
    overwrite=False,
)

print(f"Formatted: {summary.succeeded}")
print(f"Skipped events: {summary.events_skipped}")
print(f"Failed files: {summary.failed}")
```

Output is written separately and the source event tree is preserved.

Reports and logs are enabled by default, track every event, and flush progress
periodically. See [Batch reports and logs](batch-reports.md).

[See all parameters →](../api/waveform.md#format-sac-headers)
