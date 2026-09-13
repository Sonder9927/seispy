---
title: Format SAC headers
description: Populate event and station metadata in SAC headers.
---

# Format SAC headers

**Use this when:** event directories and station traces exist, but SAC headers
need consistent coordinates and event metadata.

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
