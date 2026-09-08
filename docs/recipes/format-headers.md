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
from seispy import collate

summary = collate.format_head(
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

[See all parameters →](../api/collate.md#format-sac-headers)
