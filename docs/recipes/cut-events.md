---
title: Cut event windows
description: Extract event-centered windows from continuous SAC archives.
---

# Cut event windows

## Inputs

- Continuous SAC data grouped by station
- An event CSV with time, longitude, latitude, depth, and magnitude
- Optionally, a station metadata CSV

## Example

```python
from seispy import event

summary = event.cut_events(
    "data/continuous",
    "data/events",
    "data/catalog/events.csv",
    station_csv="data/metadata/stations.csv",
    time_window=10_800,
)

print(f"Tasks: {summary.tasks_total}")
print(f"Outputs: {summary.outputs_written}")
print(f"No data: {summary.no_data}")
```

`time_window` is measured in seconds after each event origin. Start with a small
catalog to confirm the archive naming and station coverage.

## External cutter

Use `event.cut_events_binary` only when the required `mktraceiodb` and
`cutevent` executables are installed.

[ObsPy cutting API →](../api/event.md#cut-events-with-obspy)
