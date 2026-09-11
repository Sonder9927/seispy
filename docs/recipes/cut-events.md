---
title: Cut event windows
description: Extract event-centered windows from continuous SAC archives.
---

# Cut event windows

## Inputs

- A canonical network directory containing `station/year/file.sac`
- An event CSV with time, longitude, latitude, depth, and magnitude
- Optionally, a station metadata CSV

## Example

```python
from seispy import event

summary = event.cut_events(
    "data/continuous/NZ",
    "data/events",
    "data/catalog/events.csv",
    station_csv="data/metadata/stations.csv",
    time_window=10_800,
)

print(f"Tasks: {summary.tasks_total}")
print(f"Outputs: {summary.outputs_written}")
print(f"No data: {summary.no_data}")
```

`time_window` is measured in seconds after each event origin. SeisPy reads each
SAC header once to build a time-overlap index, then reuses a bounded waveform
cache while processing events chronologically. Selection is based on actual
header coverage rather than filename dates.

## External cutter

Use `event.cut_events_binary` only when the required `mktraceiodb` and
`cutevent` executables are installed.

[ObsPy cutting API →](../api/event.md#cut-events-with-obspy)
