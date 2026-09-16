---
title: Cut event windows
description: Extract event-centered windows from continuous SAC archives.
---

# Cut event windows

## Interface

`event.cut_event_waveforms(source_dir, event_csv, output_dir=..., ...)`

**Output:** `event/network/station` directories containing losslessly merged SAC
windows. Gaps and conflicting overlaps remain as explicitly named segments.

### Inputs

- Any directory tree containing SAC files; directory names and layout are ignored
- An event CSV with time, longitude, latitude, depth, and magnitude
- Optionally, a station metadata CSV

## Example

```python
from seispy import event

summary = event.cut_event_waveforms(
    "data/continuous",
    "data/catalog/events.csv",
    output_dir="data/events",
    station_csv="data/metadata/stations.csv",
    time_window=10_800,
    max_workers=16,
)

print(f"Tasks: {summary.total}")
print(f"Outputs: {summary.outputs_written}")
print(f"No data: {summary.no_data}")
```

`time_window` is measured in seconds after each event origin. SeisPy reads each
SAC header once to build a `(network, station, UTC day)` overlap index. Stations
are processed concurrently, while each worker handles one station's events in
chronological order and reuses a bounded waveform cache. Selection is based on
actual header identity and time coverage rather than paths or filename dates.

`output_dir` defaults to a sibling named `<source_dir.name>_events`. It cannot be
inside `source_dir`, so a later run never indexes its own products. Blank SAC
location codes remain blank and are represented as `--` only in filenames.

If `station_csv` is supplied, it must identify rows by `network,station` when
the source contains multiple networks. With exactly one network, the `network`
column may be omitted. Station coordinates are only replaced when the CSV
actually supplies them.

Reports and logs are enabled by default, track every event-station task, and
flush progress periodically. See [Batch reports and logs](batch-reports.md).

## External cutter

Use `event.cut_events_binary` only when the required `mktraceiodb` and
`cutevent` executables are installed.

[ObsPy cutting API →](../api/event.md#cut-events-with-obspy)
