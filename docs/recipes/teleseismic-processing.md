---
title: Teleseismic events from MiniSEED
description: Cut teleseismic events from a continuous MiniSEED archive and remove response.
---

# Teleseismic events from MiniSEED

Start with a continuous 100 Hz MiniSEED archive and a StationXML inventory.
This example downloads a teleseismic catalog, cuts event windows to SAC, and
removes instrument response, then decimates the corrected events to 1 Hz.
It does not filter neighboring events. For OBS response and axis/clock
corrections, use [OBS teleseismic events](obs-teleseismic.md).

## Inputs

- A continuous 100 Hz MiniSEED archive, such as the `01_mseed` output from
  [GeoNet continuous data](geonet-one-day-workflow.md).
- StationXML responses and a station coordinate CSV; see
  [Cut event windows](cut-events.md) for the CSV fields.

## Processing order

Choose either path with `DECIMATE_FIRST`:

- `False`: cut directly from the 100 Hz archive, then remove response. When
  the inventory also specifies 100 Hz, there is no sampling-rate warning.
  Response removal applies a factor of 4 before deconvolution, producing 25 Hz events.
- `True`: decimate the continuous archive to 25 Hz MiniSEED first, then cut
  events and remove response. The waveform rate differs from the original
  100 Hz inventory, so response removal emits an expected sampling-rate warning.
  Keep the original inventory: do not edit its response stages to suppress it.

## Runnable example

Adjust the dates and paths. The catalog uses a minimum magnitude of 5.5
without geographic or epicentral-distance limits. Each event window is three
hours long. The archive must cover these windows, including time after the
catalog end date.

```python
from pathlib import Path

from seispy import deconvolution, download, event, waveform

DECIMATE_FIRST = True
MAX_WORKERS = 4


def main():
    catalog = Path("data/catalog/teleseismic.csv")
    catalog.parent.mkdir(parents=True, exist_ok=True)
    download.download_earthquake_events(
        "2024-01-01", "2024-02-01", catalog,
        client="USGS", minmagnitude=5.5,
    )

    source = "data/archive_100hz"
    if DECIMATE_FIRST:
        summary = waveform.decimate_waveforms(
            source, factors=4, pattern="*.mseed",
            output_format="mseed", output_dir="data/archive_25hz",
            max_workers=MAX_WORKERS,
        )
        if summary.has_issues:
            raise RuntimeError("Decimation failed; inspect the batch report")
        source = "data/archive_25hz"

    event.cut_event_waveforms(
        source, catalog, pattern="*.mseed",
        output_dir="data/events_counts",
        station_csv="data/metadata/stations.csv",
        time_window=3 * 3600,
        trace_filter=waveform.TraceFilter(min_duration_seconds=90 * 60),
        max_workers=MAX_WORKERS,
    )
    deconvolution.deconvolve_waveforms(
        "data/events_counts", "data/metadata/inventory.xml",
        output_dir="data/events_displacement_25hz",
        pre_filt=(0.004, 0.006, 4.0, 5.0),
        decimate_factors=None if DECIMATE_FIRST else [4],
        trace_filter=waveform.TraceFilter(min_duration_seconds=90 * 60),
        max_workers=MAX_WORKERS,
    )
    waveform.decimate_waveforms(
        "data/events_displacement_25hz", factors=[5, 5],
        output_dir="data/events_displacement_1hz",
        max_workers=MAX_WORKERS,
    )


if __name__ == "__main__":
    main()
```

The example explicitly uses SeisPy's default response pre-filter,
`(0.004, 0.006, 4.0, 5.0)` Hz. Response removal rejects traces shorter than
90 minutes and produces displacement in nanometers at 25 Hz in either path.
The final, separate decimation applies factors `[5, 5]` to those corrected
events, producing 1 Hz SAC files with anti-alias filtering.
Inspect each batch report for missing data, rejected traces, and failures.
MiniSEED contains no station coordinates, so supply the station CSV to the
cutter; see [Cut event windows](cut-events.md) for its fields and window timing.
No continuous SAC intermediate is required in either path.

## Output layout

```text
data/
├── catalog/
│   └── teleseismic.csv
├── archive_25hz/                       # Created only when DECIMATE_FIRST=True
├── events_counts/
│   └── YYYYmmddHHMMSS/                 # One directory per event
│       └── NET.STA.LOC.CHA.YYYYmmddHHMMSS.sac
├── events_displacement_25hz/
│   └── YYYYmmddHHMMSS/
│       └── NET.STA.LOC.CHA.YYYYmmddHHMMSS.sac
└── events_displacement_1hz/
    └── YYYYmmddHHMMSS/
        └── NET.STA.LOC.CHA.YYYYmmddHHMMSS.sac
```

`events_counts` retains instrument counts at the input rate (100 or 25 Hz).
`events_displacement_25hz` contains accepted, response-corrected traces in
nanometers; traces shorter than 90 minutes are excluded here.
`events_displacement_1hz` contains the final decimated displacement traces,
preserving the event directories and filenames. Gaps may produce separately
named segments, so an event can contain multiple files for one component.
Batch reports and logs are also saved under the corresponding output roots.

## Checks and next steps

Inspect batch reports before using subsequent stages. The final products are
1 Hz displacement in nanometers. This workflow does not rotate numbered
horizontal channels or correct clocks. Use the separate
[orientation](correct-orientation.md) and [clock](correct-clock-drift.md)
interfaces when the source metadata require those corrections.

Event cutting applies the 90-minute `TraceFilter` to each continuous segment
before writing SAC. Short segments are counted in `traces_filtered`; an
entirely filtered event/station task is `skipped`, not failed. Durations are
not summed across gaps. The response-stage filter retains the same threshold
as a check on its own input.
