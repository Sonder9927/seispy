---
title: OBS teleseismic events
description: Reuse the 50 Hz OBS archive, cut events, remove response, correct clocks and axes, and decimate to 1 Hz.
---

# OBS teleseismic events

Reuse the uncalibrated 50 Hz SAC archive from
[OBS continuous data](obs-processing.md), then process only the selected event
windows. The result is three-component Z/N/E displacement at 1 Hz.

## Inputs

- `data/obs/processed/01_archive`: the 50 Hz source archive, including BHH.
- `data/obs/metadata/obs_inventory.xml`: source responses, linear clock models,
  and raw-axis directions, as described in [OBS input and metadata](obs-processing.md#input-and-metadata).
- `data/obs/metadata/stations_obs.csv`: station coordinates for event cutting.

Reuse `01_archive` even if the daily workflow has also produced calibrated
25 Hz intermediate or 5 Hz final products. Do not repeat response removal or clock/orientation correction
on those later products. Starting from `01_archive` makes the event midpoint
the reference for the constant clock shift and keeps event processing independent
of the daily calibrated products.

If you have not created an archive, set `SOURCE = ROOT / "sac_Zdenoise"` instead.
The cutter uses SAC headers and accepts that directory layout directly; there
is no need to create a duplicate archive solely for this recipe. Both inputs
must still be uncalibrated 50 Hz records with the correct NSLC headers.
BHH is excluded by the event-cut pattern.

## Processing order

1. Download the magnitude-limited catalog and cut three-hour event windows.
2. Remove response, with factor-two decimation from 50 to 25 Hz before
   deconvolution and a minimum accepted duration of 90 minutes.
3. Shift each event trace using its midpoint clock offset, without interpolation.
4. Rotate the seismic components to Z/N/E with tilt correction disabled.
5. Decimate the corrected 25 Hz events to 1 Hz using factors `[5, 5]`.

Cutting first limits response removal and rotation to the event data you need.
Clock correction changes the timestamps of the already selected windows; it
does not recut a window with exact GPS-time boundaries. The original XML
remains unchanged. Because the input to response removal is still 50 Hz,
matching this OBS inventory, the integrated factor-two decimation does not
produce a sampling-rate mismatch warning.

## Runnable example

Save this as a separate script. Adjust the catalog dates to the available OBS
recording period. The station CSV supplies the event cutter with station
coordinates. The source SAC headers must contain the correct NSLC identifiers.

```python
from pathlib import Path

from seispy import deconvolution, download, event, waveform

ROOT = Path("data/obs")
SOURCE = ROOT / "processed" / "01_archive"
INVENTORY = ROOT / "metadata" / "obs_inventory.xml"
WORK = ROOT / "teleseismic"
CATALOG = WORK / "catalog" / "events.csv"
COUNTS = WORK / "01_events_counts_50hz"
RESPONSE = WORK / "02_displacement_25hz"
CLOCK = WORK / "03_clock_shifted_25hz"
ORIENTED = WORK / "04_zne_25hz"
FINAL = WORK / "05_zne_1hz"
MAX_WORKERS = 4


def main():
    CATALOG.parent.mkdir(parents=True, exist_ok=True)
    download.download_earthquake_events(
        "2024-07-20", "2024-07-21", CATALOG,
        client="USGS", minmagnitude=5.5,
    )
    cut = event.cut_event_waveforms(
        SOURCE, CATALOG,
        pattern="*BH[ZNE]*.sac",
        output_dir=COUNTS,
        station_csv=ROOT / "metadata" / "stations_obs.csv",
        time_window=3 * 3600,
        trace_filter=waveform.TraceFilter(min_duration_seconds=90 * 60),
        max_workers=MAX_WORKERS,
    )
    print(cut)

    response = deconvolution.deconvolve_waveforms(
        COUNTS, INVENTORY,
        output_dir=RESPONSE,
        pre_filt=(0.004, 0.006, 4.0, 5.0),
        decimate_factors=[2],
        trace_filter=waveform.TraceFilter(min_duration_seconds=90 * 60),
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(response)

    clocks = waveform.correct_clock_drift(
        RESPONSE, INVENTORY,
        output_dir=CLOCK,
        layout="event",
        channels=("BHZ", "BHN", "BHE"),
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(clocks)
    if clocks.has_issues:
        raise RuntimeError("Clock correction failed; inspect the batch report")

    orientation = waveform.correct_orientation(
        CLOCK, INVENTORY,
        output_dir=ORIENTED,
        layout="event",
        components=("BHZ", "BHN", "BHE"),
        apply_tilt=False,
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(orientation)
    if orientation.has_issues:
        raise RuntimeError("Orientation correction failed; inspect the batch report")

    final = waveform.decimate_waveforms(
        ORIENTED, factors=[5, 5], output_dir=FINAL,
        max_workers=MAX_WORKERS,
    )
    print(final)


if __name__ == "__main__":
    main()
```

## Checks and interpretation

The midpoint clock shift preserves samples and their sampling interval.
Correcting event windows can produce slightly different timestamps from
correcting complete days first, because the midpoint is different.
`layout="event"` explicitly identifies direct child directories as events;
no directory-name date parsing is required. Components must share a sampling
grid; unequal coverage can produce different
midpoint shifts, and fractional-sample misalignment is rejected. Rotation
writes only common coverage and preserves real gaps. Review rejected short
traces and missing components in the reports. The current OBS tilt metadata is
nominal, so `apply_tilt=False` is appropriate.

Corrections do not automatically resume. Existing correction outputs require
a new output directory or an explicit `overwrite=True` rerun. Review all batch
summaries before using the results.

## Output layout

```text
data/obs/teleseismic/
├── catalog/events.csv
├── 01_events_counts_50hz/
│   └── YYYYmmddHHMMSS/*.sac
├── 02_displacement_25hz/
│   └── YYYYmmddHHMMSS/*.sac
├── 03_clock_shifted_25hz/
│   └── YYYYmmddHHMMSS/*.sac
├── 04_zne_25hz/
│   └── YYYYmmddHHMMSS/*.sac
└── 05_zne_1hz/
    └── YYYYmmddHHMMSS/*.sac
```

Event directories remain separate throughout the workflow, including overlapping
events on the same day and windows crossing midnight. Clock correction preserves
input filenames; orientation names Z/N/E outputs using header NSLC and the event
directory name, adding segment suffixes when needed. Final decimation preserves
that structure. The event directory keeps the catalog
origin identifier even after waveform timestamps shift. Displacement remains
in nanometers through the final 1 Hz products. Reports and logs accompany the
stage output roots.

For continuous 5 Hz daily products, return to [OBS continuous data](obs-processing.md).

Event cutting applies the 90-minute `TraceFilter` to each continuous segment
before writing SAC. Short segments are counted in `traces_filtered`; an
entirely filtered event/station task is `skipped`, not failed. Durations are
not summed across gaps. The response-stage filter retains the same threshold
as a check on its own input.
