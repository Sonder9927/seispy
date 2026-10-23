---
title: Populate daily and event SAC headers
description: Fill station and event metadata by copying files or editing headers in place.
---

# Populate daily and event SAC headers

<a id="interface"></a>

`waveform.format_sac_headers(src_dir, dest_dir=None, events_csv=None, stations_csv=..., ...)`

The function recursively discovers SAC files and matches station metadata using
**SAC network and station headers**, independently of filenames. Channel,
network, station and location identifiers are preserved, including empty location
codes. It changes metadata bytes without loading or rewriting waveform samples.

<a id="example"></a>

## Daily data: station metadata only

Omit the event table. To copy files into a separate output tree:

```python
from seispy import waveform

summary = waveform.format_sac_headers(
    "data/daydata_deconvolved",
    dest_dir="data/daydata_formatted",
    stations_csv="data/metadata/ghf_stations.csv",
    max_workers=4,
)
```

To update the source files **in place**, omit `dest_dir` (or set it to `None`):

```python
summary = waveform.format_sac_headers(
    "data/daydata_deconvolved",
    stations_csv="data/metadata/ghf_stations.csv",
)
```

Station-only runs preserve all event metadata. When station coordinates change,
geometry is recalculated using any existing event coordinates; if those are
missing or invalid, stale geometry is cleared. No event is inferred from the
recording date.

## Event data: station and event metadata

```python
summary = waveform.format_sac_headers(
    "data/events_deconvolved",
    dest_dir="data/events_formatted",  # omit to modify source headers in place
    events_csv="data/catalog/events.csv",
    stations_csv="data/metadata/ghf_stations.csv",
    max_workers=4,
    overwrite=False,
)

print(summary.succeeded, summary.failed, summary.skipped)
```

An event is matched by a `YYYYmmddHHMMSS` directory anywhere between the source
root and the file. The source root itself can be an event directory. Catalog
times are interpreted in UTC and matched at integer-second precision. Multiple
catalog events in the same second are rejected before processing. Unparseable
event times are counted; files without matching event metadata fail visibly.

This is metadata editing, not time normalization: reference time, `b/e/o`, all
picks, sample interval and sample count are preserved byte for byte. A missing
`o` is not created; use the event cutter to establish event time references.

## Output, overwrite, and recovery

With `dest_dir`, each file is first copied to a temporary file beside its target.
Only that copy's header is edited, then it is committed to the final path. Source
and destination trees must not overlap. Relative paths and filenames stay intact:

```text
source/20241217221040/XX.GL04.--.BHN.20241217221040.s01.sac
output/20241217221040/XX.GL04.--.BHN.20241217221040.s01.sac
```

`overwrite=False` skips an existing destination and counts it in `skipped` and
`output_conflicts`; `overwrite=True` replaces it after successful processing.
This is an existence check, not a check that old metadata matches the tables.

Without `dest_dir`, `overwrite` has no effect: source headers are always updated
from the tables. Files already matching the metadata are counted as `unchanged`
(within `succeeded`) and are not written, preserving their modification time.
In-place edits save the original header and v7 footer bytes and attempt to restore
them on catchable write failures. This does not protect against power loss,
forced termination, or a storage failure that also prevents restoration. It is
safe to repeat a completed or partially completed run with the same tables.
In-place symlinks and files with multiple hard links are rejected to avoid editing
aliases; copy mode can read a source file symlink without modifying its target.

Supported files are evenly spaced binary SAC time series, v6 and v7, in either
byte order. The original format and byte order are preserved; v7 double-precision
coordinates are updated along with the float header, while its double-precision
time fields remain untouched. Truncated files, unsupported formats and size
mismatches are reported as failures before any in-place modification.

## Tables and units

`stations.csv` requires `station`, `latitude`, and `longitude`. An optional
`network` column disambiguates stations with the same name. Without that column,
station names must be unique. When the SAC network is absent, only a unique
station match is accepted; defined networks must match the table or a generic
station row. Optional `elevation` and `depth` populate `stel` and `stdp` in metres.

`events.csv` requires `time`, `latitude`, `longitude`, and `mag`. Optional `depth`
populates `evdp` in **kilometres**; optional `elevation` populates `evel` in metres.
Missing optional metadata preserves the corresponding existing fields. Table
coordinates must be finite, with latitude in [-90, 90] and longitude in [-180, 180].
Invalid numeric metadata and ambiguous table keys are rejected before editing.

## Performance and reports

The implementation copies bytes and edits only the 632-byte header and, for v7,
its 176-byte footer. It does not recalculate waveform extrema. Bounded batches
use up to `max_workers` threads; small runs and `max_workers=1` run directly,
without a process pool or a multiprocessing main guard. Geometry results are
cached across channels and segments within the run.

`total = succeeded + failed + skipped`. Event counters refer to distinct path
groups, and are zero when no event table is supplied. A run matching no files
raises an error instead of returning an empty success. Existing `logs` directories
are excluded from discovery.

Reports and logs are enabled by default under the output root, or the source root
for in-place runs. To disable them, use `save_report=False, save_log=False`.
See [Batch reports and logs](batch-reports.md).

**Migration:** older versions required both tables, parsed identifiers from a
legacy filename and renamed outputs. The four positional arguments remain
accepted, but output paths now preserve the input tree, empty location codes are
kept, and destination conflicts are counted as skipped rather than failed.

[See all parameters →](../api/waveform.md#format-sac-headers)

## Distance and azimuth convention

Station and event coordinates explicitly refresh `dist`, `az`, `baz`, and
`gcarc` before writing; setting `lcalda` alone does not guarantee that ObsPy
updates existing geometry. Event cutting uses the same calculation.

`dist` is the WGS84 ellipsoidal geodesic distance in kilometres, evaluated with
GeographicLib (including nearly antipodal points). `az` (event to station),
`baz` (station to event), and `gcarc` use great-circle geometry after converting
geographic latitudes to WGS84 geocentric latitudes. This follows SAC's angular
convention; `gcarc` is **not** the ellipsoidal distance divided by a fixed Earth
radius. Distance can differ slightly from SAC's legacy distance calculation.
Depth and elevation do not enter these surface-geometry calculations.

With valid coordinates, `lcalda=True`. Missing, undefined, nonfinite, or
out-of-range coordinates clear all four geometry fields and set `lcalda=False`.
Coincident and exactly antipodal points have undefined azimuths, stored as SAC
undefined values; their distances and angular distances remain defined.
