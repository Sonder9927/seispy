---
title: Cut event windows
description: Extract event-centered windows from continuous SAC or MiniSEED archives.
---

# Cut event windows

<a id="interface"></a>

`event.cut_event_waveforms(source_dir, event_csv, output_dir=..., ...)`

**Output:** `event/network/station` directories containing losslessly merged SAC
windows. Gaps and conflicting overlaps remain as explicitly named segments.

### Inputs

- Any directory tree containing SAC or MiniSEED files; directory names and layout are ignored
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
    cache_max_bytes=256 * 1024**2,  # waveform cache budget per worker
)

print(f"Tasks: {summary.total}")
print(f"Outputs: {summary.outputs_written}")
print(f"No data: {summary.no_data}")
```

## Cut directly from a MiniSEED archive

MiniSEED output from `waveform.archive_waveforms` can be used directly without
converting the continuous archive to SAC:

```python
from seispy import event, waveform

if __name__ == "__main__":
    waveform.archive_waveforms(
        "data/raw", "data/archive", output_format="mseed",
    )
    summary = event.cut_event_waveforms(
        "data/archive", "data/catalog/events.csv",
        pattern="*.mseed",
        output_dir="data/events",
        station_csv="data/metadata/stations.csv",
        time_window=10_800,
    )
```

The filename pattern still defaults to `"*.sac"` for existing callers. Set
`pattern="*.mseed"` for MiniSEED input. Output is always SAC. Multi-trace files
are indexed by individual trace, including separate channels, stations, and
segments; cross-day traces are selected only once per window. File reads are
shared through the per-worker cache when the file fits its budget. Gaps and
conflicting overlaps remain segmented, just as with SAC input.

MiniSEED has no SAC headers. The cutter creates event and time headers, and
uses the optional station CSV for `latitude`, `longitude`, `elevation`, and
`depth`. Without station coordinates these fields and derived geometry remain
undefined. Supply the station CSV even if StationXML was used to validate the
archive: MiniSEED does not retain those coordinates or SAC picks. Existing SAC
input continues to preserve its station metadata and absolute pick times.

## Window selection

`time_window` is measured from the integer UTC second containing the event
origin (floor, not rounding): an origin at `12:34:56.789` produces a window
from `12:34:56.000` to `12:34:56.000 + time_window`. No interpolation is
performed; sample selection still follows the input waveform grid.
SeisPy reads each waveform file's headers once to build a `(network, station, UTC day)` overlap index. Stations
are processed concurrently, while each worker handles one station's events in
chronological order and reuses a bounded waveform cache. Selection is based on
actual header identity and time coverage rather than paths or filename dates.

`output_dir` defaults to a sibling named `<source_dir.name>_events`. It cannot be
inside `source_dir`, so a later run never indexes its own products. Blank
location codes remain blank and are represented as `--` only in filenames.

If `station_csv` is supplied, it must identify rows by `network,station` when
the source contains multiple networks. With exactly one network, the `network`
column may be omitted. Station coordinates are only replaced when the CSV
actually supplies them.

Reports and logs are enabled by default, track every event-station task, and
flush progress periodically. See [Batch reports and logs](batch-reports.md).

## Time headers and sample boundaries

Each cut keeps the nearest samples to the requested start and end, including
both endpoints. The index includes a half-sample margin, so a nearest sample
is not lost at a file or UTC-day boundary. A complete 3-hour window at 1 Hz
therefore has 10,801 samples. No samples are interpolated or padded.

SAC reference time is the integer-second cut start. `o` stores
the original event's fractional-second offset, while `b` and `e` locate the first
and last samples relative to that reference. Existing `a`, `f`, and `t0`–`t9`
markers are shifted to preserve their absolute times. All segments of an event
share the same reference. Event directory names retain microseconds when the
origin is not aligned to milliseconds, avoiding sub-millisecond collisions.
Distance and azimuth headers are recalculated for the new event when both event
and station coordinates are available; stale geometry is removed otherwise.

## Output filenames

Every Python output filename includes the event time through UTC integer
seconds, even when the waveform is split into segments:

```text
NZ.ABAZ.11.HHZ.20240102000000.sac       # one continuous output
NZ.ABAZ.11.HHZ.20240102000000.s01.sac   # first segment
NZ.ABAZ.11.HHZ.20240102000000.s02.sac   # second segment
```

The timestamp is the event time, not the individual segment start. Segment
start times are stored in the SAC headers. Empty location codes use `--`.
Event directories retain the precise origin to distinguish events within the
same second. Existing files are not renamed automatically.

## Discontinuous data: Python versus external tools

Integer-second alignment specifies the requested window, not gap filling.
Exact integer-second output starts require an input sample grid that contains
that time; otherwise the nearest available sample is retained without
interpolation.

| Input within the requested window | Python cutter | Tested external cutter |
| --- | --- | --- |
| Complete, contiguous records | One merged SAC file | One SAC file; samples match on aligned integer-Hz grids with integer-second windows |
| An internal gap | Separate SAC files for available segments; no zero filling | One SAC file with missing samples filled with zero |
| Partial coverage at window edges | Available data only; coverage is reported as partial | Behavior depends on the supplied tool; equality is not guaranteed |
| Conflicting overlapping records | Separate segments preserving conflicting samples | Behavior depends on the supplied tool; equality is not guaranteed |

For example, if a `00:00:00`–`00:10:00` window contains data only for
`00:00:00`–`00:04:00` and `00:06:00`–`00:10:00`, Python writes two SAC files
with `.s01` and `.s02` suffixes. Both filenames use the same event timestamp;
their different SAC `b` headers identify their actual starts. The external
cutter fills the missing interval with zeros in one file. These results are
intentionally different and must not be treated as equivalent complete data.

## Coverage, progress, and memory

`summary.partial_channels`, `gapped_channels`, and `overlapping_channels` count
event/channel windows with incomplete coverage, internal gaps, and unresolved
overlapping segments. `coverage_samples` contains up to `max_error_samples`
examples of partial or segmented windows. It describes available input coverage,
not a guarantee that every output was successfully written; check `failed` and
`outputs_written` as well. `existing_outputs` counts files preserved on reruns.

Coverage is the union of covered sample intervals on the requested nearest-sample
grid, with overlapping samples counted once. `covered_seconds` and
`requested_seconds` count one sampling interval per sample, including the end
sample; for example, a complete 1 Hz, 5-second window has 6 seconds of sample
support. Partial coverage and unresolved overlaps set `summary.has_issues` even
when writing the available segments succeeds. Missing channels with no indexed
data cannot be inferred from this statistic.

Workers report after at most 16 events or after an event completes and at least
one second has elapsed. A single long-running event is reported when it completes.
The waveform cache remains available across these progress updates.

Each worker retains at most 8 source files and `cache_max_bytes` of sample arrays
(256 MiB by default; zero disables retention). This is not a process-memory cap:
loading a large file and holding output/merge arrays needs additional memory.
For non-overlapping source records, only the event window plus a small boundary
margin is copied. Overlapping records retain full comparisons so conflicts
outside the window do not silently change merge decisions.

## External cutter

Use `event.cut_events_binary` only when the required `mktraceiodb` and
`cutevent` executables are installed. These tools must be supplied separately
by the user: their source code and executables are not included in this
repository or distributed with SeisPy. The optional tests call locally provided
executables through `SEISPY_CUT_BIN_DIR`; they do not contain or build the tools.

`write_event_catalog` retains microsecond precision in its time-only catalog.
Binary and ObsPy outputs use different gap/overlap policies: the 20160209 C
cutter fills missing samples with zero, whereas ObsPy preserves segments.
Both public cutters floor the cut start to the event's integer UTC second.
The binary wrapper creates a temporary integer-second catalog and restores
the precise origin in output SAC `o` headers; the input catalog is unchanged.
For continuous data on an integer-second sample grid, integer-Hz rates and
integer-second windows, the two methods select identical samples, including
the last sample. Arbitrary fractional window lengths or off-grid source times
can still expose the C cutter's one-microsecond endpoint subtraction.
The binary wrapper rejects catalogs with more than one event in the same
integer second, because the C output directory naming would collide; Python
retains precise event directory identities.
For opt-in compiled-cutter regression tests, set `SEISPY_CUT_BIN_DIR` to the
directory containing both executables and run
`pytest tests/test_event_binary_equivalence.py`.

[ObsPy cutting API →](../api/event.md#cut-events-with-obspy)
