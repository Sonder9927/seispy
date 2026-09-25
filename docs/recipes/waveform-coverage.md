---
title: Measure network waveform coverage
description: Measure actual daily waveform time coverage by station.
---

# Measure network waveform coverage

## Interface

`waveform.waveform_coverage(net_dir, ...)`

**Input:** one network directory inside a waveform tree.<br>
**Output:** station-day coverage, station summaries, and optional CSV and
figure files.

## Expected layout

Pass the network directory containing station folders:

```text
data/mseed/NZ/
├── AAA/2024/NZ.AAA.2024.001.mseed
└── BBB/2024/NZ.BBB.2024.001.mseed
```

The function reads trace headers and treats each sample interval as
`[starttime, endtime + delta)`. Cross-midnight traces are split at UTC day
boundaries, and overlapping intervals are counted only once.

## Reading modes

The default `read_mode="header"` reads waveform headers and measures actual
sample-time coverage. Use it when correctness matters or files may be partial,
cross midnight, contain gaps, or come from outside SeisPy. Header mode derives
identity from trace headers and ignores the directory layout, so it measures
response-removed trees too. Pass `require_canonical_paths=True` to reject any
file whose path disagrees with its headers, preserving the strict
canonical-archive contract.

For a fast inventory of a large canonical archive, use filename mode:

```python
from seispy import waveform

report = waveform.waveform_coverage(
    "data/mseed/NZ",
    start_date="2024-01-01",
    end_date="2024-12-31",
    read_mode="filename",
)
```

`read_mode="filename"` does not open waveform files. It interprets each name
through `layout` and estimates every non-empty station-day file as 100%
covered. It cannot detect truncated data, internal gaps, cross-midnight
coverage, incorrect headers, or damaged waveform bytes. The `read_mode`
column in `report.coverage` records which interpretation was used.

`layout` selects the tree's file-naming policy:

- `layout="archive"` (default) accepts canonical SeisPy names such as
  `NZ.AAA.10.HHZ.2024.001.000000.sac` and `NZ.AAA.2024.001.mseed`.
- `layout="deconvolved"` accepts response-removed SAC trees that keep the
  source stem, such as `NZ.AAA.10.HHZ.2024.001.sac`, plus multi-trace
  segment names such as `NZ.AAA.10.HHZ.2024.001T010000000.sac`.

Response removal writes one SAC file per trace under the same
`network/station/year` tree. Point `net_dir` at that tree's network
directory:

```python
report = waveform.waveform_coverage(
    "data/deconvolved/NZ",
    read_mode="filename",
    layout="deconvolved",
)
```

## Example

```python
from seispy import waveform

report = waveform.waveform_coverage(
    "data/mseed/NZ",
    start_date="2024-01-01",
    end_date="2024-12-31",
    output_figure="figures/waveform-coverage.pdf",
    output_csv="data/metadata/waveform-coverage.csv",
    station_order="coverage",
    read_mode="header",
)

print(report.coverage)
print(report.summary)
```

## Result

`report.coverage` contains one row per station-day. `coverage_seconds` is the
union of all trace intervals for that station and day, capped at 86,400 seconds;
`coverage_percent` is therefore always between 0 and 100. `file_count` and
`trace_count` describe the inputs contributing to that union.

`report.summary` aggregates actual seconds across the inclusive requested date
range. It distinguishes days containing any coverage from fully covered days
and reports total expected, covered, and missing seconds.

The figure is a station-by-day heat map: white means no coverage and the full
configured color means 100% daily coverage. The percentage shown beside each
station is its aggregate coverage across the requested period.

## Advanced use

Use `scan_waveform_coverage`, `summarize_waveform_coverage`, and
`plot_waveform_coverage` separately when you need to modify intermediate data
or customize the Matplotlib axes.

!!! note "Station-level union"

    Coverage is the union of all channels and locations observed for a station.
    Concurrent channels do not increase coverage beyond 100%. This measures
    whether the station has waveform data through time; it does not assert that
    every expected component is present.

[See all parameters →](../api/waveform.md#measure-waveform-coverage)
