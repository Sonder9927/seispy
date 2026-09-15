---
title: Measure waveform coverage
description: Measure actual daily waveform time coverage by station.
---

# Measure waveform coverage

**Use this when:** you want to quantify how much waveform data is present for
each station and UTC day, regardless of how the files were acquired.

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
cross midnight, contain gaps, or come from outside SeisPy.

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

`read_mode="filename"` does not open waveform files. It validates the
`network/station/year/filename` layout and estimates every non-empty canonical
station-day file as 100% covered. It cannot detect truncated data, internal
gaps, cross-midnight coverage, incorrect headers, or damaged waveform bytes.
The `read_mode` column in `report.coverage` records which interpretation was
used.

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
