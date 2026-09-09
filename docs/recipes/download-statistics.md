---
title: Check download completeness
description: Summarize and plot daily waveform availability by station.
---

# Check download completeness

**Use this when:** you want to find gaps after downloading daily MiniSEED or SAC
waveforms and export a figure for a report or paper.

## Expected layout

Pass the network directory containing the station folders:

```text
data/waveforms/NZ/
├── AAA/2024/001/*.sac
├── AAA/2024/002/*.sac
└── BBB/2024/001/*.mseed
```

Empty files and invalid year or Julian-day directories are ignored.

## Example

```python
from seispy import download

report = download.download_status(
    "data/waveforms/NZ",
    start_date="2024-01-01",
    end_date="2024-12-31",
    output_figure="figures/download-status.pdf",
    output_csv="data/download-status.csv",
    station_order="availability",
)

print(report.summary)
```

## Result

`report.availability` contains one row per station-day, including file count and
total size. `report.summary` reports available, expected, and missing days plus
completeness percentage for each station.

The single figure places the availability timeline and completeness percentage
on the same station row. It uses a colorblind-safe blue, alternating rows,
concise UTC date labels, and vector-friendly styling. Choose `.pdf` or `.svg`
for manuscripts and `.png` for slides or web pages.

## Advanced use

Use `scan_download_availability`, `summarize_download_availability`, and
`plot_download_availability` separately when you need to modify the intermediate
DataFrames or customize the Matplotlib axes.

!!! note "Meaning of an available day"

    Availability is based on at least one non-empty waveform file in the daily
    directory. It does not inspect sample continuity inside each waveform.

[See all parameters →](../api/download.md#download-statistics)
