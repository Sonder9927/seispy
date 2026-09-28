---
title: Quick start
description: Download, validate, and inspect one day of waveform data.
---

# Quick start

This first workflow downloads response metadata and one day of waveform data
for one station, validates the raw response into a MiniSEED archive, and checks
its time coverage.

## Install

```bash
git clone https://github.com/Sonder9927/seispy.git
cd seispy
uv sync
```

Run Python through the project environment with `uv run python`, or copy the
following cells into a notebook using the same environment.

## Complete example

Copy this entire script into the project environment. It downloads one station
for one day, validates the archive, and writes a coverage table. Internet access
and data availability at the provider are required.

```python
from seispy import download

inventory = download.download_inventory(
    "data/metadata/stations.xml",
    client="https://service.geonet.org.nz",
    network="NZ",
    station="WEL",
    channel="BH?",
    starttime="2025-01-01",
    endtime="2025-01-02",
    level="response",
)

downloaded = download.download_waveforms(
    "data/waveform-staging",
    client="https://service.geonet.org.nz",
    network="NZ",
    station="WEL",
    channel="BH?",
    starttime="2025-01-01",
    endtime="2025-01-02",
    inventory=inventory,
    network_workers=4,
)

print(downloaded.succeeded, downloaded.failed, downloaded.no_data)

from seispy import waveform
from seispy.waveform import TraceFilter

archived = waveform.archive_waveforms(
    "data/waveform-staging",
    "data/mseed",
    inventory=inventory,
    output_format="mseed",
    max_workers=2,
    trace_filter=TraceFilter(min_duration_seconds=60),
)

print(archived.succeeded, archived.failed)

coverage = waveform.waveform_coverage(
    "data/mseed/NZ",
    start_date="2025-01-01",
    end_date="2025-01-01",
    output_csv="data/metadata/waveform-coverage.csv",
)

print(coverage.summary)
```

## Outputs

<a id="1-download-stationxml"></a>
<a id="2-download-unverified-waveform-bytes"></a>
<a id="3-commit-the-trusted-miniseed-archive"></a>
<a id="4-measure-the-result"></a>

| Step | Output |
| --- | --- |
| Metadata | `data/metadata/stations.xml` and companion station CSV |
| Download | Unverified `.mseed.raw` files in `data/waveform-staging` |
| Archive | Validated MiniSEED in `data/mseed/NZ/WEL/2025` |
| Coverage | `data/metadata/waveform-coverage.csv` |

Source files are preserved. Check the printed failure counts before continuing.

## Common changes

- Change `network`, `station`, `channel`, and the dates in both download calls.
- Change the network directory and date range in the coverage call to match.
- Adjust `min_duration_seconds` for your shortest usable trace.

## Next step

[Convert to SAC](recipes/convert-miniseed.md#example) ·
[Download options](recipes/download-waveforms.md) ·
[Find another task](index.md#find-an-example)
