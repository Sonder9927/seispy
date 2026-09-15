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

## 1. Download StationXML

Interface: `download.download_inventory(output_file, ...)`

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
```

This creates `data/metadata/stations.xml` and a companion station CSV.

## 2. Download unverified waveform bytes

Interface: `download.download_waveforms(output_dir, ...)`

```python
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
```

Successful responses retain the `.mseed.raw` suffix because they have not yet
been decoded and validated.

## 3. Commit the trusted MiniSEED archive

Interface: `waveform.archive_waveforms(source_dir, output_dir, ...)`

```python
from seispy import waveform

archived = waveform.archive_waveforms(
    "data/waveform-staging",
    "data/mseed",
    inventory=inventory,
    output_format="mseed",
    max_workers=2,
    remove_original=False,
)

print(archived.succeeded, archived.failed)
```

Trusted output is written below `data/mseed/NZ/WEL/2025/`. The staging file is
kept because this learning example uses `remove_original=False`.

## 4. Measure the result

Interface: `waveform.waveform_coverage(net_dir, ...)`

```python
coverage = waveform.waveform_coverage(
    "data/mseed/NZ",
    start_date="2025-01-01",
    end_date="2025-01-01",
    output_csv="data/metadata/waveform-coverage.csv",
)

print(coverage.summary)
```

## Continue learning

- Convert the archive with [Convert MiniSEED to SAC](recipes/convert-miniseed.md).
- Understand acquisition and recovery policy in
  [Download and validate known-station waveforms](recipes/download-waveforms.md).
- Find another path from [Choose a workflow](task-guide.md).
- Look up exact parameters in the [interface reference](api/index.md).
