# Quick start

## Install

Clone the repository and create the locked environment:

```bash
git clone https://github.com/Sonder9927/seispy.git
cd seispy
uv sync
```

## Convert and resample waveforms

```python
from seispy import collate, resample_by_station

conversion = collate.mseed2sac(
    "data/miniseed",
    "data/sac",
    remove_original=False,
)
print(conversion.succeeded, conversion.failed)

resampling = resample_by_station(
    "data/sac",
    delta=1.0,
    output_dir="data/resampled",
    remove_original=False,
)
print(resampling.succeeded, resampling.failed)
```

## Remove an instrument response

```python
from seispy import download, response

inventory = download.download_inventory(
    "data/stations.xml",
    network="NZ",
    station="WEL",
    channel="BH?",
)

summary = response.deconvolution_by_station(
    "data/sac",
    "data/stations.xml",
    output_dir="data/deconvolved",
    remove_original=False,
)
print(summary.succeeded, summary.failed)
```

## Work with events

```python
from seispy import download, event

download.download_earthquake_events(
    "2025-01-01",
    "2025-02-01",
    "data/events.csv",
    minmagnitude=5.5,
)

summary = event.cut_events(
    "data/continuous",
    "data/events",
    "data/events.csv",
    station_csv="data/stations.csv",
    time_window=10_800,
)
print(summary.tasks_succeeded, summary.tasks_failed)
```

## Inspect an interface interactively

```python
from seispy import response

help(response.deconvolution_by_station)
print(response.__all__)
```

The public objects exported by each package are stable discovery points. Names
that start with an underscore are implementation details.
