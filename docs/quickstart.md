# Quick start

## Install

Clone the repository and create the locked environment:

```bash
git clone https://github.com/Sonder9927/seispy.git
cd seispy
uv sync
```

## Convert and decimate waveforms

```python
from seispy import waveform

conversion = waveform.convert_mseed_to_sac(
    "data/mseed",
    "data/sac",
    remove_original=False,
)
print(conversion.succeeded, conversion.failed)

decimation = waveform.decimate_waveforms(
    "data/sac",
    factors=[5, 5, 4],
    output_dir="data/decimated",
    remove_original=False,
)
print(decimation.succeeded, decimation.failed)
```

## Remove an instrument response

```python
from seispy import deconvolution, download

inventory = download.download_inventory(
    "data/metadata/stations.xml",
    network="NZ",
    station="WEL",
    channel="BH?",
)

summary = deconvolution.remove_instrument_response(
    "data/sac",
    "data/metadata/stations.xml",
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

summary = event.cut_event_waveforms(
    "data/continuous",
    "data/events",
    "data/events.csv",
    station_csv="data/stations.csv",
    time_window=10_800,
)
print(summary.succeeded, summary.failed)
```

## Inspect an interface interactively

```python
from seispy import deconvolution

help(deconvolution.remove_instrument_response)
print(deconvolution.__all__)
```

The public objects exported by each package are stable discovery points. Names
that start with an underscore are implementation details.
