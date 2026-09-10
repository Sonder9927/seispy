---
title: Download station metadata
description: Download response-level StationXML from an FDSN service.
---

# Download station metadata

**Use this when:** you need station coordinates or instrument responses before
response removal.

## Example

```python
from seispy import download

inventory = download.download_inventory(
    "data/metadata/stations.xml",
    client="https://service.earthscope.org",
    network="NZ",
    station="WEL",
    channel="BH?",
    starttime="2025-01-01",
    endtime="2025-02-01",
    level="response",
)

print(inventory)
```

## Result

- The function returns an ObsPy `Inventory`.
- `data/metadata/stations.xml` contains the same metadata as StationXML.
- `data/metadata/stations.csv` is created automatically with network and station
  codes, station name, coordinates, elevation, active dates, locations, and
  channels.
- Parent directories are created automatically.

The XML is both an instrument-response archive and a reusable waveform download
manifest. Passing it to `download_waveforms` avoids another station-service
query and excludes days for which no matching active channel appears in the
metadata:

```python
summary = download.download_waveforms(
    "data/waveforms",
    network="NZ",
    starttime="2025-01-01",
    endtime="2025-02-01",
    station="*",
    location="*",
    channel="BH?",
    inventory="data/metadata/stations.xml",
)
```

!!! note "Restricted metadata"

    Pass both `username` and `password` when the FDSN service requires
    authentication. Supplying only one raises `ValueError`.

[See all parameters →](../api/download.md#download-station-metadata)
