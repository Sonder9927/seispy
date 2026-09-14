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

At response level, duplicate metadata is normalized when this can be done
without guessing. Equivalent overlapping epochs are merged; when different
responses overlap, the later start time takes precedence and closes the older
epoch.

If two response epochs remain ambiguous (for example, they have the same
channel and start time but different responses), the default is deliberately
lossless: the original inventory returned by the FDSN service is written to
StationXML and CSV, returned unchanged, and a `ResponseConflictWarning` is
emitted. The function never silently selects the first response.

Set `strict_response_conflicts=True` to treat this as an error. The raw XML and
CSV are still written before `ResponseConflictError` is raised, so downloaded
metadata is not lost:

```python
inventory = download.download_inventory(
    "data/metadata/stations.xml",
    network="NZ",
    level="response",
    strict_response_conflicts=True,
)
```

The XML is both an instrument-response archive and a reusable waveform download
manifest. Passing it to `download_waveforms` avoids another station-service
query and excludes days for which no matching active channel appears in the
metadata:

```python
summary = download.download_waveforms(
    "data/waveform-staging",
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
