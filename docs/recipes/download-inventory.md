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
- Parent directories are created automatically.

!!! note "Restricted metadata"

    Pass both `username` and `password` when the FDSN service requires
    authentication. Supplying only one raises `ValueError`.

[See all parameters →](../api/download.md#download-station-metadata)
