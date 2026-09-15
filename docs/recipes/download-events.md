---
title: Download an earthquake catalog
description: Query FDSN events and save a normalized SeisPy CSV.
---

# Download an earthquake catalog

## Interface

`download.download_earthquake_events(starttime, endtime, output_file, ...)`

**Input:** time range plus optional magnitude and geographic selectors.<br>
**Output:** a normalized event `DataFrame` and CSV for header formatting or
event-window cutting.

## Example

```python
from seispy import download

events = download.download_earthquake_events(
    "2025-01-01",
    "2025-02-01",
    "data/catalog/events.csv",
    client="USGS",
    minmagnitude=5.5,
    minlatitude=-50,
    maxlatitude=-30,
    minlongitude=160,
    maxlongitude=180,
)

print(events[["time", "longitude", "latitude", "depth", "mag"]].head())
```

## Result

The returned data frame and CSV contain normalized UTC time, longitude,
latitude, depth in kilometers, magnitude, and magnitude type.

## Next step

Use the catalog with [Cut event windows](cut-events.md) or
[Format SAC headers](format-headers.md).

[See all parameters →](../api/download.md#download-earthquake-events)
