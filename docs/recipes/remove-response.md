---
title: Remove instrument response
description: Deconvolve SAC files with StationXML while preserving source data.
---

# Remove instrument response

## Prerequisite

Download response-level StationXML that covers the network, stations, channels,
and recording interval. See [Station metadata](download-inventory.md).

## Example

```python
from seispy import response

summary = response.deconvolution_by_station(
    "data/sac",
    "data/metadata/stations.xml",
    method="obspy",
    pattern="*.sac",
    output_dir="data/deconvolved",
    remove_original=False,
    max_workers=2,
)

print(f"Processed: {summary.succeeded}/{summary.total}")
print(f"Failed: {summary.failed}")
```

## Inspect issues

```python
for issue in summary.issue_samples:
    print(issue.source, issue.status, issue.error)
```

!!! note "SAC backend"

    `method="sac"` uses external SAC tools and pole-zero responses. The ObsPy
    backend is the easier starting point.

[See all parameters →](../api/response.md#remove-instrument-responses)
