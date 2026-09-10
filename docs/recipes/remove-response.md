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

The default pre-filter is `(0.004, 0.006, 4.0, 5.0)` Hz. Both backends taper
at most 5% from each edge and cap each edge at 600 seconds for daily records.
Only gaps of one second or less are interpolated; longer gaps fail explicitly.
Before processing, inventories with overlapping response epochs trigger a
lightweight header preflight. `summary.response_conflicts` reports how many
waveform files do not have one unambiguous response covering the complete
trace. Those files fail safely; the remaining files continue and no response
is selected arbitrarily.

Every generated file is validated before it replaces or accompanies the
source. Empty, constant, non-finite, truncated, time-shifted, or sampling-rate
mismatched output is rejected. ObsPy sample values are checked directly in the
already-loaded processed stream; SAC output uses its stored amplitude extrema.
Only fixed-size headers are reread, avoiding a second full read of every daily
waveform.

## Inspect issues

```python
for issue in summary.issue_samples:
    print(issue.source, issue.status, issue.error)
```

!!! note "SAC backend"

    `method="sac"` uses the same StationXML input and selects the response by
    network, station, location, channel, and recording time. Matching temporary
    pole-zero files are cached by response epoch. The SAC backend processes at
    most 100 files per SAC process by default, so two years of daily data does
    not create one oversized session. Set `sac_batch_size` to tune the balance
    between startup overhead and failure isolation. If a SAC process fails,
    its batch is split recursively until the individual bad file is isolated;
    valid neighbors are retained without slowing down successful batches.

[See all parameters →](../api/response.md#remove-instrument-responses)
