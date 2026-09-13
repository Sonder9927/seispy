---
title: SeisPy
description: Task-oriented examples for seismic data processing with SeisPy.
---

# Learn SeisPy by task

SeisPy documentation is organized around **what you want to accomplish**. Pick
a task, copy the example, then follow its link to the full API only when you
need every parameter.

<div class="hero-actions" markdown>
[Choose a task](task-guide.md){ .md-button .md-button--primary }
[Run the quick start](quickstart.md){ .md-button }
</div>

## Download data

Need a complete, runnable example? Follow the
[GeoNet one-day workflow from 100 Hz MiniSEED to 1 Hz SAC](recipes/geonet-one-day-workflow.md).

<div class="task-grid" markdown>
<div class="task-card" markdown>
### Station metadata

Download response-level StationXML from an FDSN service.

[View recipe →](recipes/download-inventory.md)
</div>

<div class="task-card" markdown>
### Earthquake catalog

Query events and save a normalized CSV catalog.

[View recipe →](recipes/download-events.md)
</div>

<div class="task-card" markdown>
### Waveforms

Download daily MiniSEED or SAC files by station.

[View recipe →](recipes/download-waveforms.md)
</div>
</div>

## Prepare waveforms

| I want to... | Copyable example |
| --- | --- |
| Convert MiniSEED to SAC | [MiniSEED to SAC](recipes/convert-miniseed.md) |
| Organize and merge daily SAC files | [Sort and merge](recipes/sort-merge.md) |
| Add event and station metadata to SAC headers | [Format headers](recipes/format-headers.md) |
| Remove an instrument response | [Remove response](recipes/remove-response.md) |
| Reduce the sampling rate | [Decimate](recipes/decimate.md) |

## Analyze events and stations

| I want to... | Copyable example |
| --- | --- |
| Cut event windows from continuous data | [Cut events](recipes/cut-events.md) |
| Correct clock drift or sensor orientation | [Correct stations](recipes/correct-stations.md) |
| Generate inputs and collect MCMC results | [MCMC workflow](recipes/mcmc.md) |

!!! tip "A simple learning path"

    Start with [MiniSEED to SAC](recipes/convert-miniseed.md), continue with
    [decimation](recipes/decimate.md), and use the returned summary objects to
    understand what each batch operation changed.

!!! warning "Protect source data"

    Keep `remove_original=False` or `remove_src=False` while learning. Enable
    deletion only after validating output on representative files.

## Look up exact parameters

Recipes teach the workflow. The [API reference](api/index.md) provides complete
signatures, return models, and source links. You can also inspect any function
without opening this site:

```python
from seispy import response

help(response.remove_instrument_response)
```
