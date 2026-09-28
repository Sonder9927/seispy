---
title: SeisPy
description: Find a task, copy an example, and process seismic data.
---

# SeisPy

Seismic data processing with ObsPy. Choose your input below to jump straight
to a working example; change the paths and selectors to match your data.

[Install and run your first workflow](quickstart.md){ .md-button .md-button--primary }

## Find an example

| Input | Task | Copy an example |
| --- | --- | --- |
| Time and region selectors | Earthquake catalog | [Download events](recipes/download-events.md#example) |
| Network and time selectors | StationXML | [Download station metadata](recipes/download-inventory.md#example) |
| StationXML or a known station list | trusted MiniSEED or SAC | [Download and validate known-station waveforms](recipes/download-waveforms.md#download-raw-responses) |
| A geographic region | discovered stations and MiniSEED | [Discover regional waveforms](recipes/mass-download-waveforms.md#recommended-example) |
| Scattered SAC files | a canonical archive | [Organize SAC files](recipes/archive-sac.md#example) |
| A waveform tree with short fragments | only usable traces | [Filter waveform traces](recipes/filter-waveforms.md#example) |
| A waveform archive | a coverage table and figure | [Measure network coverage](recipes/waveform-coverage.md#example) |
| MiniSEED | SAC | [Convert MiniSEED to SAC](recipes/convert-miniseed.md#example) |
| Multiple SAC segments per day | one file per channel-day | [Merge continuous SAC files](recipes/merge-sac.md#example) |
| Raw-count waveforms plus StationXML | physical units | [Remove instrument response](recipes/remove-response.md#example) |
| High-rate waveforms | a lower sampling rate | [Decimate waveforms](recipes/decimate.md#scipy-example) |
| Continuous SAC plus an event CSV | event windows | [Cut event windows](recipes/cut-events.md#example) |
| Event SAC plus metadata tables | populated SAC headers | [Format event SAC headers](recipes/format-headers.md#example) |
| SAC plus drift metadata | corrected timestamps | [Correct station clock drift](recipes/correct-clock-drift.md#example) |
| Three-component SAC plus orientation metadata | corrected horizontals | [Correct sensor orientation](recipes/correct-orientation.md#example) |
| Inversion source datasets | grid inputs and collected results | [Run the MCMC workflow](recipes/mcmc.md#generate-grid-inputs) |

## Complete workflows

- [GeoNet: download and process one day](recipes/geonet-one-day-workflow.md)
- [Interactive GeoNet notebook](recipes/geonet-100hz-notebook.md)

## Reference

[All API parameters](api/index.md) · [Archive paths](design/archive-layout.md) ·
[Reports and logs](recipes/batch-reports.md)
