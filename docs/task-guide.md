---
title: Choose a workflow
description: Find the SeisPy recipe from your current input and desired output.
---

# Choose a workflow

The fastest entry point is your current data:

| I currently have… | I want… | Follow this recipe |
| --- | --- | --- |
| Network and time selectors | StationXML | [Download station metadata](recipes/download-inventory.md) |
| StationXML or a known station list | trusted MiniSEED or SAC | [Download and validate known-station waveforms](recipes/download-waveforms.md) |
| A geographic region | discovered stations and MiniSEED | [Discover regional waveforms](recipes/mass-download-waveforms.md) |
| Scattered SAC files | a canonical archive | [Organize SAC files](recipes/archive-sac.md) |
| A waveform archive | a coverage table and figure | [Measure network coverage](recipes/waveform-coverage.md) |
| MiniSEED | SAC | [Convert MiniSEED to SAC](recipes/convert-miniseed.md) |
| Multiple SAC segments per day | one file per channel-day | [Merge continuous SAC files](recipes/merge-sac.md) |
| Raw-count waveforms plus StationXML | physical units | [Remove instrument response](recipes/remove-response.md) |
| High-rate waveforms | a lower sampling rate | [Decimate waveforms](recipes/decimate.md) |
| Continuous SAC plus an event CSV | event windows | [Cut event windows](recipes/cut-events.md) |
| Event SAC plus metadata tables | populated SAC headers | [Format event SAC headers](recipes/format-headers.md) |
| SAC plus drift metadata | corrected timestamps | [Correct station clock drift](recipes/correct-clock-drift.md) |
| Three-component SAC plus orientation metadata | corrected horizontals | [Correct sensor orientation](recipes/correct-orientation.md) |
| Inversion source datasets | grid inputs and collected results | [Run the MCMC workflow](recipes/mcmc.md) |

## New to the project?

Choose one of these guided paths:

- **Five-minute orientation:** [Quick start](quickstart.md)
- **One complete acquisition and processing run:**
  [Process one day of GeoNet data](recipes/geonet-one-day-workflow.md)
- **Interactive 100 Hz walkthrough:**
  [Run the GeoNet Marimo notebook](recipes/geonet-100hz-notebook.md)
- **Browse every recipe in processing order:**
  [Workflow recipes](recipes/index.md)

## Recipe or interface reference?

Use a recipe to learn ordering, paths, safe defaults, and output inspection.
Use the [interface reference](api/index.md) after you know the workflow and need
every parameter, return field, or error mode.
