---
title: Choose a task
description: Find the SeisPy recipe that matches a processing goal.
---

# Choose a task

Start from the outcome you need. Every recipe contains prerequisites, a
copyable example, expected output, safety notes, and a link to the exact API.

| Goal | Recipe | Primary function |
| --- | --- | --- |
| Download StationXML | [Station metadata](recipes/download-inventory.md) | `download_inventory` |
| Build an event CSV | [Earthquake catalog](recipes/download-events.md) | `download_earthquake_events` |
| Download daily waveforms | [Waveforms](recipes/download-waveforms.md) | `download_waveforms` |
| Convert MiniSEED files | [MiniSEED to SAC](recipes/convert-miniseed.md) | `mseed2sac` |
| Organize daily files | [Sort and merge SAC](recipes/sort-merge.md) | `sort_to`, `merge_by_day` |
| Populate SAC headers | [Format SAC headers](recipes/format-headers.md) | `format_head` |
| Remove instrument response | [Remove response](recipes/remove-response.md) | `deconvolution_by_station` |
| Change sampling rate | [Resample](recipes/resample.md) | `resample_by_station` |
| Extract event windows | [Cut event windows](recipes/cut-events.md) | `cut_events` |
| Correct station timing | [Correct stations](recipes/correct-stations.md) | `clock_drift` |
| Correct sensor orientation | [Correct stations](recipes/correct-stations.md) | `orientation` |
| Prepare an inversion | [MCMC workflow](recipes/mcmc.md) | `init_grids` |
| Collect inversion outputs | [MCMC workflow](recipes/mcmc.md) | `collect_results` |

## Browse by input

- **StationXML needed:** [download metadata](recipes/download-inventory.md)
- **MiniSEED files:** [convert to SAC](recipes/convert-miniseed.md)
- **Continuous SAC files:** [sort and merge](recipes/sort-merge.md),
  [remove response](recipes/remove-response.md), or
  [cut events](recipes/cut-events.md)
- **Event and station CSV files:** [format headers](recipes/format-headers.md)
- **Completed inversion directories:** [collect results](recipes/mcmc.md#collect-results)

## Browse by output

- **StationXML:** [Station metadata](recipes/download-inventory.md)
- **Event CSV:** [Earthquake catalog](recipes/download-events.md)
- **SAC directory tree:** [MiniSEED to SAC](recipes/convert-miniseed.md)
- **Processed waveform copy:** [Remove response](recipes/remove-response.md) or
  [resample](recipes/resample.md)
- **Event waveform directories:** [Cut event windows](recipes/cut-events.md)
- **MCMC input grids and summary CSV files:** [MCMC workflow](recipes/mcmc.md)
