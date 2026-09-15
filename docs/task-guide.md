---
title: Choose a task
description: Find the SeisPy recipe that matches a processing goal.
---

# Choose a task

Start from the outcome you need. Every recipe contains prerequisites, a
copyable example, expected output, safety notes, and a link to the exact API.

| Goal | Recipe | Primary function |
| --- | --- | --- |
| Learn XML-guided GeoNet 100 Hz downloads interactively | [GeoNet Marimo notebook](recipes/geonet-100hz-notebook.md) | `download_inventory`, `download_waveforms` |
| Process one GeoNet day end to end | [GeoNet one-day workflow](recipes/geonet-one-day-workflow.md) | download, response removal, decimation |
| Download StationXML | [Station metadata](recipes/download-inventory.md) | `download_inventory` |
| Build an event CSV | [Earthquake catalog](recipes/download-events.md) | `download_earthquake_events` |
| Download daily waveforms | [Waveforms](recipes/download-waveforms.md) | `download_waveforms` |
| Bulk-download from providers | [Experimental MassDownloader](recipes/mass-download-waveforms.md) | `mass_download_waveforms` |
| Measure station waveform coverage | [Waveform coverage](recipes/waveform-coverage.md) | `waveform_coverage` |
| Convert MiniSEED files | [MiniSEED to SAC](recipes/convert-miniseed.md) | `convert_mseed_to_sac` |
| Organize daily files | [Archive and merge SAC](recipes/sort-merge.md) | `archive_waveforms`, `merge_waveforms_by_day` |
| Populate SAC headers | [Format SAC headers](recipes/format-headers.md) | `format_sac_headers` |
| Remove instrument response | [Remove response](recipes/remove-response.md) | `remove_instrument_response` |
| Reduce sampling rate | [Decimate](recipes/decimate.md) | `decimate_waveforms` |
| Extract event windows | [Cut event windows](recipes/cut-events.md) | `cut_event_waveforms` |
| Correct station timing | [Correct stations](recipes/correct-stations.md) | `correct_clock_drift` |
| Correct sensor orientation | [Correct stations](recipes/correct-stations.md) | `orientation` |
| Prepare an inversion | [MCMC workflow](recipes/mcmc.md) | `init_grids` |
| Collect inversion outputs | [MCMC workflow](recipes/mcmc.md) | `collect_results` |

## Browse by input

- **StationXML needed:** [download metadata](recipes/download-inventory.md)
- **MiniSEED files:** [convert to SAC](recipes/convert-miniseed.md)
- **Waveform archive:** [measure coverage](recipes/waveform-coverage.md)
- **Continuous SAC files:** [archive and merge](recipes/sort-merge.md),
  [remove response](recipes/remove-response.md), or
  [cut events](recipes/cut-events.md)
- **Event and station CSV files:** [format headers](recipes/format-headers.md)
- **Completed inversion directories:** [collect results](recipes/mcmc.md#collect-results)

## Browse by output

- **StationXML:** [Station metadata](recipes/download-inventory.md)
- **Event CSV:** [Earthquake catalog](recipes/download-events.md)
- **SAC directory tree:** [MiniSEED to SAC](recipes/convert-miniseed.md)
- **Processed waveform copy:** [Remove response](recipes/remove-response.md) or
  [resample](recipes/decimate.md)
- **Event waveform directories:** [Cut event windows](recipes/cut-events.md)
- **MCMC input grids and summary CSV files:** [MCMC workflow](recipes/mcmc.md)
