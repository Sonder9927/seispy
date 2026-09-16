---
title: Workflow recipes
description: Learn SeisPy through ordered, copyable seismic-data workflows.
---

# Workflow recipes

Each recipe teaches one outcome through one primary interface. Start with the
input you already have, run the minimal example, inspect the stated output,
then follow the next-step link. Complete parameter lists belong in the
[interface reference](../api/index.md).

## Understand the directory layout

The examples use one consistent project tree:

```text
data/
├── metadata/             # StationXML and station tables
├── catalog/              # earthquake catalogs
├── waveform-staging/     # unverified .mseed.raw responses
├── mseed/                # trusted MiniSEED archive
│   └── <network>/<station>/<year>/...
└── sac/                  # trusted SAC archive
    └── <network>/<station>/<year>/...
```

Functions whose first argument is `net_dir` expect a single network directory,
such as `data/sac/NZ`, rather than the format root `data/sac`.

Read [Archive layout](../design/archive-layout.md) before importing an existing
collection whose paths differ from this structure.

## 1. Acquire data

| Outcome | Start with | Primary interface |
| --- | --- | --- |
| Save response-level station metadata | network and time selectors | [`download.download_inventory`](download-inventory.md) |
| Save an earthquake catalog | time, magnitude, and region selectors | [`download.download_earthquake_events`](download-events.md) |
| Download known stations safely | StationXML or explicit stations | [`download.download_waveforms`](download-waveforms.md) |
| Discover and download a region | geographic constraints | [`download.mass_download_waveforms`](mass-download-waveforms.md) |

For a complete example rather than individual steps, use the
[GeoNet single-day workflow](geonet-one-day-workflow.md).

## 2. Build and inspect the waveform archive

| Outcome | Start with | Primary interface |
| --- | --- | --- |
| Validate staged downloads | `.mseed.raw` responses | [`waveform.archive_waveforms`](download-waveforms.md#archive-as-miniseed) |
| Organize scattered SAC files | SAC files in any directory layout | [`waveform.archive_waveforms`](archive-sac.md) |
| Measure actual or estimated coverage | one network directory | [`waveform.waveform_coverage`](waveform-coverage.md) |

## 3. Process continuous waveforms

| Outcome | Start with | Primary interface |
| --- | --- | --- |
| Convert MiniSEED to SAC | MiniSEED file or directory | [`waveform.convert_mseed_to_sac`](convert-miniseed.md) |
| Merge same-day SAC segments | canonical SAC archive | [`waveform.merge_waveforms_by_day`](merge-sac.md) |
| Deconvolve waveforms | waveform source tree plus StationXML | [`deconvolution.deconvolve_waveforms`](remove-response.md) |
| Reduce sampling rate | waveform directory and factor sequence | [`waveform.decimate_waveforms`](decimate.md) |

## 4. Prepare events and station corrections

| Outcome | Start with | Primary interface |
| --- | --- | --- |
| Cut event windows | SAC source tree plus event CSV | [`event.cut_event_waveforms`](cut-events.md) |
| Add event and station SAC headers | event tree plus two metadata tables | [`waveform.format_sac_headers`](format-headers.md) |
| Correct station timestamps | network directory plus drift CSV | [`correct.correct_clock_drift`](correct-clock-drift.md) |
| Correct horizontal orientation | three-component network directory plus orientation CSV | [`correct.correct_orientation`](correct-orientation.md) |

## 5. Prepare inversion inputs

Use [`mcmc.init_grids` and `mcmc.collect_results`](mcmc.md) after the waveform
and dispersion inputs have been validated.

## Reading a recipe

Every recipe follows the same order:

1. **Interface** — the function and its essential arguments.
2. **Input and output** — the required data contract and resulting paths.
3. **Example** — a safe, copyable call.
4. **Next step** — the natural continuation of the workflow.

Long-running functions return summaries and normally write reports and logs.
See [Understand batch reports and logs](batch-reports.md) once, then use
`summary.ok` and `summary.issue_samples` throughout the other recipes.
