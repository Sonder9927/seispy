---
title: Project structure
description: Package responsibilities, dependency direction, and public seams.
---

# Project structure

SeisPy is organized by workflow domain. Applications normally enter through
`seispy.<domain>` and do not need to know where an implementation file lives.

```text
seispy/
├── archive.py     # waveform identity, naming, and archive invariants
├── workflow.py    # safe outputs, reports, logs, and run lifecycle
├── download/      # remote acquisition
├── inventory/     # station metadata analysis and manipulation
├── waveform/      # coverage, conversion, layout, headers, integrity, decimation
├── deconvolution/ # instrument-response removal
├── event/         # event catalogs and waveform cutting
├── correct/       # station timing and orientation corrections
└── mcmc/          # inversion input and result workflows
```

## Dependency direction

`archive` and `workflow` are deep foundation modules and do not depend on a
workflow domain. Domain packages may use them, while higher-level workflows may
compose other domains where the scientific operation requires it. In
particular, deconvolution uses StationXML safety checks and waveform decimation.

StationXML ownership is intentionally split by action rather than caller:

- `download` acquires metadata;
- `inventory` analyzes, selects, combines, adjusts, and writes metadata;
- `deconvolution` consumes metadata while removing an instrument response.

This keeps each rule in one canonical module and prevents download and
deconvolution workflows from accumulating duplicate StationXML utilities.

## Public interfaces

The supported user interface is exported from each domain package. Preferred
workflow names describe the outcome, for example
`waveform.convert_mseed_to_sac()` and `waveform.decimate_waveforms()`.

`seispy.archive` provides the archive identity interface. `seispy.workflow`
provides the observable batch-run interface used consistently by downloads and
file-processing workflows. No compatibility packages or duplicate operation
names are maintained in the 0.1 series.
