---
title: Run the MCMC preparation workflow
description: Generate per-grid inputs and collect completed inversion outputs.
---

# MCMC workflow

<a id="interfaces"></a>

- `mcmc.init_grids(config_file)` builds per-grid inversion inputs serially.
- `mcmc.plot_grids(grids_dir)` redraws the per-point figures from the written files.
- `mcmc.collect_results(src_dir, dest_dir)` collects completed outputs into Parquet tables and returns diagnostics without printing.

**Input:** a JSON configuration and its referenced geophysical datasets.<br>
**Output:** per-grid inversion directories followed by collected result tables
and figures.

## Generate grid inputs

Prepare a JSON configuration matching `seispy.mcmc.config.Config`, including input
paths, region, grid spacing, physical constraints, and output directory.
Start from the [complete config.json example and quantity definitions](../mcmc/configuration.md#complete-configjson-example).

```python
from seispy import mcmc

mcmc.init_grids(
    "config/mcmc.json",
    plot=True,  # optional: also write point.png per grid point
)
```

The workflow reads topography, sediment thickness, Moho depth, reference
velocity models, and phase-dispersion data, then creates one inversion directory
per valid grid point. Each directory holds `phase.input`, `para.inp`,
`input_DRAM_T.dat`, `prior_bounds.csv`, and `point.json`; with `plot=True` it
also contains a combined dispersion and Vs-model figure. Writing and plotting
are separate phases, so a figure failure cannot lose inputs and the figures can
be regenerated later:

```python
mcmc.plot_grids("output/grids")  # rebuild point.png from the written files
```

Prior centres are the least-squares projection of the reference profile into
the Fortran coefficient space, so the two interface coefficients already carry
the reference model's Moho contrast; no synthetic jump is needed. See
[the Moho discontinuity and the interface coefficients](../mcmc/parameterization.md#the-moho-discontinuity-and-the-interface-coefficients).

[![Per-point figure at 122.00_33.50: phase dispersion and Vs search intervals
with projection centres](../assets/mcmc-point-example.png){ .example-figure }](../assets/mcmc-point-example.png)

A point with fewer valid dispersion rows than
`phase_constraints.minimum_periods` is skipped before its directory is created,
so skipped points leave no files and no folder.

## Collect results

After the external inversion has completed in each grid directory:

```python
from seispy import mcmc

summary = mcmc.collect_results(
    "output/grids",
    "output/summary",
    lab_settings=mcmc.LABSettings(
        min_depth_km=50, max_depth_km=250,
        moho_buffer_km=15, smoothing_km=5,
        min_drop_km_s=0.05, min_width_km=10,
    ),
)

import pandas as pd
profiles = pd.read_parquet(summary.output_paths["profiles"])
interfaces = pd.read_parquet(summary.output_paths["interfaces"])
issues = summary.diagnostics.query("status != 'ok'")
```

Install the `mcmc` extra for the Parquet dependency. Collection creates:

| Product | Contents |
| --- | --- |
| `profiles.parquet` | Original `x,y,z,vs`, plus full-chain `vsv_mean`, `vsv_std`, `vsv_median`, `vsv_q16`, `vsv_q84`, `vsv_q025`, `vsv_q975` (and other enabled components) |
| `interfaces.parquet` | Original `x,y,misfit,moho`; updated `lab`; separately named posterior Moho/LAB/width/drop/thickness statistics and detection fractions |
| `diagnostics.parquet` | One row per grid, with `ok`, `partial`, or `failed`, messages and the retained posterior path |
| `metadata.json` | Run ID, source definitions, units and all LAB settings |
| `posterior/<run_id>/<grid>/` | Compressed derived samples with multiplicities, ordered retained chain, parameter covariance and input snapshots |

The original `vs`, `moho`, and `misfit` values are intentionally preserved from
`mean_prob.lst`, `moho.lst`, and `Litmod_output.log`. They retain the limitations
of those Fortran summaries; they are **not** silently replaced by full-chain
estimates. Use `vsv_mean` and `moho_km_mean` for the new estimates. `misfit` is
not a log posterior or MAP score. Original `z` and updated `lab` are negative
downward; `moho` and explicitly named `*_km*` depth quantities are positive
downward. Unavailable LAB is NaN, never -999.

Collection reads `para.inp` and `input_DRAM_T.dat` written by `init_grids` in each
point directory, and checks coordinate identity against `point.json` when
present. Keep these inputs with the outputs of the matching inversion; source
datasets and a separate current configuration are not needed. Full-chain
statistics require the normal two-header-line `fort.8010`, matching retained
row count, and standard random acceptance (`biasfac >= 1`). Biased runs,
forward-only exports and truncated chains are not used as posterior samples.
A valid historical summary with missing/invalid posterior inputs is `partial`:
its old fields remain available, but LAB is missing. Completely unreadable
summaries are `failed`. `success_count` counts both `ok` and `partial` grids;
inspect diagnostics to count complete posterior products.

The LAB method reconstructs each unique state with the Fortran layer geometry,
interpolates its mantle velocity nodes to a regular grid, smooths at a specified
physical scale, and identifies negative-gradient peaks. It is a seismic proxy,
not a thermal isotherm. Width is the half-prominence width of the gradient peak;
drop is measured across that width in the smoothed velocity. Candidate searches
exclude the Moho buffer and reject boundary-limited or ambiguous detections.
Settings above are operational defaults, not calibrated universal thresholds;
check sensitivity and recovery for the study's depth resolution.

`lab_depth_km_*` intervals are conditional on unambiguous detection. Always use
`lab_detected_fraction`, `lab_ambiguous_fraction`, and `lab_boundary_limited_fraction`
alongside the reported depth. Per-state candidate lists remain in
`samples.parquet`; multiple peaks across the ensemble are not automatically
collapsed into distinct geological interpretations. Retained repeated states
keep their iteration multiplicities; no likelihood reweighting is applied.
Pointwise velocity intervals are not simultaneous whole-profile bands.

A single-chain autocorrelation ESS estimate is reported when estimable; short
or constant chains have unavailable ESS, and R-hat is unavailable without
independent chains. These diagnostics do not certify convergence. No automatic
thermal conversions or posterior predictive calculations are performed.

Each collection gets a separate run directory for sample products and copied
probability figures, preventing stale samples from failed grids being mistaken
for the current run. Top-level tables refer to the latest collection. Collection
prints nothing; display, filter or log `summary.diagnostics` in the caller.

!!! note "Start with one grid point"

    MCMC inputs depend on several external data products. Validate one point
    before preparing a full region.

[See the MCMC API →](../api/mcmc.md)
