---
title: Run the MCMC preparation workflow
description: Generate per-grid inputs and collect completed inversion outputs.
---

# MCMC workflow

## Interfaces

- `mcmc.init_grids(config_file)` builds per-grid inversion inputs serially.
- `mcmc.plot_grids(grids_dir)` redraws the per-point figures from the written files.
- `mcmc.collect_results(src_dir, dest_dir)` collects completed outputs and prints a per-grid diagnostic table.

**Input:** a JSON configuration and its referenced geophysical datasets.<br>
**Output:** per-grid inversion directories followed by collected result tables
and figures.

## Generate grid inputs

Prepare a JSON configuration matching `seispy.mcmc.config.Config`, including input
paths, region, grid spacing, physical constraints, and output directory.
Start from the [complete config.json example and quantity definitions](../api/mcmc.md#complete-configjson-example).

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
[the Moho discontinuity and the interface coefficients](../api/mcmc.md#the-moho-discontinuity-and-the-interface-coefficients).

![Per-point figure at 122.00_33.50: phase dispersion and Vs search intervals
with projection centres](../assets/mcmc-point-example.png)

A point with fewer valid dispersion rows than
`phase_constraints.minimum_periods` is skipped before its directory is created,
so skipped points leave no files and no folder.

## Collect results

After the external inversion has completed in each grid directory:

```python
from seispy import mcmc

mcmc.collect_results(
    "output/grids",
    "output/summary",
)
```

The summary directory contains copied probability figures, `vs.csv`, and
`misfit_moho_lab.csv`. A failed point is reported in the terminal diagnostic
table without stopping other points. Pass `diagnostics_path=...` to save that
table explicitly.

!!! note "Start with one grid point"

    MCMC inputs depend on several external data products. Validate one point
    before preparing a full region.

[See the MCMC API →](../api/mcmc.md)
