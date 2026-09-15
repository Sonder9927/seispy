---
title: Run the MCMC preparation workflow
description: Generate per-grid inputs and collect completed inversion outputs.
---

# MCMC workflow

## Interfaces

- `mcmc.init_grids(config_file, ...)` builds per-grid inversion inputs.
- `mcmc.collect_results(src_dir, dest_dir)` collects completed outputs.

**Input:** a JSON configuration and its referenced geophysical datasets.<br>
**Output:** per-grid inversion directories followed by collected result tables
and figures.

## Generate grid inputs

Prepare a JSON configuration matching `seispy.mcmc.configuration.Config`, including input
paths, region, grid spacing, physical constraints, and output directory.

```python
from seispy import mcmc

mcmc.init_grids(
    "config/mcmc.json",
    max_workers=4,
)
```

The workflow reads topography, sediment thickness, Moho depth, reference
velocity models, and phase-dispersion data, then creates one inversion directory
per valid grid point.

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
`misfit_moho_lab.csv`.

!!! note "Start with one grid point"

    MCMC inputs depend on several external data products. Validate one point
    before enabling multiple workers over a full region.

[See the MCMC API →](../api/mcmc.md)
