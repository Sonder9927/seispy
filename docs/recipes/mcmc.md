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

After the external inversions finish, collect only the values needed for mapping
and interpretation. No retained-chain reconstruction is performed.

```python
import pandas as pd
from seispy import mcmc

lab_settings = mcmc.LABSettings(
    min_depth_km=50.0,       # Minimum LAB center depth (km).
    max_depth_km=200.0,      # Maximum LAB center depth (km).
    moho_buffer_km=15.0,     # Minimum distance below the mean Moho (km).
    smoothing_km=5.0,        # Gaussian sigma on the mean Vs profile (km).
    min_drop_km_s=0.015,      # Minimum localized velocity decrease (km/s).
)
summary = mcmc.collect_results(
    grids_dir="output/grids",
    out_dir="output/summary-simple",
    lab_settings=lab_settings,
    collect_prob_figures=False,  # Set True to copy existing probalCr.png files.
)

print(f"Total grids: {summary.grid_count}")
print(f"Collected grids: {summary.success_count}")
print(f"Failed grids: {summary.failed_count}")
print(f"Valid LAB estimates: {summary.lab_valid_count}")
print(f"NaN LAB estimates: {summary.lab_nan_count}")
print(f"Probability figures copied: {summary.probability_figures}")
if summary.errors:
    print("Collection errors:")
    for grid, message in summary.errors.items():
        print(f"  {grid}: {message}")

profiles = pd.read_parquet(summary.output_paths["profiles"])
interfaces = pd.read_parquet(summary.output_paths["interfaces"])
print("Grids with NaN LAB estimates:")
print(interfaces.loc[interfaces["lab"].isna(), ["x", "y"]].to_string(index=False))
```

Install the `mcmc` extra for the Parquet dependency. The collector writes exactly
two numerical products by default:

| Product | Columns |
| --- | --- |
| `profiles.parquet` | `x, y, z, vs` |
| `interfaces.parquet` | `x, y, misfit, moho, lab` |

`vs` reads the second column of `mean_prob.lst`; `misfit` reads the same final-log
field as before; `moho` is the arithmetic mean of `moho.lst`. These definitions
are unchanged. Coordinates and all depth samples are preserved; depths are
positive downward in kilometres and velocities are in km/s.

Each grid needs only `mean_prob.lst`, `Litmod_output.log` and `moho.lst`.
The collector does not read `fort.8010`, `para.inp` or the original source data,
and does not write posterior statistics, sample exports or metadata. A malformed grid is recorded in `summary.errors` without preventing
other grids from being collected. The function prints nothing automatically.

Set `collect_prob_figures=True` to also copy available `probalCr.png` files
from successfully collected grids. `summary.probability_figures` gives the number
copied. When a figure directory is created, its path is available through
`summary.output_paths["prob_figures"]`. Each enabled run uses a separate directory
under `prob_figures/`, so earlier copies cannot be mistaken for the current run.
Missing figures are skipped. Copy errors appear under image-path keys in
`summary.errors` and do not discard numerical results or increase `failed_count`.
No figures are generated or copied by default.

`lab_nan_count` counts NaN interface rows. A completely failed grid has no
interface row and is counted separately by `failed_count`. Therefore:

```text
lab_valid_count + lab_nan_count = success_count
success_count + failed_count = grid_count
```

The two tables are prepared before either published output is replaced.
Existing unrelated files in the output directory are not removed.

### LAB calculation

LAB is calculated **once per grid from the mean Vs profile**, using the mean
Moho as the crustal boundary. It is the center of a localized velocity-decrease
band, rather than the minimum velocity or a thermal isotherm.

1. Select the mantle part of `mean_prob.lst`. Interpolate onto a regular grid
   with spacing equal to the larger of 1 km and the median supplied mantle
   depth spacing. This is numerical sampling, not physical resolving power.
2. Apply Gaussian smoothing with sigma `smoothing_km` and calculate the negative
   velocity gradient. Candidate centers must lie between
   `max(min_depth_km, moho + moho_buffer_km)` and `max_depth_km`.
3. Find descent peaks with numerical height/prominence at least 1e-8 and an
   excess above background of at least 20% of the peak height. Background is the
   larger of zero, the median mantle descent and the local prominence baseline.
   Band shoulders occur at 10% of the excess above that background.
4. Subtract background descent times band width from the observed shoulder-to-
   shoulder velocity drop. Require this localized drop to be at least
   `min_drop_km_s` and the band to span at least two evaluation intervals.
   Require the center to be more than three smoothing sigmas from actual mantle
   profile endpoints. A band censored at both profile ends is rejected.
5. Return the gradient-peak depth of the strongest qualifying band. If a second
   distinct qualifying band has at least 80% of its drop, return NaN. Missing
   coverage or no qualifying band also gives NaN.

The five settings above are operational choices, not universal or
literature-calibrated thresholds. A 60–150 km regional prior can be expressed by
changing the search bounds; narrowing the range does not guarantee more valid
estimates. Profiles with no localized decrease can correctly retain NaN.

On the 24-grid `data/mcmc-v3` dataset, reducing `min_drop_km_s` from
0.020 to 0.015 km/s increased finite estimates from 17 to 20 without changing
the original 17 depths. The 5 km smoothing scale was retained. This is a
dataset-specific sensitivity check, not independent validation of LAB accuracy;
the threshold can be overridden for other datasets.

A derived quantity of the mean model generally differs from an aggregate of
per-state quantities;
posterior propagation such as that discussed by
[Hazzard et al. (2023)](https://doi.org/10.1029/2023JB026653) and
[Xing and Beghein (2015)](https://doi.org/10.1093/gji/ggv401) requires analysis of individual
retained states. This collector provides no posterior support
fraction, credible interval, covariance or convergence diagnostic.
A reported number is a mean-profile estimate, not evidence of posterior
certainty.

The seismic estimate also differs from the thermal LAB defined using the
1250 degrees C isotherm by
[Salajegheh et al. (2025)](https://doi.org/10.1029/2024GC011724).

### References

Hazzard, J. A. N., Richards, F. D., Goes, S. D. B. & Roberts, G. G. (2023).
Probabilistic assessment of Antarctic thermomechanical structure: Impacts on
ice sheet stability.
*Journal of Geophysical Research: Solid Earth*, **128**, e2023JB026653.
[https://doi.org/10.1029/2023JB026653](https://doi.org/10.1029/2023JB026653)

Salajegheh, F., Afonso, J. C., Minakov, A., Ajourlou, P., Gaina, C. &
Ortega-Gelabert, O. (2025).
The lithosphere-asthenosphere system beneath the North Atlantic and
surroundings: Results from multi-observable probabilistic
inversions.
*Geochemistry, Geophysics, Geosystems*, **26**, e2024GC011724.
[https://doi.org/10.1029/2024GC011724](https://doi.org/10.1029/2024GC011724)

Xing, Z. & Beghein, C. (2015).
A Bayesian approach to assess the importance of crustal corrections in global
anisotropic surface wave tomography.
*Geophysical Journal International*, **203**, 1832–1846.
[https://doi.org/10.1093/gji/ggv401](https://doi.org/10.1093/gji/ggv401)

!!! note "Start with one grid point"

    MCMC inputs depend on several external data products. Validate one point
    before preparing a full region.

[See the MCMC API →](../api/mcmc.md)
