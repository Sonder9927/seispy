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

# Defaults perform chain reconstruction, LAB identification and quality checks.
summary = mcmc.collect_results("output/grids", "output/summary")

import pandas as pd
profiles = pd.read_parquet(summary.output_paths["profiles"])
interfaces = pd.read_parquet(summary.output_paths["interfaces"])
issues = summary.diagnostics.query("status != 'ok'")
```

Install the `mcmc` extra for the Parquet dependency. Collection creates:

| Product | Contents |
| --- | --- |
| `profiles.parquet` | `x,y,z,vs`, plus full-chain `vsv_mean`, `vsv_std`, `vsv_median`, `vsv_q16`, `vsv_q84`, `vsv_q025`, `vsv_q975` (and other enabled components) |
| `interfaces.parquet` | Original `x,y,misfit,moho`; updated `lab`; separately named posterior Moho/LAB/width/drop/thickness statistics and detection fractions |
| `diagnostics.parquet` | One row per grid, with `ok`, `partial`, or `failed`, messages and the retained posterior path |
| `metadata.json` | Run ID, source definitions, units and all LAB settings |
| `posterior/<run_id>/<grid>/` | Compressed derived samples with multiplicities, ordered retained chain, parameter covariance and input snapshots |

The original `vs`, `moho`, and `misfit` values are intentionally preserved from
`mean_prob.lst`, `moho.lst`, and `Litmod_output.log`. They retain the limitations
of those Fortran summaries; they are **not** silently replaced by full-chain
estimates. Use `vsv_mean` and `moho_km_mean` for the new estimates. `misfit` is
not a log posterior or MAP score. All depths (`z`, `moho`, `lab` and the
explicitly named `*_km*` depth quantities) are positive downward in km. Their
reference datum is inherited from the input model; collection applies no datum
conversion. Unavailable LAB is NaN, never -999. Schema version 3 changes the
sign of `z` and `lab` relative to earlier collection outputs; regenerate old
results before combining them with new ones.

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

### LAB calculation

The reported LAB is the center of a resolved mantle velocity-decrease band.
The calculation follows `identify_lab` and `summarize_lab` in
[`lab.py`](https://github.com/Sonder9927/seispy/blob/main/src/seispy/mcmc/lab.py),
with model reconstruction in `posterior.py`. All depths are positive downward
in km. The original `vs` field still reads the second column of
`mean_prob.lst`; that mean profile is not used to calculate LAB.

#### Posterior samples and velocity reconstruction

1. Read every retained state in `fort.8010`, including the repeated states
   produced by rejected proposals. Do not apply burn-in a second time: the
   file already starts at the configured `burn_in` iteration.
2. Reconstruct each distinct parameter state using its own Moho depth,
   B-spline coefficients and the original Fortran layer geometry. Reuse
   identical states while retaining their iteration multiplicities as weights.
   The last `out_best` accepted models are not the statistical population.
3. Select Vsv when available, otherwise Vsh, as recorded in `lab_component`.
   Linearly interpolate its mantle nodes to the evaluation grid. With positive
   downward depth, define descent as `D(z) = -d(V_smooth)/dz`; positive D
   denotes decreasing velocity.
4. Identify a valid band and its gradient-peak depth for each state, then
   summarize the resulting depths with their iteration multiplicities.
   This computes a derived quantity for each posterior state before aggregation,
   following the uncertainty-propagation principle described by
   [Hazzard et al. (2023)](https://doi.org/10.1029/2023JB026653).
   A LAB extracted from the average velocity profile
   generally differs from the aggregate of individual LAB estimates.

#### Default identification rules

| Quantity or decision | Default |
| --- | --- |
| Evaluation grid | 1 km spacing |
| Velocity smoothing | Gaussian sigma 5 km, nearest-value endpoint extension |
| Search interval for the center | max(50 km, sample Moho + 15 km) to min(250 km, available evaluation depth) |
| Numerical peak floor | Gradient height and prominence at least 1e-8 (km/s)/km |
| Minimum relative prominence | 20% of the descent peak height |
| Band shoulders | Expand while descent exceeds 10% of the peak height |
| Minimum observed band width | 10 km |
| Minimum velocity drop across the observed band | 0.05 km/s |
| Competing candidates | A second interior band with at least 80% of the strongest band's drop makes the state ambiguous |
| Minimum support for the final estimate | Valid centers in at least 50% of all retained iteration mass |

The numerical thresholds and the gradient-band definition are SeisPy analysis
choices; the cited studies motivate the statistical treatment, but do not
prescribe this detection algorithm or calibrate these thresholds.
The literature motivates posterior propagation, distribution summaries and an
explicit LAB definition. It does not validate these defaults for every study
region. A 1-km evaluation grid is a numerical sampling interval, not a claim
of 1-km physical resolution.

The user-facing `lab` is one depth per grid (positive downward, km). The
collector uses the median of successfully identified per-state centers, only
when at least 50% of the retained iteration mass supports a detection and the
conditional depth distribution has no clearly separated comparable modes.
Otherwise `lab` is NaN; `lab_status` explains low support or multiple modes.
The 50% rule is an operational reporting threshold, not a probability that a
physical LAB exists. No per-point tuning is required for normal use.

#### Per-state candidate selection and boundaries

The LAB method reconstructs each unique state with the Fortran layer geometry,
interpolates mantle velocity nodes to a 1-km grid and smooths with a 5-km
Gaussian sigma. Search centers lie between max(50 km, Moho + 15 km) and
250 km. Peaks with prominence below 20% of their height are discarded as
ripples; shoulders are where descent falls below 10% of the peak. Peaks inside
the same descent band are merged. Require a band at least 10 km wide and a
velocity drop of at least 0.05 km/s measured across that band. The strongest
drop selects a candidate unless a distinct interior band has at least 80%
of its drop, in which case the sample remains ambiguous.

Centers within 5 km of the search boundary or three smoothing sigmas of the
actual profile endpoints are censored. A band with both shoulders censored at
the profile ends has no resolved center. A single censored shoulder or a width
crossing an artificial search limit does not invalidate an interior center;
`lab_width_limited` marks this in the samples, and its fraction is reported.
Widths and drops of censored bands describe only the observed span. A boundary
candidate does not veto an independent valid interior candidate.

#### Final estimate and uncertainty

The ensemble mode check uses 5-km bins smoothed with sigma 7.5 km. Comparable
peaks must reach half the maximum height, be at least 20 km apart, and have an
intervening valley below half the smaller peak. This is a conservative signal
check, not a formal multimodality test. All defaults are operational choices;
optional `mcmc.LABSettings(...)` adjusts the physical search and filtering scales.
LAB denotes the center of a seismic velocity decrease, **not** the velocity
minimum or a thermal isotherm.

For valid states with depths `L_i` and iteration multiplicities `w_i`, the
reporting rule is:

```text
support = sum(w_i for valid states) / sum(w_i for all states)
lab = median(L_i repeated w_i times), if support >= 0.5 and the mode check passes
lab = NaN, otherwise
```

The output retains `lab_status` (`resolved`, `low_support`, `multimodal`, or
`posterior_unavailable`), `lab_detected_fraction` and the other diagnostic
fractions. `lab_estimate_km` equals the reported `lab` when resolved. Conditional
quantiles are obtained from the same multiplicity-weighted empirical samples;
`lab_depth_km_q16`–`lab_depth_km_q84` and
`lab_depth_km_q025`–`lab_depth_km_q975` span the central 68% and 95%, respectively.
They are not automatically equal to mean plus/minus one or two standard
deviations. Posterior summaries and parameter trade-offs should be interpreted
together, as discussed by
[Xing and Beghein (2015)](https://doi.org/10.1093/gji/ggv401).

`lab_depth_km_*` intervals remain conditional on successful per-state detection,
even when the single user-facing `lab` is withheld. Diagnostics retain detection,
ambiguity and boundary fractions; candidate lists and censored band bounds
remain in `samples.parquet`. Retained repeated states keep their iteration
multiplicities; no likelihood reweighting is applied. Pointwise velocity
intervals are not simultaneous whole-profile bands. Normal Fortran chain
records may wrap over several text lines; every numeric token is parsed and
the exact total state count checked against the run inputs.

This seismic gradient-center definition must be distinguished from a thermal
LAB: [Salajegheh et al. (2025)](https://doi.org/10.1029/2024GC011724)
define the thermal lithosphere
base using the 1250 degrees C isotherm. SeisPy's collector does not infer that
isotherm or convert velocity to temperature. A low-velocity-zone minimum is
also a different quantity from the maximum descent location.

#### Sampling diagnostics and result storage

A single-chain autocorrelation ESS estimate is reported when estimable; short
or constant chains have unavailable ESS, and R-hat is unavailable without
independent chains. These diagnostics do not certify convergence. No automatic
thermal conversions or posterior predictive calculations are performed.

Each collection gets a separate run directory for sample products and copied
probability figures, preventing stale samples from failed grids being mistaken
for the current run. Top-level tables refer to the latest collection. Collection
prints nothing; display, filter or log `summary.diagnostics` in the caller.

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
