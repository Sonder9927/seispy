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

After the external inversions finish, collect all grid results in one run.
The example lists the available LAB controls explicitly, using the current
defaults as a starting point:

```python
import pandas as pd
from seispy import mcmc

lab_settings = mcmc.LABSettings(
    min_depth_km=50.0,            # Lower search bound for the LAB center (km).
    max_depth_km=250.0,           # Upper search bound for the LAB center (km).
    moho_buffer_km=15.0,          # Minimum distance below each sample's Moho (km).
    step_km=1.0,                  # Evaluation spacing, not physical resolution (km).
    smoothing_km=3.0,             # Minimum Gaussian sigma; native spacing also applies.
    min_drop_km_s=0.01,           # Absolute floor for localized velocity drop (km/s).
    min_relative_drop=0.005,      # Relative contrast floor: 0.5% of local Vs.
    noise_multiplier=4.0,         # Multiplier for the roughness-based contrast floor.
    min_width_km=5.0,             # Minimum half-prominence width; native spacing also applies.
    boundary_buffer_km=5.0,       # Minimum search-boundary margin; other scales also apply.
    min_detection_fraction=0.5,   # Required fraction of retained iteration mass.
)

summary = mcmc.collect_results(
    grids_dir="output/grids",
    out_dir="output/summary",
    lab_settings=lab_settings,
)

# Collection is silent; display or log the results centrally after it finishes.
print(f"Total grids: {summary.grid_count}")
print(f"Complete posterior results: {summary.posterior_count}")
print(f"Partial results: {summary.partial_count}")
print(f"Failed grids: {summary.failed_count}")
print(f"Valid LAB estimates: {summary.lab_valid_count}")
print(f"NaN LAB estimates: {summary.lab_nan_count}")
print(f"LAB status counts: {summary.lab_status_counts}")

profiles = pd.read_parquet(summary.output_paths["profiles"])
interfaces = pd.read_parquet(summary.output_paths["interfaces"])
diagnostics = summary.diagnostics

# Match lab_nan_count: failed grids have no exported interface row.
missing_lab = diagnostics.loc[
    diagnostics["lab"].isna() & diagnostics["status"].ne("failed"),
    ["grid", "lab_status"],
]
if not missing_lab.empty:
    print("\nGrids with NaN LAB estimates:")
    print(missing_lab.to_string(index=False))

failed = diagnostics.loc[diagnostics["status"].eq("failed"), ["grid", "message"]]
if not failed.empty:
    print("\nFailed grids and error messages:")
    print(failed.to_string(index=False))
```

`min_detection_fraction` accepts values in `(0, 1]` and defaults to `0.5`.
It weights retained chain iterations, including repeated states after rejected
proposals. Increasing it makes reporting more conservative; reducing it allows
estimates supported by fewer states and does not establish greater reliability.

`lab_nan_count` includes partial results with missing LAB. Completely failed
grids have no exported interface row and are counted separately by `failed_count`.
Thus `lab_valid_count + lab_nan_count == success_count`, and adding
`failed_count` gives the total grid count. These LAB counts are also saved under
`lab_summary` in `metadata.json`.

The velocity-drop threshold is the maximum of the absolute, relative and
roughness-based floors; lowering `min_drop_km_s` alone may have no effect.
`smoothing_km`, `min_width_km` and `boundary_buffer_km` are lower bounds:
the effective values also depend on native mantle node spacing. Roughness is
not observational error, and `noise_multiplier` is not a significance level.

If independent regional evidence supports a 60–150 km search interval, change
`min_depth_km` and `max_depth_km` accordingly. These bounds constrain the center;
band shoulders may extend beyond them. A narrower interval need not increase
detection: the `mcmc-v3` range comparison did not produce more resolved grids.
See [LAB calculation](#lab-calculation) below for the complete rules.

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

Let `h` be the median spacing of the original forward mantle nodes and `dz`
the evaluation spacing. The collector supplies `h` before interpolation;
it is a model sampling scale, not a measured resolving power.

| Quantity or decision | Default |
| --- | --- |
| Evaluation grid | 1 km spacing |
| Gaussian smoothing sigma | `max(3 km, h/2, dz)`, nearest-value endpoint extension |
| Confirmation smoothing | Twice the primary sigma |
| Search interval for the center | max(50 km, sample Moho + 15 km) to min(250 km, available evaluation depth) |
| Numerical peak floor | Gradient height and prominence at least 1e-8 (km/s)/km |
| Minimum relative prominence | 20% of the descent peak height |
| Background descent | Maximum of zero, the median profile descent and the local prominence baseline |
| Band shoulders | 10% of the peak excess above background |
| Minimum half-prominence width | `max(5 km, h, 2*dz)` |
| Minimum localized velocity drop | `max(0.01 km/s, 0.005*local_Vs, 4*roughness)` |
| Center stability | A corresponding band at twice sigma; center shift at most `max(sigma, h)` and drop still above threshold |
| Unlocalized descent | Reject a half-prominence width exceeding 80% of the available mantle profile span |
| Competing candidates | A second interior band with at least 80% of the strongest band's localized drop makes the state ambiguous |
| Minimum support for the final estimate | `min_detection_fraction`, default 0.5 of all retained iteration mass |

These thresholds are SeisPy operational choices. The cited studies motivate
posterior propagation and distribution summaries; they do not prescribe or
validate these detection defaults. The relative floor corresponds to a 0.5%
velocity contrast (about 0.0225 km/s at 4.5 km/s), rather than a fixed 0.05 km/s
for every model. This permits weak but coherent transitions without declaring
every negative gradient an interface.

The roughness proxy is `1.4826*MAD(second differences)/sqrt(6)`, evaluated after
resampling to spacing `max(h, dz)` so that interpolation does not create an
artificially small estimate. Here MAD is the median absolute deviation from
the median. This is a high-frequency signal proxy, **not** observational noise,
a posterior standard deviation or a statistical significance test. Posterior
uncertainty is still propagated through individual retained states.

The defaults were checked using known tanh-shaped transitions and flat or
linear backgrounds with noise at several native node spacings. These checks
bound behavior on those synthetic cases; they do not establish regional
physical accuracy or guarantee convergence. A 1-km evaluation grid does not
imply 1-km physical resolution.

#### Per-state candidate selection and boundaries

Each unique state is reconstructed with the Fortran layer geometry. Its mantle
velocity nodes are linearly interpolated, then smoothed at the resolution-aware
scale above. Positive descent peaks are grouped into bands above their local
and regional gradient background. `lab_width_km` measures the observed span
between the 10% shoulders; `lab_fwhm_km` measures half-prominence width above
the effective background and supplies the minimum-width check.

`lab_drop_km_s` is the localized drop after subtracting background descent times
band width. `lab_total_drop_km_s` retains the total smoothed velocity difference
between shoulders. The localized drop must exceed the adaptive floor and
remain above it at doubled smoothing. The center must also remain in the same
band within `max(sigma, h)`. The largest qualifying localized drop selects the
candidate, unless a competing band has at least 80% of its drop.

Centers must lie strictly farther than `max(5 km, sigma, h)` from the search
boundaries and three primary sigmas from the actual profile endpoints. A band
with both shoulders at the profile ends has no resolved center. A single
censored shoulder or a band crossing a search boundary can retain its interior
center; `lab_width_limited` marks the truncated span. `lab_width_resolved` is
true only when the observed half-prominence width spans at least two native
node intervals and three smoothing sigmas. It is an operational width-quality
flag; it does not turn the measured smoothed width into an intrinsic physical
transition thickness. A thin interface may have a usable center and an
unresolved width.

Rejected states retain `lab_candidate_depth_km`, `lab_candidate_drop_km_s`,
`lab_drop_threshold_km_s`, `lab_smoothing_km` and `lab_rejection_reason` when
available. Reasons distinguish `weak_drop`, `too_narrow`, `no_descent_peak`,
`unlocalized_descent`, `boundary_center`, `scale_unstable`, `competing_bands`
and `insufficient_coverage`. A search endpoint with the largest gradient alone
does not establish a boundary-limited candidate.

The user-facing `lab` remains a multiplicity-weighted median, conditional on
successful detection. At least `min_detection_fraction` (default 50%) of the
retained iteration mass must support a valid center, and the conditional depth distribution must pass the mode
check below; otherwise `lab` is NaN. The support rule is an operational
reporting threshold, not a probability that a physical LAB exists.

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
lab = median(L_i repeated w_i times), if support >= min_detection_fraction and the mode check passes
lab = NaN, otherwise
```

The output retains `lab_status` (`resolved`, `low_support`, `multimodal`, or
`posterior_unavailable`), `lab_detected_fraction` and the other diagnostic
fractions. `lab_estimate_km` equals the reported `lab` when resolved. Conditional
quantiles are obtained from the same multiplicity-weighted empirical samples;
`lab_depth_km_q16`–`lab_depth_km_q84` and
`lab_depth_km_q025`–`lab_depth_km_q975` span the central 68% and 95%, respectively.
`lab_detected_states` and `lab_detected_unique_states` report the support behind
these intervals. `lab_interval_status` is `conditional` only for a resolved
summary; otherwise it is `insufficient_support` or `multimodal`. Numerical
conditional quantiles may still be stored when the main estimate is withheld,
but a narrow interval based on a handful of detected states is not strong
unconditional evidence.
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
independent chains. `lab_chain_quality` separately flags `low_parameter_ess`
when the minimum estimable parameter ESS is below 100, `ess_unavailable` when
none is estimable, or `single_chain_not_certified` otherwise. The ESS threshold
is a screening choice: it does not certify convergence or automatically remove
a resolved LAB. `lab_main_rejection_reason` reports the most common failed-state
reason by retained iteration mass. `lab_width_unresolved_fraction` is conditional
on successful detection. These diagnostics do not certify convergence. No automatic
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
