# MCMC configuration and inputs

[Run the workflow](../recipes/mcmc.md) · [API reference](../api/mcmc.md)

## Complete config.json example

The example below lists **every** supported configuration key explicitly.
Replace the example `paths` with your own files; the remaining values are a
working starting template. Unknown keys are rejected, so keep the spelling exact.

```json
{
  "region": [100, 101, 50, 51],
  "grid_spacing": 0.5,
  "paths": {
    "topography_file": "data/topography.nc",
    "sediment_file": "data/sediment.xyz",
    "moho_file": "data/moho.csv",
    "vs_model_file": "data/reference_vs.parquet",
    "phase_dispersion_file": "data/phase_dispersion.csv",
    "output_dir": "output/grids"
  },
  "input_units": {
    "topography": "m",
    "sediment": "km",
    "moho": "km",
    "vs": "km/s",
    "phase_velocity": "km/s",
    "phase_std": "m/s"
  },
  "search_radius": {
    "sediment": 2.0,
    "moho": 5.0,
    "crust_vs": 0.3,
    "mantle_vs": 0.2
  },
  "vs_constraints": {
    "global_vs_max": 4.9,
    "no_shallow_layers_vs_min": 0.5,
    "deepest_vs_min": 4.0,
    "moho_strict_margin": 0.001,
    "allow_shallow_extrapolation": true,
    "max_shallow_extrapolation_km": 5.0
  },
  "water_threshold": 1.0,
  "sediment_threshold": 2.0,
  "sediment_vs": [
    [0.5, 2.5],
    [1, 3.0]
  ],
  "n_coeff_crust": 4,
  "n_coeff_mantle": 5,
  "sm_on": 0,
  "ice_on": 0,
  "factor": 2.0,
  "zmax_Bs": 300,
  "NPTS_cBs": 21,
  "NPTS_mBs": 41,
  "reference_model": "prem_noocean.txt",
  "reference_water_model": "prem_ocean.txt",
  "default_phase_std": 0.03,
  "phase_constraints": {
    "minimum_periods": 5,
    "skip_if_insufficient": true
  },
  "mcmc_params": {
    "mineos_on": 0,
    "nsimu": 200000,
    "inm": 1,
    "nc": 1,
    "adaptint": 10000,
    "imat_fac": 5,
    "verbo": 0,
    "dodr": 1,
    "sigma2": 1.0,
    "DRscale": 2.0,
    "iresetad": 40000000,
    "id_run": 2,
    "biasfac": 1.7,
    "burn_in": 100000,
    "out_best": 3000
  }
}
```

Generate inputs with:

```python
from seispy import mcmc

mcmc.init_grids("config.json")
```

Relative paths in `paths` are resolved against the directory containing
`config.json`. In contrast, `reference_model` and `reference_water_model` are
written verbatim into `para.inp`: make these Fortran reference files accessible
from each inversion's working directory, or supply absolute paths. They are
separate from `paths.vs_model_file`, which supplies the reference profiles
projected into the Fortran coefficient space.
Input generation does not launch the external Fortran inversion.

### Top-level settings

| Key                               | Unit      | Meaning                                                                                                                                                                                                           |
| --------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `region`                          | degrees   | `[west, east, south, north]`; both endpoints are included. Latitude must stay within `[-90, 90]`.                                                                                                                 |
| `grid_spacing`                    | degrees   | Horizontal grid interval, not a distance in km. Both regional extents must be divisible by it. The example region spans 1° x 1° at 0.5°, i.e. a 3 x 3 set of inversion points.                                    |
| `paths`                           | -         | Input and output file locations; see the next table.                                                                                                                                                              |
| `input_units`                     | -         | Declared units of the input products; see the table after that.                                                                                                                                                   |
| `search_radius`                   | km / km/s | Prior half-widths; see "Search widths and Vs constraints".                                                                                                                                                        |
| `vs_constraints`                  | km/s      | Hard Vs limits and extrapolation policy; see the same section.                                                                                                                                                    |
| `water_threshold`                 | km        | Water becomes active where `max(0, -elevation)` is strictly greater than this value, unless sediment is active.                                                                                                   |
| `sediment_threshold`              | km        | Sediment becomes active where thickness is strictly greater than this value; sediment has priority over water.                                                                                                    |
| `sediment_vs`                     | km/s      | Explicit `[lower, upper]` search intervals for the sediment parameters (one pair per parameter). Any number of intervals is accepted; their search centres must strictly increase, and the intervals may overlap. |
| `n_coeff_crust`, `n_coeff_mantle` | -         | Number of adjustable Vs coefficients per section, at least 3.                                                                                                                                                     |
| `sm_on`, `ice_on`                 | 0/1       | Fortran smoothing-weight and ice-layer switches. Keep both 0 unless your executable accepts the extra records.                                                                                                    |
| `factor`                          | -         | Dimensionless B-spline knot-spacing control.                                                                                                                                                                      |
| `zmax_Bs`                         | km        | Bottom of the inversion interval; reference profiles must cover this depth.                                                                                                                                       |
| `NPTS_cBs`, `NPTS_mBs`            | -         | Number of depth samples used to evaluate each spline in the forward model, not extra free parameters.                                                                                                             |
| `reference_model`                 | path/name | Reference model written to `para.inp` when no water layer is active.                                                                                                                                              |
| `reference_water_model`           | path/name | Reference model written to `para.inp` when the water layer is active.                                                                                                                                             |
| `default_phase_std`               | km/s      | Replacement uncertainty for missing or invalid phase-dispersion `std`; 0.03 means 30 m/s.                                                                                                                         |
| `phase_constraints`               | -         | Quality-control thresholds for writing `phase.input`.                                                                                                                                                             |
| `mcmc_params`                     | -         | Fortran run settings written to `input_DRAM_T.dat`; see the table below.                                                                                                                                          |

### Paths

| Key                           | Meaning                                                                                                                  |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `paths.topography_file`       | Elevation/bathymetry grid (CSV, Parquet, NetCDF or XYZ).                                                                 |
| `paths.sediment_file`         | Sediment-thickness grid.                                                                                                 |
| `paths.moho_file`             | Moho-depth grid.                                                                                                         |
| `paths.vs_model_file`         | Reference Vs model used to build the coefficient-space search centres.                                                   |
| `paths.phase_dispersion_file` | Phase-dispersion product: a regular lon/lat grid with one row per lon/lat/period, or a NetCDF `(period, lat, lon)` cube. |
| `paths.output_dir`            | Directory that receives one sub-directory per inversion point.                                                           |

### Input units

| Key                          | Allowed values | Internal target |
| ---------------------------- | -------------- | --------------- |
| `input_units.topography`     | `m`, `km`      | metres          |
| `input_units.sediment`       | `m`, `km`      | kilometres      |
| `input_units.moho`           | `m`, `km`      | kilometres      |
| `input_units.vs`             | `m/s`, `km/s`  | km/s            |
| `input_units.phase_velocity` | `m/s`, `km/s`  | km/s            |
| `input_units.phase_std`      | `m/s`, `km/s`  | km/s            |

`input_units.phase_std` is declared separately from
`input_units.phase_velocity` because a single file may store `phv` in km/s
while its `std` column is in m/s.

### Physical quantities and units

| Quantity          | Unit                         | Meaning and convention                                                                                                                                                      |
| ----------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Topography        | `input_units.topography`     | Elevation, positive above sea level and negative below. Water depth is `max(0, -elevation)` after unit conversion.                                                          |
| Sediment          | `input_units.sediment`       | Nonnegative sediment thickness, not an absolute elevation.                                                                                                                  |
| Moho              | `input_units.moho`           | Positive-downward Moho depth. It must exceed the shallow interface and remain shallower than `zmax_Bs`.                                                                     |
| Reference Vs      | `input_units.vs`             | Shear-wave speed used to construct coefficient search centers. Its depth coordinates must already be in **km**, positive downward; `input_units.vs` converts velocity only. |
| Dispersion period | s                            | Surface-wave period in the phase-dispersion input.                                                                                                                          |
| Phase velocity    | `input_units.phase_velocity` | Observed surface-wave phase speed; distinct from the local body-wave speed Vs.                                                                                              |

Use a consistent depth zero for the reference profile, Moho, and model bottom.
The preparation code does **not** transform Moho or profile depths between
sea-level and local-surface datums or correct them for positive topography. For
marine points, water depth is derived from sea-level-referenced bathymetry;
ensure the other depths use the matching datum before preparation.

Layer switches are determined separately at each grid point from its bathymetry
and sediment-thickness data, after conversion to km. The configuration provides
thresholds, not global water/sediment on/off switches. With the example values
(`water_threshold` 1 km, `sediment_threshold` 2 km):

| Water depth | Sediment thickness | Active shallow layer            |
| ----------- | ------------------ | ------------------------------- |
| <= 1 km     | <= 2 km            | Neither                         |
| > 1 km      | <= 2 km            | Water only                      |
| <= 1 km     | > 2 km             | Sediment only                   |
| > 1 km      | > 2 km             | Sediment only; water is ignored |

Equality does not enable a layer. Water and sediment remain mutually exclusive:
sediment has priority. The crustal spline starts at the sediment bottom when
sediment is active (without adding the ignored water depth), the water bottom
when only water is active, or zero when neither is active. A sediment-priority
point uses `reference_model`, not `reference_water_model`.

### Search widths and Vs constraints

Typical `search_radius` settings are **2 km for sediment thickness**, **5 km
for Moho depth**, **0.3 km/s for crustal Vs**, and **0.2 km/s for mantle Vs**.
All four values are search half-widths.

| Setting                                             | Unit | Interpretation                                                                                                                                                                                                                                                      |
| --------------------------------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `search_radius.sediment`                            | km   | Half-width around sediment thickness; the lower endpoint is clipped at zero.                                                                                                                                                                                        |
| `search_radius.moho`                                | km   | Half-width around Moho depth. Choose it so the searched interface stays below the shallow layers and above the model bottom.                                                                                                                                        |
| `search_radius.crust_vs`, `search_radius.mantle_vs` | km/s | Half-width around each least-squares-projected reference Vs coefficient, **not a percentage or standard deviation**. Defaults are 0.3 and 0.2. A scalar applies to every coefficient; a list must have one value per coefficient.                              |
| `sediment_vs`                                       | km/s | Explicit `[lower, upper]` search intervals for the sediment parameters, not half-widths. The intervals may overlap; only their search centres must increase strictly. The example `[[0.2, 2.5], [0.5, 3.0]]` caps sediment Vs at 3 km/s with centres 1.35 and 1.75. |
| `vs_constraints.global_vs_max`                      | km/s | Fortran upper limit on the **reconstructed node velocity**, 4.9, covering crust, mantle and sediment; equality is allowed. Applied to the model, not to each coefficient.                                                                                          |
| `vs_constraints.no_shallow_layers_vs_min`           | km/s | Lower bound 0.5 on the **reconstructed node velocity** for crust and mantle, applied only when water, sediment and ice are all disabled. It cannot be set below 0.5.                                                                                              |
| `vs_constraints.deepest_vs_min`                     | km/s | Lower bound 4.0 for the deepest node, which is exactly the deepest mantle coefficient; it cannot be set below 4.0.                                                                                                                                                 |
| `vs_constraints.moho_strict_margin`                 | km/s | Numerical separation of the first mantle and last crust **initial midpoints**. It does not force disjoint search intervals or prescribe the geological Moho contrast.                                                                                               |
| `vs_constraints.moho_vs_jump`                       | km/s | **Deprecated and ignored.** The projection gives the interface pair a positive layer-mean contrast, so a nonzero value only raises a `DeprecationWarning`.                                                                                                        |
| `vs_constraints.allow_shallow_extrapolation`        | bool | Whether a finite shallow gap in the reference profile is filled by linear extrapolation from the two shallowest samples.                                                                                                                                            |
| `vs_constraints.max_shallow_extrapolation_km`       | km   | Maximum shallow gap filled when extrapolation is enabled; `null` removes the gap limit.                                                                                                                                                                             |

Bounds intersect the requested interval with the applicable limits; only a
wholly out-of-domain interval triggers fallback translation. Consequently,
Fortran's initial midpoint can differ from the projection centre. For example,
crust `4.1 +/- 0.3` becomes `[3.8, 4.3]`, while mantle
`4.95 +/- 0.2` becomes `[4.75, 4.9]`. Inspect
`prior_bounds.csv` for the projection centres, final bounds, and
initialization midpoints. The
[prior-bound rules](parameterization.md#fortran-compatible-vs-prior-bounds) below explain Moho
repair and output precision.

### Spline resolution and phase constraints

| Setting                                  | Meaning                                                                                                                                                                          |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `n_coeff_crust`, `n_coeff_mantle`        | Number of adjustable Vs coefficients, at least 3 each. This Fortran parameterization uses polynomial degree `K - 2`; changing the count changes both flexibility and degree.     |
| `NPTS_cBs`, `NPTS_mBs`                   | Number of depth samples for evaluating each spline in the forward model, not additional free parameters. Use at least 2 because the Fortran sampling step divides by `NPTS - 1`. |
| `factor`                                 | Dimensionless knot-spacing control. For interval `[a, b]`, the interior knot is at `a + (b-a)/(1+factor)`; `2` places it one-third of the way down.                              |
| `default_phase_std`                      | Replacement uncertainty in **km/s** for missing or invalid positive dispersion standard deviations; 0.03 means 30 m/s.                                                           |
| `phase_constraints.minimum_periods`      | Minimum number of valid dispersion rows for writing a point when `skip_if_insufficient` is true.                                                                                 |
| `phase_constraints.skip_if_insufficient` | When true, a grid point with too few valid periods is skipped instead of aborting the run.                                                                                       |

Keep `sm_on` and `ice_on` at zero for this example. The supplied
Fortran reader requires extra smoothing-weight or ice-layer records when these
switches are one; the current writer does not emit those additional records.

### Fortran run settings (`mcmc_params`)

These values are written to `input_DRAM_T.dat` in the exact order below.
Confirm the meaning of each field against your Fortran executable; the
descriptions here are the conventional DRAM interpretation.

| Key         | Meaning                                                                            |
| ----------- | ---------------------------------------------------------------------------------- |
| `mineos_on` | 0/1 switch that selects the reference-Earth (Mineos) branch of the forward solver. |
| `nsimu`     | Total number of MCMC iterations.                                                   |
| `inm`       | Inversion/parameterization selector read by the executable.                        |
| `nc`        | Number of Markov chains.                                                           |
| `adaptint`  | Interval (in iterations) between proposal-covariance adaptations.                  |
| `imat_fac`  | Scaling factor applied to the proposal covariance matrix.                          |
| `verbo`     | Verbosity level of the executable.                                                 |
| `dodr`      | 0/1 switch enabling delayed rejection.                                             |
| `sigma2`    | Initial data-error variance used by the likelihood.                                |
| `DRscale`   | Delayed-rejection scaling factor.                                                  |
| `iresetad`  | Iteration at which the adaptation state is reset.                                  |
| `id_run`    | Run identifier, typically used in output naming.                                   |
| `biasfac`   | Bias/outlier weighting factor.                                                     |
| `burn_in`   | Number of initial iterations discarded as burn-in; must be smaller than `nsimu`.   |
| `out_best`  | Number of best-fitting models written to the output.                               |

These settings are separate from the physical Vs prior and from the
observational uncertainty (`default_phase_std` and the input `std` column).

## Input formats and target grid

The MCMC configuration defines one target grid through `region` and
`grid_spacing`. Both regional extents must be divisible by `grid_spacing`; the
target coordinates are then constructed as

```python
lon = np.linspace(xmin, xmax, nx)
lat = np.linspace(ymin, ymax, ny)
```

where `nx = (xmax - xmin) / grid_spacing + 1` and
`ny = (ymax - ymin) / grid_spacing + 1`. This grid is the authoritative set
of inversion points. Topography, sediment thickness and Moho depth must each
form a **complete regular longitude/latitude grid**: at least two nodes on each
axis, uniform spacing along each axis, one finite value at every coordinate
pair, and no duplicate nodes. The two axis spacings may differ, and different
fields may have different spacings. Table rows may be unordered and NetCDF axes
may be ascending or descending.

All three fields use the shared `interpolate_regular_grid(xyz, target)`
function, which aligns the source to the authoritative `TargetGrid` with one of
three strategies, with no GMT dependency:

- **equal spacing and aligned origin** — the matching nodes are copied directly;
- **coarser source** — linear interpolation at the target nodes;
- **finer source** — a conservative area average over each target cell.

Partially covered cells are averaged over the available overlap. Incomplete
grids, nonuniform axes, missing values and targets outside the source extent are
rejected rather than extrapolated. NetCDF reads retain the neighboring nodes
outside the target region so that boundary processing remains supported.
Longitude conventions must match; automatic dateline wrapping and polar
extrapolation are not provided.

For example, ETOPO at 1 arc-minute spacing and sediment thickness at 1-degree
spacing can both be aligned to a 0.5-degree inversion grid. Upsampling a coarse
sediment model does not turn it into independent 0.5-degree observations, and
downsampling a fine topography grid now yields an area average rather than a
point sample. Layer thresholds are applied **after** alignment and unit
conversion, with sediment taking priority.

```python
from seispy.mcmc.gridding import TargetGrid
from seispy.mcmc.spatial import interpolate_regular_grid

target = TargetGrid.from_region([100, 102, 30, 32], spacing=0.5)
# xyz is an N × 3 array: longitude, latitude, scalar value.
field = interpolate_regular_grid(xyz, target)
# field has dimensions (y, x); value units are unchanged.
```

The scalar spatial fields (for example sediment thickness, water depth, and
Moho depth), the reference Vs model, and phase-dispersion data may be supplied
as CSV, NetCDF, or Parquet. Spatial scalar fields also accept whitespace-separated
XYZ files with longitude, latitude and value as their first three columns. Coordinate aliases are normalized consistently:

```text
longitude: x, lon, longitude
latitude:  y, lat, latitude
```

Input units are explicit in the optional `input_units` block. The defaults are
topography in metres, sediment and Moho depth in kilometres, and Vs/phase
velocity in kilometres per second. The internal MCMC conventions are applied
before interpolation and prior construction.

```json
"input_units": {
  "topography": "m",
  "sediment": "km",
  "moho": "km",
  "vs": "km/s",
  "phase_velocity": "km/s",
  "phase_std": "m/s"
}
```

All five products are aligned with the same three-case strategy. Phase
dispersion is pivoted to a `(period, y, x)` cube and the reference Vs model to
a `(z, y, x)` cube before alignment; cells or profiles without source coverage
stay NaN and are reported by the Vs preflight. `input_units.phase_std` is
declared separately from `input_units.phase_velocity` because one file may
store `phv` in km/s while its `std` column is in m/s.

Per-grid output directories retain the existing `{lon:.2f}_{lat:.2f}` naming
convention. No coordinate metadata sidecar files are generated; preparation
performs a preflight collision check before writing outputs.

