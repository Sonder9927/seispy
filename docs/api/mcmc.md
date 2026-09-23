# MCMC interface

`seispy.mcmc` turns a JSON configuration and five regular lon/lat products
(topography, sediment thickness, Moho depth, reference Vs, and phase
dispersion) into one self-contained inversion directory per grid point. Grid
preparation never launches the external Fortran inversion:

1. **Target grid** - `region` and `grid_spacing` define the authoritative nodes.
2. **Spatial fields** - topography, sediment and Moho are aligned to that grid.
3. **Phase dispersion** - the long table or NetCDF cube is pivoted and aligned.
4. **Reference Vs** - profiles are aligned, and every target node must have one.
5. **Preflight** - every inversion point is built and its prior bounds are
   validated before any output directory is created.
6. **Write** - each valid point receives `phase.input`, `para.inp`,
   `input_DRAM_T.dat`, and `prior_bounds.csv` (plus `point.png` when `plot=True`).

All five products share one three-case alignment (copy / interpolate /
aggregate); no external gridding tool is required. A point with too few valid
dispersion rows is skipped before its directory is created.

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
    "crust_vs_max": 4.1,
    "no_shallow_layers_vs_min": 0.5,
    "deepest_vs_min": 4.0,
    "moho_strict_margin": 0.001,
    "moho_vs_jump": 0.3,
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
separate from `paths.vs_model_file`, which supplies the Greville search centers.
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
| `paths.vs_model_file`         | Reference Vs model used to build the Greville search centres.                                                            |
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

| Setting                                             | Unit | Interpretation                                                                                                                                                                                                                                                      |
| --------------------------------------------------- | ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `search_radius.sediment`                            | km   | Half-width around sediment thickness; the lower endpoint is clipped at zero.                                                                                                                                                                                        |
| `search_radius.moho`                                | km   | Half-width around Moho depth. Choose it so the searched interface stays below the shallow layers and above the model bottom.                                                                                                                                        |
| `search_radius.crust_vs`, `search_radius.mantle_vs` | km/s | Half-width around each Greville-interpolated reference Vs, **not a percentage or standard deviation**. Defaults are 0.3 and 0.2. A scalar applies to every coefficient; a list must have one value per coefficient.                                                 |
| `sediment_vs`                                       | km/s | Explicit `[lower, upper]` search intervals for the sediment parameters, not half-widths. The intervals may overlap; only their search centres must increase strictly. The example `[[0.2, 2.5], [0.5, 3.0]]` caps sediment Vs at 3 km/s with centres 1.35 and 1.75. |
| `vs_constraints.global_vs_max`                      | km/s | Fortran upper limit, 4.9, covering crust, mantle and sediment; equality is allowed.                                                                                                                                                                                 |
| `vs_constraints.crust_vs_max`                       | km/s | Configurable crustal prior cap, 4.3 here; an empirical choice rather than a universal physical bound.                                                                                                                                                               |
| `vs_constraints.no_shallow_layers_vs_min`           | km/s | Lower bound 0.5 for the crust and mantle, applied only when water, sediment and ice are all disabled. It cannot be set below 0.5.                                                                                                                                   |
| `vs_constraints.deepest_vs_min`                     | km/s | Lower bound 4.0 for the deepest mantle coefficient only, not the whole mantle; it cannot be set below 4.0.                                                                                                                                                          |
| `vs_constraints.moho_strict_margin`                 | km/s | Numerical separation of the first mantle and last crust **initial midpoints**. It does not force disjoint search intervals or prescribe the geological Moho contrast.                                                                                               |
| `vs_constraints.moho_vs_jump`                       | km/s | Expected physical Vs contrast across the Moho, split symmetrically between the last crustal and first mantle coefficient. `0` keeps both centred on the continuous reference value; the example grid uses `0.3`. See [the Moho contrast prior](#the-moho-contrast-prior). |
| `vs_constraints.allow_shallow_extrapolation`        | bool | Whether a finite shallow gap in the reference profile is filled by linear extrapolation from the two shallowest samples.                                                                                                                                            |
| `vs_constraints.max_shallow_extrapolation_km`       | km   | Maximum shallow gap filled when extrapolation is enabled; `null` removes the gap limit.                                                                                                                                                                             |

Bounds intersect the requested interval with the applicable limits; only a
wholly out-of-domain interval triggers fallback translation. Consequently,
Fortran's initial midpoint can differ from the reference center. For example,
crust `4.1 +/- 0.3` becomes `[3.8, 4.3]`, while mantle
`4.95 +/- 0.2` becomes `[4.75, 4.9]`. Inspect
`prior_bounds.csv` for the original centers, final bounds, and
initialization midpoints. The
[prior-bound rules](#fortran-compatible-vs-prior-bounds) below explain Moho
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

## Module layout

The implementation is organized around explicit seams rather than one monolithic
preparation script. Each module has one reason to change, and dependencies only
point inwards: `config` and `gridding` are leaves, the domain modules
(`spatial`, `dispersion`, `velocity`, `bspline`, `inversion`, `priors`) build on
them, and `serialization`, `plotting`, `workflow` and `collection` sit at the
edges.

| Module             | Responsibility                                                                        |
| ------------------ | ------------------------------------------------------------------------------------- |
| `config.py`        | Validate the JSON configuration; normalize relative paths and units.                   |
| `inputs.py`        | Resolve coordinate/value aliases and read CSV, NetCDF and Parquet tables.              |
| `gridding.py`      | Define the authoritative `TargetGrid` and align source grids to it (copy/interpolate/aggregate). |
| `spatial.py`       | Read and normalize topography, sediment and Moho fields onto the target grid.          |
| `dispersion.py`    | Read phase dispersion and align it to a `(period, y, x)` cube.                         |
| `velocity.py`      | Load reference Vs profiles and interpolate them at requested depths.                   |
| `bspline.py`       | Reproduce the Fortran knot vector and Greville depths.                                 |
| `inversion.py`     | The `InversionPoint` model and its layer/threshold logic.                              |
| `priors.py`        | Pure computation of the final per-coefficient Vs prior bounds.                         |
| `serialization.py` | Write `phase.input`, `para.inp`, `prior_bounds.csv` and `input_DRAM_T.dat`.            |
| `plotting.py`      | Diagnostic dispersion and Vs-model figures (needs the `plot` extra).                   |
| `workflow.py`      | Preflight every point, then write each point serially.                                 |
| `collection.py`    | Collect completed inversions into summary tables and figures.                          |

The package-level API is intentionally small:
`mcmc.init_grids(config_path)` and
`mcmc.collect_results(grids_dir, out_dir)`. Internal modules are
free to evolve without coupling callers to file-format details.

## Per-point figures

Three plotting helpers make a single inversion point inspectable. Install the
optional extra first with `pip install "seispy[plot]"`.

```python
from seispy.mcmc.plotting import plot_dispersion, plot_model, plot_point
from seispy.mcmc.priors import PriorSettings, compute_point_bounds

bounds = compute_point_bounds(point, PriorSettings.from_config(cfg))
plot_dispersion(curve, default_sigma=cfg.default_phase_std)  # period vs phase velocity
plot_model(point, bounds)                                    # depth vs Vs search intervals
figure, axes = plot_point(point, curve, bounds, output_file="point.png")
```

![Example per-point figure: dispersion on the left, Vs search intervals on
the right](../assets/mcmc-point-example.png)

*Real-data example generated with `plot=True` at grid point
`122.00_33.50` (Moho 32.49 km, `moho_vs_jump = 0.3`). Left: phase dispersion
with one-sigma bars. Right: Greville Vs coefficients with their final search
intervals, the reference profile (grey), the sediment layer, the Moho, and the
model bottom. The last crustal marker (blue) and first mantle marker (orange)
sit on either side of the Moho line, as described under
[the Moho contrast prior](#the-moho-contrast-prior).*

- `plot_dispersion(curve, ...)` draws phase velocity against period with
  one-sigma error bars; non-finite sigmas fall back to `default_sigma`
  when it is given, otherwise those samples are drawn without error bars.
- `plot_model(point, bounds, ...)` draws one marker per Greville coefficient
  with a horizontal error bar spanning the final clipped `[lower, upper]`
  interval, overlays the reference profile, and marks the water or sediment
  interface (if any), the Moho, and the model bottom. Sediment intervals are
  placed across the sediment layer and joined to show the linear trend.
- `compute_point_bounds(point, settings)` is the pure bound
  computation shared by the writer and the plots, so a figure always matches
  `para.inp`.
- `mcmc.init_grids(config_path, plot=True)` writes the combined figure
  as `point.png` in each point directory. Skipped points are neither
  written nor plotted and leave no directory behind.

## B-spline parameterization

The MCMC input uses separate velocity parameterizations for the sediment,
crust, and mantle layers. If the sediment thickness is
`z_sed`, the Moho depth is `z_moho`, and the maximum inversion depth is
`z_max`, the intervals are:

| Layer    | Depth interval   | MCMC representation                    |
| -------- | ---------------- | -------------------------------------- |
| Sediment | `0`–`z_sed`      | Separate sediment parameters           |
| Crust    | `z_sed`–`z_moho` | `n_coeff_crust` B-spline coefficients  |
| Mantle   | `z_moho`–`z_max` | `n_coeff_mantle` B-spline coefficients |

For one layer, let `[a, b]` be its depth interval and let `K` be its number
of B-spline coefficients. The original Fortran parameterization uses

```text
degBs = K - 1
p     = degBs - 1 = K - 2       # actual polynomial degree
L     = K + degBs = 2K - 1      # knot-vector length
```

Here `p` is the degree of the recursive B-spline basis. This is a project
specific relation between coefficient count and degree; it is not the general
definition of a B-spline.

### Knot construction

Define

```text
D       = b - a
epsilon = D / 100000
f       = factor
```

The knot vector contains `K - 1` knots close to each endpoint and one
interior knot:

```text
t = [
    a, a + epsilon, ..., a + (K - 2) * epsilon,
    a + D / (1 + f),
    b - (K - 2) * epsilon, ..., b - epsilon, b,
]
```

The small endpoint offsets are intentional: they reproduce the original
Fortran input convention. The endpoint basis-function convention is handled
by the original evaluator; these knots should therefore not be replaced by
strictly repeated endpoints when reproducing the Fortran parameterization.
The `factor` controls the interior-knot location and does not change the
polynomial degree.

Because `degBs = K - 1`, the interior-knot count is
`K - p - 1 = K - (K - 2) - 1 = 1` for **every** `K`. Increasing
`n_coeff_crust` or `n_coeff_mantle` therefore raises the polynomial degree and
widens the basis functions; it does not add independent depth resolution. The
practical consequences for the crust/mantle interface are described under
[the Moho contrast prior](#the-moho-contrast-prior).

### Greville depths

The Greville depth for coefficient `i` is the mean of `p` consecutive knots.
Using one-based knot indexing, this is

```text
xi_i = (t_(i+1) + t_(i+2) + ... + t_(i+p)) / p,
       i = 1, ..., K
```

In zero-based Python slicing, the same operation is:

```python
np.mean(knots[j + 1 : j + p + 1])  # j = 0, ..., K - 1
```

For example, with `factor=2`:

```text
Crust:  z_sed=5 km, z_moho=40 km, K=4, p=2
knots:  [5.000000, 5.000350, 5.000700, 16.666667,
         39.999300, 39.999650, 40.000000]
xi:     [5.000525, 10.833683, 28.332983, 39.999475] km

Mantle: z_moho=40 km, z_max=300 km, K=5, p=3
knots:  [40.000000, 40.002600, 40.005200, 40.007800, 126.666667,
         299.992200, 299.994800, 299.997400, 300.000000]
xi:     [40.005200, 68.893222, 155.555556, 242.217889, 299.994800] km
```

### Why use reference Vs at Greville depths as search centers?

The coefficients are not point samples of the velocity model. The velocity
profile is reconstructed as

```text
Vs(z) = sum_j c_j * B_j(z)
```

so each coefficient affects a depth range through its basis function. A
Greville depth is a geometry-derived representative location for that basis
function. Interpolating the reference profile at these locations and using

```text
center_j = Vs_reference(xi_j)
```

For a reference profile given by depth--velocity pairs `(z_i, V_i)`, the
center is obtained by linear interpolation on the actual depth coordinates:

```text
c_j = V_i + (xi_j - z_i) / (z_(i+1) - z_i) * (V_(i+1) - V_i),
      where z_i <= xi_j <= z_(i+1)
```

The reference samples do not need to be uniformly spaced. If a profile starts
at 3 km while a requested Greville depth lies between 0 and 3 km, the intended
shallow extension is the same straight line defined by the two shallowest
reference samples:

```text
V_s(z) = V_1 + (V_2 - V_1) / (z_2 - z_1) * (z - z_1)
```

This extension is an explicit shallow-depth assumption and should be flagged
when inspecting the generated centers. Deep extrapolation is not used.

The implementation allows this shallow extrapolation by default up to 5 km.
The limit and the policy can be changed with
`vs_constraints.allow_shallow_extrapolation` and
`vs_constraints.max_shallow_extrapolation_km`. Every prior record stores a
`shallow_extrapolated` flag so that this assumption remains auditable.

Using the reference Vs at Greville depths as the MCMC prior center is useful
because it:

- gives one reproducible center for every coefficient without pretending that
  the coefficient is an exact point velocity;
- follows the actual knot and basis-function geometry, including different
  crust and mantle intervals;
- works with non-uniform reference-model depth samples through direct linear
  interpolation;
- keeps the prior centered near the reference Earth model while allowing the
  inversion to change the whole profile through the B-spline basis;
- avoids fitting or resampling the reference model onto an unrelated uniform
  depth grid.

This is a stable center approximation, not an interpolation constraint: the
reconstructed profile is not required to pass through the reference Vs values
at the Greville depths. The selected half-widths still determine the actual
search range, and the resulting bounds should be checked for physical
validity, boundary-hugging posteriors, and the ability to represent narrow
low-velocity structures.

### The Moho contrast prior

**Why the two interface coefficients coincide.** The Fortran knot rule always
produces exactly **one** interior knot, so for any coefficient count `K` the
spline is clamped at both ends and the first and last Greville abscissae sit on
the layer boundaries. Because the crustal spline ends and the mantle spline
begins at the same Moho depth, the last crustal and first mantle coefficients
are *by construction* the two Moho velocities: their Greville depths coincide to
within a few metres (for the worked example below, about 6 m).

With `moho_vs_jump = 0` both coefficients are centred on the same continuous
reference value, and `moho_strict_margin` is left to force a purely numerical
separation of the initial midpoints. That has two costs:

- the two search windows are near-duplicates, so a large fraction of that pair's
  joint prior box is rejected by the executable's strict-jump check;
- the prior says nothing about the expected size of the Moho contrast, even
  though a positive jump is required.

**What `moho_vs_jump` changes.** Let `V = Vs_reference(z_moho)`; the two
interface centres become

```text
crust_last_center   = V - moho_vs_jump / 2
mantle_first_center = V + moho_vs_jump / 2
```

The half-widths are then applied as usual, and `moho_strict_margin` remains a
numerical floor. No other coefficient is affected.

**Worked example: point `122.00_33.50`.** Sediment is 2.40 km thick and the Moho
is at 32.49 km, so `V = 3.7495` km/s; the half-widths are 0.3 (crust) and 0.2
(mantle). Compare `moho_vs_jump = 0` with `0.3`:

| Interface coefficient | `jump = 0` centre | `jump = 0` window | `jump = 0.3` centre | `jump = 0.3` window |
| --------------------- | ----------------- | ----------------- | ------------------- | ------------------- |
| last crust (c4)       | 3.7494            | `[3.450, 4.049]`  | 3.5995              | `[3.300, 3.899]`    |
| first mantle (m1)     | 3.7503            | `[3.551, 3.950]`  | 3.8995              | `[3.700, 4.099]`    |
| realized contrast     | 0.001             |                   | 0.300               |                     |
| `P(m1 > c4)`          | 0.502             |                   | 0.918               |                     |

For `jump = 0` the mantle window `[3.551, 3.950]` sits entirely inside the
crustal window `[3.450, 4.049]` — the two coefficients are effectively
indistinguishable in the prior. At `jump = 0.3` the windows are shifted apart
about the reference value and only about 8% of the joint box violates the strict
jump, against about 50% before. Across the nine points of the example grid the
average rises from 0.501 to 0.917. The right-hand panel of the
[per-point figure](#per-point-figures) shows the resulting separation: the last
crustal marker (blue) and first mantle marker (orange) straddle the Moho line
instead of overlapping.

**Advantages of setting the jump.**

1. It encodes the *sign and scale* of the Moho contrast that the executable
   already requires, instead of relying on the 0.001 km/s numerical tick. The
   initial `oldpar` model is physically valid by construction.
2. It recovers a large part of the proposal space: the interface pair goes from
   a coin flip to a strongly biased prior, so fewer proposals are discarded by
   `goodmodel` before the likelihood is even evaluated.
3. It puts prior information exactly where the model is otherwise weakest. The
   basis diagnostics for the same point give mass fractions of 0.22 (c4) and
   0.08 (m1) and centroids of 27.5 km and 50.3 km, so both coefficients act far
   from the interface; without an explicit jump prior the pair carries almost no
   information about the very discontinuity the inversion is meant to resolve.
4. It does not pin the jump. The windows still overlap, so the contrast remains
   free over a broad range (5th-95th percentile about `jump ± 0.34` km/s); the
   executable's `goodmodel` check and the data stay in control.
5. It is auditable. `prior_bounds.csv` records `basis_centroid_km`,
   `basis_mass_fraction` and the realized `moho_contrast_km_s` on the two
   interface rows.

**Choosing the value.** `moho_vs_jump` is a physical assertion, not a tuning
knob for acceptance alone: the implied contrast keeps a standard deviation of
about 0.21 km/s set by the half-widths, so larger jumps simply shift the prior
towards larger contrasts. Take the value from receiver functions, petrology or a
regional reference; `0.3` km/s is the recommended value for the example grid
(`V = 3.75` km/s, roughly 8%). Keep `0.0` when the interface contrast should
stay genuinely uninformative, and remember that the overall acceptance rate
combines every executable check, of which this pair is one contributor.

## Fortran-compatible Vs prior bounds

Search centers are the reference Vs interpolated at each coefficient's Greville
depth. Configure the half-widths in `config.json` (km/s):

```json
"search_radius": {
  "sediment": 0.2,
  "moho": 1.0,
  "crust_vs": 0.3,
  "mantle_vs": 0.2
}
```

The Vs defaults are **0.3 for crust** and **0.2 for mantle**; either accepts
one value per coefficient. Sediment and Moho depth half-widths (km) remain
explicit. These widths express a chosen search prior, not measured uncertainty.

The writer constructs each requested `[center - radius, center + radius]`:

- Intersect with the global upper limit **4.9**, inclusive, for both crust and
  mantle. When water, sediment and ice are all off, also apply **0.5**, inclusive.
  Otherwise no 0.5 cutoff applies; the writer retains a nonnegative domain.
- Additionally cap crustal coefficients at **4.3 km/s**, inclusive, using
  `vs_constraints.crust_vs_max`. This configurable empirical prior is not a
  universal physical limit. The effective cap is the smaller of this value and
  `global_vs_max`; mantle and sediment retain the global cap. For a crustal
  center of 4.1 and radius 0.3, `[3.8, 4.4]` becomes `[3.8, 4.3]`.
- Apply **4.0**, inclusive, only to the deepest mantle coefficient's lower
  bound, using the Fortran spline endpoint convention.
- Only if the entire requested interval lies outside the admissible domain,
  translate its window to the nearest boundary, keeping the requested width where it fits.
  An interval touching the boundary is retained as a single value.
  This fallback is visible in the audit table and deserves reference-model review.
- Round lower bounds upward and upper bounds downward to three decimals.
- Allow overlapping Moho priors. If the first mantle midpoint already exceeds
  the last crust midpoint by `moho_strict_margin`, leave both intervals alone.
  Otherwise translate these two intervals by the smallest total number of
  output ticks needed, splitting the correction equally where space permits.
  If both sides exhaust their translation room, trim only the opposing edges.
  Impossible repairs fail with a diagnostic.

Fortran initializes `oldpar` at each interval midpoint. Thus the repair gives
this initial Vs model a strict Moho jump without excluding all overlapping
search ranges. The Fortran `goodmodel` check still rejects proposed models with
an invalid jump. There is no imposed monotonicity within crust or mantle, and
no requirement that every mantle coefficient exceed every crust coefficient.
For enabled sediment, the existing first-three-parameter monotonic bounds remain.

| Coefficient  | Reference center | Half-width | Written interval | Initial midpoint |
| ------------ | ---------------- | ---------- | ---------------- | ---------------- |
| Last crust   | 3.9              | 0.3        | [3.600, 4.200]   | 3.900            |
| First mantle | 4.0              | 0.2        | [3.800, 4.200]   | 4.000            |

Both reference centers remain inside these overlapping intervals. A deepest
reference of 4.1 with radius 0.2 becomes `[4.000, 4.300]`, without raising its
upper bound. Near a physical boundary, the actual half-width may shrink.
For example, a mantle reference of 4.95 with radius 0.2 yields
`[4.750, 4.900]` (initial midpoint 4.825), while 4.9 yields `[4.700, 4.900]`.
These rules ensure the stated Vs constraints for the midpoint initialization;
validity of every sampled model and other executable checks remain Fortran's job.

```json
"vs_constraints": {
  "global_vs_max": 4.9,
  "crust_vs_max": 4.3,
  "no_shallow_layers_vs_min": 0.5,
  "deepest_vs_min": 4.0,
  "moho_strict_margin": 0.001,
  "moho_vs_jump": 0.3
}
```

Limits may be tightened but cannot relax the Fortran constraints. The Moho margin
is a numerical initialization separation, not a prescribed geological jump; use
`moho_vs_jump` when a physical contrast prior is wanted. `prior_bounds.csv`
records the original `reference_vs_km_s`, configured radius, final bounds and
actual midpoint `effective_center_vs_km_s`; boundary adjustment never overwrites
the reference value. Check this audit, the basis diagnostics
(`basis_centroid_km`, `basis_mass_fraction`) and the realized
`moho_contrast_km_s`, plus posterior boundary accumulation when tuning priors.
Overlapping ranges can increase Fortran rejection rates compared with fully
separated intervals, while preserving more search space.

## Common errors

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `region extents must be divisible by grid_spacing` | extent is not an integer multiple of the spacing | adjust `region` or `grid_spacing`. |
| `... axis must have uniform spacing` | a source grid is not strictly regular | resample the product; off-grid points are rejected, not snapped. |
| `source ... does not cover the target ...` | a fixed field does not bracket the region | enlarge the source extent or shrink `region`. |
| `Reference Vs model is missing N inversion-grid profiles` | the Vs grid does not cover every target point | supply a model that covers `region`. |
| `[SKIP] ... only K valid dispersion points` | fewer periods than `minimum_periods` after alignment | lower `minimum_periods` or fix the dispersion product. |
| `Unknown configuration keys: ...` | unknown or misspelled configuration key | compare with the complete example above. |
| Matplotlib cache warning under a read-only `$HOME` | Matplotlib cannot write its config dir | set `MPLCONFIGDIR` to a writable directory. |

## Functions

### Prepare inversion grids

::: seispy.mcmc.workflow.init_grids

### Collect inversion results

::: seispy.mcmc.collection.collect_results

`collect_results` writes the established summary products for successful grid
points and continues after per-point failures. Its diagnostic table is printed
to the terminal by default; pass `diagnostics_path` only when a CSV diagnostic
file is wanted.

### Prior bounds

The writer and the diagnostic figures share one pure computation, so a figure
always matches the numbers written to `para.inp`.

::: seispy.mcmc.priors.compute_point_bounds

::: seispy.mcmc.priors.PriorSettings

### Target grid

::: seispy.mcmc.gridding.TargetGrid
