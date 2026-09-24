# MCMC interface

`seispy.mcmc` turns a JSON configuration and five regular lon/lat products
(topography, sediment thickness, Moho depth, reference Vs, and phase
dispersion) into one self-contained inversion directory per grid point. Grid
preparation never launches the external Fortran inversion:

1. **Target grid** - `region` and `grid_spacing` define the authoritative nodes.
2. **Spatial fields** - topography, sediment and Moho are aligned to that grid.
3. **Phase dispersion** - the long table or NetCDF cube is pivoted and aligned.
4. **Reference Vs** - profiles are aligned, and every target node must have one.
5. **Preflight** - every inversion point is built, its prior bounds are
   validated, and points with too few valid dispersion rows are skipped before
   any output directory is created.
6. **Write** - each accepted point receives `phase.input`, `para.inp`,
   `input_DRAM_T.dat`, `prior_bounds.csv`, and `point.json`.
7. **Plot** (optional) - `point.png` is redrawn from the written files, so
   plotting is a separate phase that can be re-run with `mcmc.plot_grids`.

All five products share one three-case alignment (copy / interpolate /
aggregate); no external gridding tool is required.

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
| `vs_constraints.moho_vs_jump`                       | km/s | **Deprecated and ignored.** Least-squares projection already reproduces the reference Moho contrast; a nonzero value only raises a `DeprecationWarning`.                                                                                                        |
| `vs_constraints.allow_shallow_extrapolation`        | bool | Whether a finite shallow gap in the reference profile is filled by linear extrapolation from the two shallowest samples.                                                                                                                                            |
| `vs_constraints.max_shallow_extrapolation_km`       | km   | Maximum shallow gap filled when extrapolation is enabled; `null` removes the gap limit.                                                                                                                                                                             |

Bounds intersect the requested interval with the applicable limits; only a
wholly out-of-domain interval triggers fallback translation. Consequently,
Fortran's initial midpoint can differ from the projection centre. For example,
crust `4.1 +/- 0.3` becomes `[3.8, 4.3]`, while mantle
`4.95 +/- 0.2` becomes `[4.75, 4.9]`. Inspect
`prior_bounds.csv` for the projection centres, final bounds, and
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
| `bspline.py`       | Reproduce the Fortran knot vector, Greville depths and coefficient-space projection.   |
| `inversion.py`     | The `InversionPoint` model and its layer/threshold logic.                              |
| `priors.py`        | Pure computation of the final per-coefficient Vs prior bounds.                         |
| `serialization.py` | Write `phase.input`, `para.inp`, `prior_bounds.csv`, `point.json` and `input_DRAM_T.dat`. |
| `point_io.py`      | Read a written point directory back into point, bounds and dispersion.                  |
| `plotting.py`      | Diagnostic dispersion and Vs-model figures (needs the `plot` extra).                   |
| `workflow.py`      | Preflight every point, then write each point serially.                                 |
| `collection.py`    | Collect completed inversions into summary tables and figures.                          |

The package-level API is intentionally small:
`mcmc.init_grids(config_path)`, `mcmc.plot_grids(grids_dir)` and
`mcmc.collect_results(grids_dir, out_dir)`. Internal modules are
free to evolve without coupling callers to file-format details.

## Per-point figures

Four plotting helpers make a single inversion point inspectable. Install the
optional extra first with `pip install "seispy[plot]"`.

```python
from seispy.mcmc.plotting import plot_dispersion, plot_model, plot_point
from seispy.mcmc.priors import PriorSettings, compute_point_bounds

bounds = compute_point_bounds(point, PriorSettings.from_config(cfg))
plot_dispersion(curve, default_sigma=cfg.default_phase_std)  # period vs phase velocity
plot_model(point, bounds)  # depth vs Vs search intervals
figure, axes = plot_point(point, curve, bounds, output_file="point.png")
```

![Example per-point figure: dispersion on the left, Vs search intervals on
the right](../assets/mcmc-point-example.png)

*Real-data example generated with `plot=True` at grid point
`122.00_33.50` (Moho 32.49 km). Left: phase dispersion with one-sigma bars.
Right: Vs coefficients with their final search intervals placed at their basis
centroids, the least-squares reference projection as the dashed initial spline,
the raw reference profile (grey), the sediment layer, the Moho, and the model
bottom. The last crustal marker (blue) and first mantle marker (orange) sit on
either side of the Moho line, as described under
[the Moho discontinuity and the interface coefficients](#the-moho-discontinuity-and-the-interface-coefficients).
This reference has a sharp shallow gradient and an uppermost-mantle
low-velocity zone, so the degree-2 and degree-3 layer splines cannot follow it
exactly; the annotated layer projection error (0.800 km/s crust, 0.511 km/s
mantle) is the audit signal for that resolution limit.*

- `plot_dispersion(curve, ...)` draws phase velocity against period with
  one-sigma error bars; non-finite sigmas fall back to `default_sigma`
  when it is given, otherwise those samples are drawn without error bars.
- `plot_model(point, bounds, ...)` places each coefficient interval at its
  basis centroid (the depth where the basis function carries its weight, which
  is the meaningful depth for reading a coefficient value), overlays the raw
  reference profile and the reconstructed initial spline, annotates each
  layer's projection error, and marks the water or sediment interface (if any),
  the Moho, and the model bottom. Sediment intervals are placed across the
  sediment layer and joined to show the linear trend.
- `compute_point_bounds(point, settings)` is the pure bound
  computation shared by the writer and the plots, so a figure always matches
  `para.inp`.
- `plot_point_dir(point_dir, ...)` redraws `point.png` from a written
  directory (`point.json`, `prior_bounds.csv`, `phase.input`), so figures can
  be regenerated without the source grids or the configuration.
- `mcmc.init_grids(config_path, plot=True)` writes the combined figure as
  `point.png` after every input is written, and `mcmc.plot_grids(grids_dir)`
  redraws them later. Skipped points are neither written nor plotted and leave
  no directory behind.

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
[the Moho discontinuity and the interface coefficients](#the-moho-discontinuity-and-the-interface-coefficients).

### Greville depths and coefficient-space locations

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

Extending the same example with a reference profile that has a sharp Moho at
40 km:

```text
depth (km):   5     10    20    40    40.1   60    80    120   200   300
Vs (km/s):   3.20  3.35  3.55  3.80  4.47  4.49  4.50  4.50  4.50  4.60
```

The **basis centroid** `c_bar_j` is the mass-weighted depth of basis function
`B_j`,

```text
c_bar_j = ( integral z * B_j(z) dz ) / ( integral B_j(z) dz )
```

evaluated by midpoint quadrature (see `bspline.basis_geometry`). The
**projection centre** `c*_j` is the least-squares solution of

```text
min_c || sum_j c_j * B_j(z) - Vs_reference(z) ||
```

on the layer, with `z` taken on a fine interior grid (see
`bspline.projection_coefficients`). For this example:

| Layer  | `j` | Greville `xi_j` (km) | centroid `c_bar_j` (km) | `Vs_reference(c_bar_j)` (km/s) | projection `c*_j` (km/s) |
| ------ | ----- | ---------------------- | ------------------------- | -------------------------------- | -------------------------- |
| crust  | 1     | 5.000525               | 7.916741                  | 3.2875                           | 3.1984                     |
| crust  | 2     | 10.833683              | 16.666721                 | 3.4833                           | 3.3894                     |
| crust  | 3     | 28.332983              | 25.416600                 | 3.6177                           | 3.6769                     |
| crust  | 4     | 39.999475              | 34.166629                 | 3.7271                           | 3.7924                     |
| mantle | 1     | 40.005200              | 57.334124                 | 4.4873                           | 4.4662                     |
| mantle | 2     | 68.893222              | 109.334499                | 4.5000                           | 4.5134                     |
| mantle | 3     | 155.555556             | 161.333333                | 4.5000                           | 4.4757                     |
| mantle | 4     | 242.217889             | 213.332020                | 4.5133                           | 4.5198                     |
| mantle | 5     | 299.994800             | 265.332938                | 4.5653                           | 4.6074                     |

The maximum reconstruction error
`max |sum_j c*_j B_j(z) - Vs_reference(z)|` is 0.0115 km/s for the crust and
0.0096 km/s for the mantle.

A Greville depth is the natural, geometry-derived **label** of a coefficient.
The Greville abscissae also reproduce linear functions exactly, because

```text
sum_j xi_j * B_j(z) = z
```

That identity is why Greville interpolation is accurate whenever the reference
velocity is locally linear. It is also why it fails at the Moho. In the table
above the first mantle Greville depth is 40.005 km, only 5 m below the
interface, where this reference is still on the crustal side of the jump
(about 3.84 km/s). Its basis centroid is at 57.33 km, where the reference is
4.49 km/s, and the projection is 4.47 km/s. Using the Greville sample would
place the whole prior window for the uppermost mantle far too low.

### Why projection centres, and why the basis centroid matters

The coefficients are not point samples of the velocity model. The velocity
profile is reconstructed as

```text
Vs(z) = sum_j c_j * B_j(z)
```

so each coefficient affects a depth range through its basis function. Two
ways of locating a coefficient follow.

- **Greville depth** is the coefficient's canonical index label. Sampling
  `Vs_reference(xi_j)` is exact only for linear profiles.
- **Basis centroid** is where the basis function actually carries its mass:
  the depth `c_bar_j = integral z * B_j(z) dz / integral B_j(z) dz`. For the
  first mantle coefficient this is tens of kilometres below the Moho, and the
  reference velocity there is a far better representative of that coefficient
  than the Greville sample. `prior_bounds.csv` records both the Greville depth
  and the centroid, together with the reference velocity at the centroid.

The bound constructor uses neither sample directly. It uses the **coefficient
space projection**, the vector `c*` that minimises

```text
|| sum_j c_j * B_j(z) - Vs_reference(z) ||
```

over the layer. That is the coefficient-space analogue of sampling: it returns
the coefficients that best represent the whole layer, so the reference model
stays inside the prior box. Its value tracks the basis centroid rather than the
Greville depth at the interface.

For a reference profile given by depth--velocity pairs `(z_i, V_i)`, the
projection is evaluated from `V(z)` sampled on a fine interior depth grid; the
reference samples do not need to be uniformly spaced. If a profile starts at
3 km while the layer starts at 0 km, the intended shallow extension is the same
straight line defined by the two shallowest reference samples:

```text
V_s(z) = V_1 + (V_2 - V_1) / (z_2 - z_1) * (z - z_1)
```

This extension is an explicit shallow-depth assumption and should be flagged
when inspecting the generated centers. Deep extrapolation is not used.

The implementation allows this shallow extrapolation by default up to 5 km.
The limit and the policy can be changed with
`vs_constraints.allow_shallow_extrapolation` and
`vs_constraints.max_shallow_extrapolation_km`. Every prior record stores a
`shallow_extrapolated` flag so that this assumption remains auditable. The flag
is recorded per layer: if the layer starts above the reference coverage, every
coefficient in it depends on the extrapolation.

The old Greville-sampled rule is kept for comparison as the deprecated
`seispy.mcmc.priors.greville_reference_centers`. It emits a
`DeprecationWarning` and should not be used for new priors.

Projection centres are a stable fit, not an interpolation constraint: the
reconstructed profile is not required to pass through any particular reference
value. The selected half-widths still determine the actual search range, and
the resulting bounds should be checked for physical validity,
boundary-hugging posteriors, and the ability to represent narrow low-velocity
structures.

### The Moho discontinuity and the interface coefficients

The Fortran knot rule always produces exactly **one** interior knot, so for any
coefficient count `K` the spline is clamped at both ends and the first and last
Greville abscissae sit on the layer boundaries. The crustal spline ends and the
mantle spline begins at the same Moho depth, so the last crustal and first
mantle coefficients are the two Moho velocities.

With Greville sampling those two coefficients were both centred on the
continuous reference value at the interface, and `moho_vs_jump` was needed to
separate them. **Least-squares projection removes that need.** The two layers
are projected independently, so the last crustal coefficient is fit to the
crustal reference and the first mantle coefficient to the mantle reference; the
pair automatically carries the reference model's own contrast.

`moho_strict_margin` remains as a numerical floor. It only fires if clipping or
a genuinely flat reference leaves the two initial midpoints closer than the
margin, and it never prescribes a geological contrast. `moho_vs_jump` is
deprecated and ignored: a nonzero value raises a `DeprecationWarning`.

This is also why a reference model with a sharp Moho must be sampled finely
enough around the interface. The projection uses the reference velocities on
each side of the jump, so a profile that ramps over tens of kilometres will
produce a correspondingly smooth contrast bias.

For the worked example point `122.00_33.50`, the crustal projection ends near
the crustal value below the sediment and the mantle projection starts near the
mantle value below the Moho, so the two interface windows straddle the
interface and the initial model has a positive jump. `prior_bounds.csv`
records the realized `moho_contrast_km_s` on the two interface rows and the
layer projection error on every row; check them, plus posterior boundary
accumulation, when tuning priors.

## Fortran-compatible Vs prior bounds

Search centers are the least-squares projection of the reference profile into
the Fortran coefficient space, one projection per layer. Configure the
half-widths in `config.json` (km/s):

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

The writer starts from each requested `[center - radius, center + radius]`:

- The Fortran hard limits are **model-space** limits: the solver rejects a
  proposal when a reconstructed node velocity exceeds **4.9**, drops below
  **0.5** (only when water, sediment and ice are all off) or, for the deepest
  node, falls below **4.0**. The writer enforces the same constraints on the
  reconstructed box `node_matrix @ upper` and `node_matrix @ lower`, **not on
  each coefficient**. An interior coefficient window may therefore extend past
  4.9 (or below 0.5) while every node velocity stays inside the limit; endpoint
  coefficients are node values (the Fortran basis is clamped), so their caps are
  exact.
- A projection centre outside the admissible domain is clipped into it first,
  and the deepest mantle coefficient is raised to `deepest_vs_min`.
- If the requested box still reconstructs outside the limits, a common factor
  scales the upper or lower half-width in until the whole box fits. The tighter
  factor is written as `window_scale` (1.0 when the request already fits).
- Round lower bounds upward and upper bounds downward to three decimals.
- Allow overlapping Moho priors. If the first mantle midpoint already exceeds
  the last crust midpoint by `moho_strict_margin`, leave both intervals alone.
  Otherwise translate these two intervals by the smallest total number of
  output ticks needed, splitting the correction equally where space permits.
  If both sides exhaust their translation room, trim only the opposing edges.
  Impossible repairs fail with a diagnostic.

Fortran initializes `oldpar` at each interval midpoint. Because the projection
already places the two interface midpoints on the correct sides of the Moho,
the initial Vs model has a positive jump without a synthetic `moho_vs_jump`.
The Fortran `goodmodel` check still rejects proposed models with an invalid
jump. There is no imposed monotonicity within crust or mantle, and no
requirement that every mantle coefficient exceed every crust coefficient. For
enabled sediment, the existing first-three-parameter monotonic bounds remain.

Near a physical boundary the actual half-width may shrink. A mantle projection
of 4.95 with radius 0.2 is clipped to 4.9, so the writer scales the upper side in
until the box fits and records `window_scale < 1`. Because the projection is a
coefficient-space fit it can sit outside the sampled velocity range; the audit
table records the raw projection (`projection_vs_km_s`), the clipped centre
(`center_vs_km_s`), the written interval and the applied scale. These rules
guarantee the **model** constraints for the box; the midpoint initialization and
every sampled model are still validated by the executable.

```json
"vs_constraints": {
  "global_vs_max": 4.9,
  "no_shallow_layers_vs_min": 0.5,
  "deepest_vs_min": 4.0,
  "moho_strict_margin": 0.001
}
```

Limits may be tightened but cannot relax the Fortran constraints. The Moho
margin is a numerical initialization separation, not a prescribed geological
jump. `prior_bounds.csv` records, per coefficient, the Greville label
(`greville_depth_km`), the basis centroid (`basis_centroid_km`), the raw
projection (`projection_vs_km_s`), the clipped centre actually used
(`center_vs_km_s`), the reference velocity at the centroid
(`reference_vs_at_centroid_km_s`), the configured radius, the written
`lower_km_s`/`upper_km_s`, the initialization midpoint
(`initial_midpoint_vs_km_s`), the basis mass fraction, and the
shallow-extrapolation flag. The per-layer projection error
(`layer_projection_max_error_km_s`) and applied scaling (`window_scale`) are
repeated on every row of the layer they describe, and the realized
`moho_contrast_km_s` is attached to the two interface rows. The figure marks
each coefficient at its basis centroid and annotates each layer's projection
error. Check the audit for centres outside the reference range (boundary
adjustment), zero-width intervals, large projection errors, and posterior
boundary accumulation. Overlapping ranges can increase Fortran rejection rates
compared with fully separated intervals, while preserving more search space.

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

### Plot inversion grids

::: seispy.mcmc.workflow.plot_grids

`plot_grids` redraws `point.png` for every point directory that holds a
`point.json`. It reads only the written files, so it can run after an inversion
or after moving the directory, and a failure at one point is reported without
stopping the others.

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
