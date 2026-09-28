# MCMC API reference

[Copy a workflow](../recipes/mcmc.md#generate-grid-inputs) · [Configuration](../mcmc/configuration.md) · [Parameterization](../mcmc/parameterization.md)

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

## Workflow details

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

<a id="complete-configjson-example"></a>
<a id="top-level-settings"></a>
<a id="paths"></a>
<a id="input-units"></a>
<a id="physical-quantities-and-units"></a>
<a id="search-widths-and-vs-constraints"></a>
<a id="spline-resolution-and-phase-constraints"></a>
<a id="fortran-run-settings-mcmc_params"></a>
<a id="input-formats-and-target-grid"></a>

## MCMC configuration and inputs

See [MCMC configuration and inputs](../mcmc/configuration.md) for the full explanation.

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

[![Example per-point figure: dispersion on the left, Vs search intervals on
the right](../assets/mcmc-point-example.png){ .example-figure }](../assets/mcmc-point-example.png)

*Real-data example generated with `plot=True` at grid point `122.00_33.50`
(Moho 34.54 km). Left: phase dispersion with one-sigma bars. Right: Vs
coefficients with their final search intervals placed at their basis centroids,
the least-squares reference projection as the dashed initial spline, the raw
reference profile (grey), the sediment layer, the Moho, and the model bottom.
The last crustal marker (blue) and first mantle marker (orange) sit on either
side of the Moho line. The annotated layer projection error (0.132 km/s crust,
0.219 km/s mantle) is the audit signal for how well the chosen coefficient
count follows this reference. For how the centring rule itself is chosen, see
[Greville sampling versus least-squares projection](#greville-sampling-versus-least-squares-projection).*

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

<a id="b-spline-parameterization"></a>
<a id="knot-construction"></a>
<a id="greville-depths-and-coefficient-space-locations"></a>
<a id="greville-sampling-versus-least-squares-projection"></a>
<a id="why-projection-centres-and-why-the-basis-centroid-matters"></a>
<a id="the-moho-discontinuity-and-the-interface-coefficients"></a>
<a id="fortran-compatible-vs-prior-bounds"></a>

## MCMC parameterization and priors

See [MCMC parameterization and priors](../mcmc/parameterization.md) for the full explanation.

## Common errors

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| `region extents must be divisible by grid_spacing` | extent is not an integer multiple of the spacing | adjust `region` or `grid_spacing`. |
| `... axis must have uniform spacing` | a source grid is not strictly regular | resample the product; off-grid points are rejected, not snapped. |
| `source ... does not cover the target ...` | a fixed field does not bracket the region | enlarge the source extent or shrink `region`. |
| `Reference Vs model is missing N inversion-grid profiles` | the Vs grid does not cover every target point | supply a model that covers `region`. |
| `[SKIP] ... only K valid dispersion points` | fewer periods than `minimum_periods` after alignment | lower `minimum_periods` or fix the dispersion product. |
| `Unknown configuration keys: ...` | unknown or misspelled configuration key | compare with the [configuration example](../mcmc/configuration.md#complete-configjson-example). |
| Matplotlib cache warning under a read-only `$HOME` | Matplotlib cannot write its config dir | set `MPLCONFIGDIR` to a writable directory. |
