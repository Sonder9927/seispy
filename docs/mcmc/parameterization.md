# MCMC parameterization and priors

[Run the workflow](../recipes/mcmc.md) · [API reference](../api/mcmc.md)

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

### Greville sampling versus least-squares projection

The synthetic profile above isolates the geometry. The two figures below repeat
the comparison on a **real reference model**: point `122.00_33.50` of the
project's reference Vs model (a Shen2016/SL2013sv one-step combination), whose
aligned sediment thickness is 2.400 km and Moho depth is 34.544 km. Four crustal
and five mantle coefficients are used with `factor = 2` and a 300 km model
bottom, exactly as in the configuration.

Both panels are depth profiles in the same style as the model panel of
`point.png`: depth increases downward, the thick grey curve is the raw
reference, the blue and orange curves are the crustal and mantle fits, and the
shaded band between a fit and the reference is that layer's error.

[![Greville sampling on a real reference model: every coefficient is set to the
reference velocity at its Greville depth](../assets/mcmc-greville-centers.png){ .example-figure .example-figure--portrait }](../assets/mcmc-greville-centers.png)

*Greville sampling. The crustal fit is close, but the mantle fit starts at the
reference value at the Moho (4.07 km/s) and needs about 30 km to reach the
high-velocity lid, so the shallow mantle is reconstructed too slow; the residual
is concentrated between the Moho and roughly 80 km.*

[![Least-squares projection on the same reference model: coefficients are the
best fit of the reference profile in coefficient space](../assets/mcmc-projection-centers.png){ .example-figure .example-figure--portrait }](../assets/mcmc-projection-centers.png)

*Least-squares projection. Both fits follow the reference closely and the
leftover error is spread thinly instead of concentrating at the interface. The
coefficients themselves are scattered, because the projection trades local
accuracy for a smaller total residual.*

| Layer               | Metric       | Greville sampling | projection | improvement |
| ------------------- | ------------ | ----------------- | ---------- | ----------- |
| crust [2.40, 34.54] | max \|ΔVs\|  | 0.215 km/s        | 0.132 km/s | 1.6x        |
| crust [2.40, 34.54] | rms \|ΔVs\|  | 0.069 km/s        | 0.031 km/s | **2.2x**    |
| mantle [34.54, 300] | max \|ΔVs\|  | 0.320 km/s        | 0.219 km/s | 1.5x        |
| mantle [34.54, 300] | rms \|ΔVs\|  | 0.126 km/s        | 0.036 km/s | **3.4x**    |

Errors are measured on the same 400 interior samples the projection uses, so the
projection maximum is exactly the per-layer residual reported by `point.png`
and `prior_bounds.csv` (0.132 km/s crust, 0.219 km/s mantle here). Projection is
lower on every layer-point of the example grid -- 18 of 18 for the maximum, and
roughly 2-3.4x for the RMS -- because it is the least-squares optimum by
definition. The `max |ΔVs|` annotation in each figure is that audit signal.

What matters for a prior is not the error at one depth, but whether the
reference *as a coefficient vector* stays representable. That vector is exactly
`c*`, so a box centred on the projection contains it by construction: across the
nine-point grid, none of the 81 written intervals excludes it. A box centred on
the Greville samples with the configured half-widths (0.3 km/s crust, 0.2 km/s
mantle) does not: 32 of the 81 coefficients fall outside, by up to 0.44 km/s.
That is the sense in which projection is the better centring rule here. It also
removes the need for `moho_vs_jump`, because the projected pair differs by the
layer-mean contrast, which is positive for this point, whereas under Greville
sampling the pair collapses onto the 0.001 km/s numerical margin. See
[the Moho discontinuity and the interface coefficients](#the-moho-discontinuity-and-the-interface-coefficients)
for the caveat: that positive jump is a layer-mean contrast, not the reference
value at the interface.

The trade-off is visible at the clamped interface. The first mantle coefficient
*is* the reconstructed Vs at the Moho, so the Greville sample is exact there
(4.068 km/s, the reference value), while projection moves it to 4.317 km/s to
fit the whole layer and therefore overshoots the immediate Moho value by about
0.25 km/s. That is why `moho_strict_margin` and the model-space adjustment of
the prior box remain useful, and why the per-layer projection residual is
reported as an audit signal (`point.png` prints it, and `para.inp` bounds are
handled in node space).

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
separate them. **Least-squares projection removes that need**, because the two
layers are projected independently: the pair difference becomes the layer-mean
contrast, which is positive for a normal crust/mantle pair and therefore
satisfies the executable's strict-jump check without a synthetic margin.

The pair does not preserve the exact interface velocity, though. On the worked
example point the reference is continuous through the Moho (4.07 km/s on both
sides), while the projections give 4.05 km/s for the last crustal and 4.32 km/s
for the first mantle coefficient: the realized `moho_contrast_km_s` is
`+0.264` km/s, produced by fitting each layer as a whole rather than by the
reference value at the interface. The mantle coefficient consequently overshoots
the immediate Moho velocity by about 0.25 km/s. Increase the coefficient count,
or use a reference model that has a genuine sharp Moho sampled finely on both
sides of the interface, when the exact interface velocity matters.

`moho_strict_margin` remains as a numerical floor. It only fires if clipping or
a genuinely flat reference leaves the two initial midpoints closer than the
margin, and it never prescribes a geological contrast. `moho_vs_jump` is
deprecated and ignored: a nonzero value raises a `DeprecationWarning`.

For the worked example point `122.00_33.50` the written interface intervals are
`[3.753, 4.352]` km/s (last crustal) and `[4.117, 4.516]` km/s (first mantle),
so the two windows straddle the interface and the initial model has a positive
jump. `prior_bounds.csv` records the realized `moho_contrast_km_s` on the two
interface rows and the layer projection error on every row; check them, plus
posterior boundary accumulation, when tuning priors.

## Fortran-compatible Vs prior bounds

Search centers are the least-squares projection of the reference profile into
the Fortran coefficient space, one projection per layer. A typical
`config.json` configuration uses the following search half-widths; sediment
and Moho values are in **km**, while crustal and mantle Vs values are in
**km/s**:

```json
"search_radius": {
  "sediment": 2.0,
  "moho": 5.0,
  "crust_vs": 0.3,
  "mantle_vs": 0.2
}
```

These settings request sediment thickness within **±2 km** of the reference
thickness and Moho depth within **±5 km** of the reference depth. The sediment
lower endpoint is clipped at zero. Crustal and mantle coefficient windows
start at **±0.3 km/s** and **±0.2 km/s** around their respective projection
centres, before the constraints below are applied.

The Vs values are also the code defaults; either accepts one value per
coefficient. Sediment and Moho depth half-widths must be supplied explicitly.
These widths express a chosen search prior, not measured uncertainty.

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

