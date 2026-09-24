"""Prior bounds for the Fortran MCMC executable.

The executable samples B-spline Vs coefficients per layer and validates the
reconstructed node velocities, so these bounds are built in two spaces:

* each coefficient is centred on the least-squares projection of the reference
  profile into the coefficient space;
* the requested window is then projected onto the executable's model-space
  limits (upper 4.9, lower 0.5 when no shallow layer, deepest node 4.0). Interior
  coefficients may pass the limits while the reconstructed box stays inside
  them; endpoint coefficients are node values, so their limits are exact.

Projecting the reference keeps it representable: coefficients live in
coefficient space, while a Greville sample only matches one for linear
profiles. The old Greville rule is kept, deprecated, in
greville_reference_centers.

This module is pure: it computes and returns bounds and never touches the
filesystem.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, replace

import numpy as np

from seispy.mcmc.bspline import (
    basis_geometry,
    basis_matrix,
    greville_depths,
    node_matrix,
    projection_coefficients,
)
from seispy.mcmc.config import (
    Config,
    SearchRadius,
    VsConstraints,
    expand_half_widths,
)
from seispy.mcmc.inversion import CRUST, MANTLE, SEDIMENT, InversionPoint
from seispy.mcmc.velocity import velocity_at_depths

BOUND_DECIMALS = 3
"""Decimal places written to para.inp; bounds are quantized to this grid."""

PROJECTION_SAMPLES = 400
"""Depth samples used for the least-squares projection of a reference layer."""

_SECTIONS = (SEDIMENT, CRUST, MANTLE)


@dataclass(frozen=True, eq=False)
class LayerSpec:
    """Geometry, sizing and requested widths for one B-spline layer.

    Bundles the values that every layer-level step needs, so they travel as one
    object instead of a repeated (z_top, z_bottom, factor, n_coeff, n_nodes)
    clump.

    Attributes:
    section
        Layer name (crust or mantle).
    z_top, z_bottom
        Layer depth interval in km.
    factor
        Fortran knot-spacing control.
    n_coeff
        Number of adjustable B-spline coefficients.
    n_nodes
        Number of forward-model nodes the executable validates.
    half_widths
        Requested per-coefficient search half-widths in km/s.
    """

    section: str
    z_top: float
    z_bottom: float
    factor: float
    n_coeff: int
    n_nodes: int
    half_widths: np.ndarray

    @property
    def width_array(self) -> np.ndarray:
        """Return the requested half-widths as a float array."""

        return np.asarray(self.half_widths, dtype=float)


@dataclass(frozen=True)
class LayerDiagnostics:
    """Diagnostics shared by every coefficient of one layer.

    Attributes:
    projection_max_error
        Largest absolute residual of the reference projection, in km/s.
    window_scale
        Factor applied to the requested half-widths on the tighter side of the
        model-space feasibility scaling; 1.0 means the request fitted.
    """

    projection_max_error: float | None = None
    window_scale: float = 1.0


@dataclass(frozen=True)
class PriorBound:
    """Final prior bounds for one B-spline Vs coefficient.

    Attributes:
    section, coefficient
        Layer name and 1-based coefficient index.
    greville_depth
        Canonical geometric label of the coefficient, in km.
    basis_centroid
        Mass-weighted depth of the basis function, in km.
    reference_vs_at_centroid
        Reference velocity at the centroid, in km/s.
    projection_vs
        Raw least-squares projection coefficient, in km/s.
    center_vs
        Projection coefficient after clipping into the admissible domain; the
        nominal centre of the requested window.
    search_radius
        Requested half-width in km/s.
    lower, upper
        Written interval in km/s; its midpoint is effective_center_vs.
    shallow_extrapolated
        Whether the coefficient's Greville label lies above the reference
        profile coverage.
    basis_mass_fraction
        Share of the layer the basis function controls.
    """

    section: str
    coefficient: int
    greville_depth: float
    basis_centroid: float
    reference_vs_at_centroid: float
    projection_vs: float
    center_vs: float
    search_radius: float
    lower: float
    upper: float
    shallow_extrapolated: bool = False
    basis_mass_fraction: float | None = None

    @property
    def effective_center_vs(self) -> float:
        """Midpoint of the written interval, where Fortran starts oldpar."""

        return 0.5 * (self.lower + self.upper)


@dataclass(frozen=True)
class PointPriorBounds:
    """Final bounds and layer diagnostics for a single inversion point.

    Attributes:
    sediment
        One (lower, upper) Vs interval per sediment parameter.
    crust, mantle
        One PriorBound per B-spline coefficient.
    water_bottom, sediment_bottom, moho_depth, max_depth
        Layer interfaces in km; 0.0 when a shallow layer is inactive.
    factor
        Knot-spacing control needed to reconstruct the spline.
    moho_contrast
        Realized Vs separation between the two Moho midpoints, in km/s.
    crust_diagnostics, mantle_diagnostics
        Per-layer projection error and applied window scale.
    """

    sediment: tuple[tuple[float, float], ...]
    crust: tuple[PriorBound, ...]
    mantle: tuple[PriorBound, ...]
    water_bottom: float
    sediment_bottom: float
    moho_depth: float
    max_depth: float
    factor: float
    moho_contrast: float | None = None
    crust_diagnostics: LayerDiagnostics = LayerDiagnostics()
    mantle_diagnostics: LayerDiagnostics = LayerDiagnostics()


@dataclass(frozen=True)
class PriorSettings:
    """Prior-construction settings extracted from the full configuration.

    Attributes:
    search_radius
        Requested per-coefficient Vs half-widths.
    vs_constraints
        Executable limits and the extrapolation policy.
    sediment_intervals
        Configured sediment Vs intervals.
    n_coeff_crust, n_coeff_mantle
        Number of B-spline coefficients per layer.
    factor
        Knot-spacing control.
    npts_crust, npts_mantle
        Forward-model node counts.
    bound_decimals
        Decimal places written to para.inp.
    projection_samples
        Depth samples used for the least-squares projection.
    """

    search_radius: SearchRadius
    vs_constraints: VsConstraints
    sediment_intervals: tuple[tuple[float, float], ...]
    n_coeff_crust: int
    n_coeff_mantle: int
    factor: float
    npts_crust: int = 20
    npts_mantle: int = 20
    bound_decimals: int = BOUND_DECIMALS
    projection_samples: int = PROJECTION_SAMPLES

    @classmethod
    def from_config(cls, cfg: Config) -> "PriorSettings":
        """Build prior settings from a validated configuration.

        Args:
        cfg
            Fully validated configuration.

        Returns:
        PriorSettings
            Settings for compute_point_bounds.
        """

        return cls(
            search_radius=cfg.search_radius,
            vs_constraints=cfg.vs_constraints,
            sediment_intervals=cfg.sediment_intervals(),
            n_coeff_crust=cfg.n_coeff_crust,
            n_coeff_mantle=cfg.n_coeff_mantle,
            factor=cfg.factor,
            npts_crust=cfg.NPTS_cBs,
            npts_mantle=cfg.NPTS_mBs,
        )

    def layer(self, section: str, z_top: float, z_bottom: float) -> LayerSpec:
        """Return the geometry and widths for one Vs layer.

        Args:
        section
            CRUST or MANTLE.
        z_top, z_bottom
            Layer interval in km.

        Returns:
        LayerSpec
            Layer geometry, coefficient/node counts and requested half-widths.

        Raises:
        ValueError
            If the section is neither crust nor mantle.
        """

        if section == CRUST:
            n_coeff = self.n_coeff_crust
            n_nodes = self.npts_crust
            radius = self.search_radius.crust_vs
        elif section == MANTLE:
            n_coeff = self.n_coeff_mantle
            n_nodes = self.npts_mantle
            radius = self.search_radius.mantle_vs
        else:
            raise ValueError(f"Vs layer geometry is not defined for: {section}")
        return LayerSpec(
            section=section,
            z_top=float(z_top),
            z_bottom=float(z_bottom),
            factor=float(self.factor),
            n_coeff=n_coeff,
            n_nodes=n_nodes,
            half_widths=expand_half_widths(
                radius, n_coeff, f"search_radius.{section}_vs"
            ),
        )


# =========================
# PUBLIC ENTRY POINT
# =========================


def compute_point_bounds(
    point: InversionPoint,
    settings: PriorSettings,
) -> PointPriorBounds:
    """Compute the final prior bounds for one inversion point.

    Args:
    point
        Validated inversion point with its reference Vs profile.
    settings
        Prior-construction settings.

    Returns:
    PointPriorBounds
        Final clipped intervals and per-layer diagnostics, shared by the
        Fortran writer and the diagnostic figures.

    Raises:
    ValueError
        If the projection cannot produce a representable interval for a layer.
    """

    sediment = _sediment_bounds(settings) if point.sediment_on else ()
    minimum = minimum_vs(point, settings.vs_constraints)
    crust_layer = settings.layer(CRUST, point.crustal_spline_top, point.moho_depth)
    mantle_layer = settings.layer(MANTLE, point.moho_depth, point.max_depth)

    crust_centers, crust_error = _projection_centers(point, settings, crust_layer)
    mantle_centers, mantle_error = _projection_centers(point, settings, mantle_layer)

    crust, crust_diagnostics = _bounds_from_centers(
        point, settings, crust_layer, crust_centers, crust_error, minimum
    )
    mantle, mantle_diagnostics = _bounds_from_centers(
        point, settings, mantle_layer, mantle_centers, mantle_error, minimum
    )
    crust, mantle = _repair_moho_jump(minimum, settings, crust, mantle)

    contrast = None
    if crust and mantle:
        contrast = float(mantle[0].effective_center_vs - crust[-1].effective_center_vs)

    return PointPriorBounds(
        sediment=tuple((float(low), float(high)) for low, high in sediment),
        crust=crust,
        mantle=mantle,
        water_bottom=float(point.water_depth) if point.water_on else 0.0,
        sediment_bottom=float(point.sediment_thickness) if point.sediment_on else 0.0,
        moho_depth=float(point.moho_depth),
        max_depth=float(point.max_depth),
        factor=float(settings.factor),
        moho_contrast=contrast,
        crust_diagnostics=crust_diagnostics,
        mantle_diagnostics=mantle_diagnostics,
    )


def reconstruct_initial_model(
    point: InversionPoint,
    bounds: PointPriorBounds,
    *,
    samples: int = 401,
) -> tuple[np.ndarray, np.ndarray]:
    """Evaluate the spline built from the written interval midpoints.

    Args:
    point
        Inversion point giving the layer interfaces.
    bounds
        Final bounds whose interval midpoints initialize the executable.
    samples
        Number of depth samples per layer; must be at least 2.

    Returns:
    tuple of numpy.ndarray
        (depth_km, vs_km_s) for the concatenated two-layer profile.

    Raises:
    ValueError
        If samples is smaller than 2.
    """

    if samples < 2:
        raise ValueError(f"samples must be >= 2, got {samples}")
    z_crust = np.linspace(point.crustal_spline_top, point.moho_depth, samples)
    z_mantle = np.linspace(point.moho_depth, point.max_depth, samples)
    a_crust = basis_matrix(
        len(bounds.crust),
        point.crustal_spline_top,
        point.moho_depth,
        bounds.factor,
        z_crust,
    )
    a_mantle = basis_matrix(
        len(bounds.mantle),
        point.moho_depth,
        point.max_depth,
        bounds.factor,
        z_mantle,
    )
    c = np.array([b.effective_center_vs for b in bounds.crust])
    m = np.array([b.effective_center_vs for b in bounds.mantle])
    vs_crust = a_crust @ c
    vs_mantle = a_mantle @ m
    # Fortran clamps the endpoint basis functions, so a layer boundary is
    # exactly its first or last coefficient.
    vs_crust[0], vs_crust[-1] = c[0], c[-1]
    vs_mantle[0], vs_mantle[-1] = m[0], m[-1]
    depth = np.concatenate([z_crust, z_mantle[1:]])
    vs = np.concatenate([vs_crust, vs_mantle[1:]])
    return depth, vs


# =========================
# VS DOMAIN
# =========================


def minimum_vs(point: InversionPoint, vc: VsConstraints) -> float:
    """Return the Fortran lower bound for this point's active model.

    Args:
    point
        Inversion point whose shallow-layer switches decide the floor.
    vc
        Executable Vs constraints.

    Returns:
    float
        Lower node-velocity bound in km/s: 0.0 when water, sediment or ice is
        active, otherwise the configured no-shallow-layer floor.
    """

    if point.water_on or point.sediment_on or point.ice_on:
        return 0.0
    return float(vc.no_shallow_layers_vs_min)


def _section_limits(
    section: str,
    vc: VsConstraints,
    *,
    minimum: float = 0.0,
) -> tuple[float, float]:
    """Return the executable Vs limits for a section."""

    if section not in _SECTIONS:
        raise ValueError(f"Unknown Vs section: {section}")
    lower = float(minimum)
    upper = float(vc.global_vs_max)
    if not np.isfinite(lower) or not np.isfinite(upper) or lower > upper:
        raise ValueError(f"Invalid {section} Vs limits: lower={lower}, upper={upper}")
    return lower, upper


def _quantize_down(value: float, scale: float) -> float:
    return float(np.floor(value * scale + 1e-9) / scale)


def _quantize_up(value: float, scale: float) -> float:
    return float(np.ceil(value * scale - 1e-9) / scale)


# =========================
# B-SPLINE CENTRES AND BOUNDS
# =========================


def greville_reference_centers(
    point: InversionPoint,
    settings: PriorSettings,
    *,
    section: str,
    z_top: float,
    z_bottom: float,
) -> tuple[np.ndarray, np.ndarray]:
    """DEPRECATED backup of the pre-projection prior centres.

    .. deprecated::
        Sampling the reference at each coefficient's Greville depth assumes the
        coefficient equals the velocity at that depth. That holds only for
        linear profiles: at the Moho the first mantle Greville depth sits on the
        crustal side of the jump, so the sampled centre can be far below the
        coefficient that fits the layer.

    Args:
    point
        Inversion point with the reference Vs profile.
    settings
        Prior settings providing the factor and extrapolation policy.
    section
        CRUST or MANTLE.
    z_top, z_bottom
        Layer interval in km.

    Returns:
    tuple of numpy.ndarray
        (greville_depths, reference_centers) for the layer.

    Warns:
    DeprecationWarning
        Always; use compute_point_bounds instead.
    """

    warnings.warn(
        "greville_reference_centers() is deprecated: Greville-sampled centres "
        "are only valid for linear profiles. Use the least-squares projection "
        "centres instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    vc = settings.vs_constraints
    layer = settings.layer(section, z_top, z_bottom)
    depths = greville_depths(layer.n_coeff, layer.z_top, layer.z_bottom, layer.factor)
    centers = velocity_at_depths(
        point.vs_profile,
        depths,
        allow_shallow_extrapolation=vc.allow_shallow_extrapolation,
        max_shallow_extrapolation_km=vc.max_shallow_extrapolation_km,
    )
    return depths, centers


def _projection_centers(
    point: InversionPoint,
    settings: PriorSettings,
    layer: LayerSpec,
) -> tuple[np.ndarray, float]:
    """Return (projection coefficients, max layer reconstruction error)."""

    vc = settings.vs_constraints
    # Sample strictly inside the layer: the Fortran basis is degenerate at the
    # exact endpoints, which would otherwise dominate the residual.
    depths = np.linspace(layer.z_top, layer.z_bottom, settings.projection_samples + 2)[
        1:-1
    ]
    values = velocity_at_depths(
        point.vs_profile,
        depths,
        allow_shallow_extrapolation=vc.allow_shallow_extrapolation,
        max_shallow_extrapolation_km=vc.max_shallow_extrapolation_km,
    )
    return projection_coefficients(
        layer.n_coeff, layer.z_top, layer.z_bottom, layer.factor, depths, values
    )


def _model_space_window(
    layer: LayerSpec,
    raw_centers: np.ndarray,
    *,
    minimum: float,
    deepest_min: float | None,
    model_max: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float]:
    """Project the requested coefficient window onto the model-space limits.

    The Fortran solver validates reconstructed node velocities, not the
    coefficients, so this enforces
    node_matrix @ lower >= model_min_nodes and node_matrix @ upper <= model_max.
    Interior coefficients may therefore pass the limits while the reconstructed
    box stays inside them.

    Returns:
    tuple
        (clipped_centers, lower, upper, lower_scale, upper_scale) where the
        scales are in [0, 1] and equal 1.0 when the request already fits.
    """

    centers = np.clip(np.asarray(raw_centers, dtype=float), minimum, model_max)
    if deepest_min is not None and centers.size:
        centers[-1] = max(float(centers[-1]), float(deepest_min))

    matrix = node_matrix(
        layer.n_coeff, layer.z_top, layer.z_bottom, layer.factor, layer.n_nodes
    )
    node_center = matrix @ centers
    node_extent = matrix @ layer.width_array
    model_min_nodes = np.full(layer.n_nodes, float(minimum))
    if deepest_min is not None:
        model_min_nodes[-1] = max(float(minimum), float(deepest_min))

    positive = node_extent > 1e-12
    upper_scale = 1.0
    lower_scale = 1.0
    if positive.any():
        upper_scale = float(
            np.min((model_max - node_center[positive]) / node_extent[positive])
        )
        lower_scale = float(
            np.min(
                (node_center[positive] - model_min_nodes[positive])
                / node_extent[positive]
            )
        )
        upper_scale = min(1.0, max(0.0, upper_scale))
        lower_scale = min(1.0, max(0.0, lower_scale))

    half = layer.width_array
    return (
        centers,
        centers - lower_scale * half,
        centers + upper_scale * half,
        lower_scale,
        upper_scale,
    )


def _bounds_from_centers(
    point: InversionPoint,
    settings: PriorSettings,
    layer: LayerSpec,
    raw_centers: np.ndarray,
    layer_error: float,
    minimum: float,
) -> tuple[tuple[PriorBound, ...], LayerDiagnostics]:
    """Clip one layer's windows in model space and attach diagnostics."""

    vc = settings.vs_constraints
    deepest_min = float(vc.deepest_vs_min) if layer.section == MANTLE else None
    centers, lower, upper, lower_scale, upper_scale = _model_space_window(
        layer,
        raw_centers,
        minimum=minimum,
        deepest_min=deepest_min,
        model_max=float(vc.global_vs_max),
    )

    ticks = 10**settings.bound_decimals
    lower = np.array([_quantize_up(value, ticks) for value in lower])
    upper = np.array([_quantize_down(value, ticks) for value in upper])
    if np.any(lower > upper + 1e-12):
        raise ValueError(
            f"No representable {layer.section} Vs interval after model-space limits"
        )

    mass_fraction, centroid = basis_geometry(
        layer.n_coeff, layer.z_top, layer.z_bottom, layer.factor
    )
    greville = greville_depths(layer.n_coeff, layer.z_top, layer.z_bottom, layer.factor)
    reference_at_centroid = velocity_at_depths(
        point.vs_profile,
        centroid,
        allow_shallow_extrapolation=vc.allow_shallow_extrapolation,
        max_shallow_extrapolation_km=vc.max_shallow_extrapolation_km,
    )
    raw = np.asarray(raw_centers, dtype=float)

    bounds = tuple(
        PriorBound(
            section=layer.section,
            coefficient=index,
            greville_depth=float(greville[index - 1]),
            basis_centroid=float(centroid[index - 1]),
            reference_vs_at_centroid=float(reference_at_centroid[index - 1]),
            projection_vs=float(raw[index - 1]),
            center_vs=float(centers[index - 1]),
            search_radius=float(layer.width_array[index - 1]),
            lower=float(lower[index - 1]),
            upper=float(upper[index - 1]),
            shallow_extrapolated=bool(greville[index - 1] < point.vs_profile.min_depth),
            basis_mass_fraction=float(mass_fraction[index - 1]),
        )
        for index in range(1, layer.n_coeff + 1)
    )
    diagnostics = LayerDiagnostics(
        projection_max_error=float(layer_error),
        window_scale=float(min(lower_scale, upper_scale)),
    )
    return bounds, diagnostics


def _repair_moho_jump(
    minimum: float,
    settings: PriorSettings,
    crust: tuple[PriorBound, ...],
    mantle: tuple[PriorBound, ...],
) -> tuple[tuple[PriorBound, ...], tuple[PriorBound, ...]]:
    """Repair the mid-point Moho jump, retaining overlapping priors.

    The projection centres already reproduce the reference Moho contrast, so
    this only fires when clipping or a flat reference leaves the two initial
    midpoints closer than moho_strict_margin. The intervals themselves may keep
    overlapping; the executable still validates every proposed model.

    The correction is expressed in whole output ticks so rounding cannot erase
    the required separation.
    """

    if not crust or not mantle:
        return crust, mantle

    vc = settings.vs_constraints
    scale = 10**settings.bound_decimals
    floor_tick = int(np.ceil(minimum * scale - 1e-9))
    ceiling_tick = int(np.floor(vc.global_vs_max * scale + 1e-9))
    gap_ticks = max(1, int(np.ceil(vc.moho_strict_margin * scale - 1e-9)))

    last_crust, first_mantle = crust[-1], mantle[0]
    cl, cu, ml, mu = (
        int(round(value * scale))
        for value in (
            last_crust.lower,
            last_crust.upper,
            first_mantle.lower,
            first_mantle.upper,
        )
    )

    # Each tick of translation moves a midpoint by half a tick, so a pair of
    # opposing ticks closes one full tick of separation.
    steps = max(0, (2 * gap_ticks - (ml + mu - cl - cu) + 1) // 2)
    if not steps:
        return crust, mantle

    down_room, up_room = cl - floor_tick, ceiling_tick - mu
    down = min(steps // 2, down_room)
    up = min(steps - down, up_room)
    down = min(steps - up, down_room)
    cl, cu, ml, mu = cl - down, cu - down, ml + up, mu + up

    # If very wide windows exhaust translation room, trim opposing edges while
    # retaining at least one output tick for each non-fixed interval.
    remaining = max(0, 2 * gap_ticks - (ml + mu - cl - cu))
    if remaining:
        crust_room = max(0, cu - cl - 1)
        mantle_room = max(0, mu - ml - 1)
        trim_c = min(remaining // 2, crust_room)
        trim_m = min(remaining - trim_c, mantle_room)
        trim_c = min(remaining - trim_m, crust_room)
        if trim_c + trim_m < remaining:
            raise ValueError(
                "Cannot initialize a strict Moho Vs jump within the physical limits"
            )
        cu -= trim_c
        ml += trim_m

    adjusted_crust = crust[:-1] + (
        replace(last_crust, lower=cl / scale, upper=cu / scale),
    )
    adjusted_mantle = (
        replace(first_mantle, lower=ml / scale, upper=mu / scale),
    ) + mantle[1:]
    return adjusted_crust, adjusted_mantle


# =========================
# SEDIMENT BOUNDS
# =========================


def _sediment_bounds(
    settings: PriorSettings,
) -> tuple[tuple[float, float], ...]:
    """Return sediment ranges whose search centres strictly increase."""

    if not settings.sediment_intervals:
        raise ValueError(
            "At least one sediment Vs parameter is required when the "
            "sediment layer is enabled"
        )

    vc = settings.vs_constraints
    vs_min, vs_max = _section_limits(SEDIMENT, vc)
    scale = 10**settings.bound_decimals
    ranges: list[tuple[float, float]] = []
    for bounds in settings.sediment_intervals:
        low = float(np.clip(bounds[0], vs_min, vs_max))
        high = float(np.clip(bounds[1], vs_min, vs_max))
        low = _quantize_up(low, scale)
        high = _quantize_down(high, scale)
        if high < low:
            raise ValueError(
                f"Sediment Vs interval [{bounds[0]}, {bounds[1]}] has no "
                "representable value at para.inp precision"
            )
        ranges.append((low, high))

    # Quantization can collapse two distinct configured centres onto one output
    # tick, so the ordering requirement is re-checked on the written values.
    centres = [0.5 * (low + high) for low, high in ranges]
    for index in range(len(centres) - 1):
        if centres[index] >= centres[index + 1]:
            raise ValueError(
                "sediment_vs search centres must be strictly increasing at "
                "para.inp precision: "
                f"centre {index + 1}={centres[index]:.3f} >= "
                f"centre {index + 2}={centres[index + 1]:.3f}"
            )
    return tuple(ranges)
