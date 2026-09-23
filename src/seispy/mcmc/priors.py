"""Prior bounds for the Fortran MCMC executable.

The executable samples ``n_basis`` B-spline Vs coefficients per layer. For each
coefficient the prior is a search interval centred on the reference model
interpolated at that coefficient's Greville depth, intersected with the
executable's admissible Vs domain and then repaired so the initial mid-point
model has a strict Moho jump.

This module is pure: it computes and returns bounds, and never touches the
filesystem. :mod:`seispy.mcmc.serialization` writes them.
"""

from __future__ import annotations

from dataclasses import dataclass, replace

import numpy as np

from seispy.mcmc.bspline import basis_geometry, greville_depths
from seispy.mcmc.config import (
    Config,
    SearchRadius,
    VsConstraints,
    expand_half_widths,
)
from seispy.mcmc.inversion import CRUST, MANTLE, SEDIMENT, InversionPoint
from seispy.mcmc.velocity import velocity_at_depths

BOUND_DECIMALS = 3
"""Decimal places written to ``para.inp``; bounds are quantized to this grid."""

_SECTIONS = (SEDIMENT, CRUST, MANTLE)


@dataclass(frozen=True)
class PriorBound:
    """Final prior bounds for one B-spline Vs coefficient.

    ``basis_centroid`` and ``basis_mass_fraction`` are diagnostics that show
    where the coefficient's basis function actually sits on the layer; they do
    not affect the written Fortran inputs.
    """

    section: str
    coefficient: int
    representative_depth: float
    reference_vs: float
    effective_center_vs: float
    search_radius: float
    lower: float
    upper: float
    shallow_extrapolated: bool = False
    basis_centroid: float | None = None
    basis_mass_fraction: float | None = None


@dataclass(frozen=True)
class PointPriorBounds:
    """Final, clipped prior bounds for a single inversion point.

    ``sediment`` holds one ``(lower, upper)`` Vs interval per sediment
    parameter; ``crust`` and ``mantle`` hold one :class:`PriorBound` per
    B-spline coefficient. Layer interfaces are 0.0 when inactive.
    ``moho_contrast`` is the realized Vs separation between the two Moho
    midpoints of the initial model, in km/s.
    """

    sediment: tuple[tuple[float, float], ...]
    crust: tuple[PriorBound, ...]
    mantle: tuple[PriorBound, ...]
    water_bottom: float
    sediment_bottom: float
    moho_depth: float
    max_depth: float
    moho_contrast: float | None = None


@dataclass(frozen=True)
class PriorSettings:
    """Prior-construction settings extracted from the full configuration."""

    search_radius: SearchRadius
    vs_constraints: VsConstraints
    sediment_intervals: tuple[tuple[float, float], ...]
    n_coeff_crust: int
    n_coeff_mantle: int
    factor: float
    bound_decimals: int = BOUND_DECIMALS

    @classmethod
    def from_config(cls, cfg: Config) -> "PriorSettings":
        return cls(
            search_radius=cfg.search_radius,
            vs_constraints=cfg.vs_constraints,
            sediment_intervals=cfg.sediment_intervals(),
            n_coeff_crust=cfg.n_coeff_crust,
            n_coeff_mantle=cfg.n_coeff_mantle,
            factor=cfg.factor,
        )

    def half_widths(self, section: str) -> np.ndarray:
        if section == CRUST:
            value, n_coeff = self.search_radius.crust_vs, self.n_coeff_crust
        elif section == MANTLE:
            value, n_coeff = self.search_radius.mantle_vs, self.n_coeff_mantle
        else:
            raise ValueError(f"Vs half-width is not defined for section: {section}")
        return expand_half_widths(value, n_coeff, f"search_radius.{section}_vs")


# =========================
# PUBLIC ENTRY POINT
# =========================


def compute_point_bounds(
    point: InversionPoint,
    settings: PriorSettings,
) -> PointPriorBounds:
    """Compute the final prior bounds for one inversion point.

    The same value is consumed by the writer and by the diagnostic figures, so
    a figure always matches the numbers written to ``para.inp``.

    The two layers share the Moho, and the Fortran knot rule pins the last
    crustal and first mantle Greville depth onto that shared interface. With
    ``vs_constraints.moho_vs_jump`` set, their centres are instead separated by
    the requested physical contrast; see :func:`_split_moho_centers`.
    """

    sediment = _sediment_bounds(settings) if point.sediment_on else ()
    minimum = minimum_vs(point, settings.vs_constraints)

    crust_depths, crust_centers = _layer_centers(
        point,
        settings,
        z_top=point.crustal_spline_top,
        z_bottom=point.moho_depth,
        section=CRUST,
    )
    mantle_depths, mantle_centers = _layer_centers(
        point,
        settings,
        z_top=point.moho_depth,
        z_bottom=point.max_depth,
        section=MANTLE,
    )
    crust_centers, mantle_centers = _split_moho_centers(
        point, settings, crust_centers, mantle_centers
    )

    crust = _bounds_from_centers(
        point,
        settings,
        section=CRUST,
        z_top=point.crustal_spline_top,
        z_bottom=point.moho_depth,
        depths=crust_depths,
        centers=crust_centers,
        minimum=minimum,
    )
    mantle = _bounds_from_centers(
        point,
        settings,
        section=MANTLE,
        z_top=point.moho_depth,
        z_bottom=point.max_depth,
        depths=mantle_depths,
        centers=mantle_centers,
        minimum=minimum,
    )
    crust, mantle = _repair_moho_jump(minimum, settings, crust, mantle)
    mantle = _constrain_deepest(mantle, settings)

    contrast = None
    if crust and mantle:
        contrast = float(
            mantle[0].effective_center_vs - crust[-1].effective_center_vs
        )

    return PointPriorBounds(
        sediment=tuple((float(low), float(high)) for low, high in sediment),
        crust=crust,
        mantle=mantle,
        water_bottom=float(point.water_depth) if point.water_on else 0.0,
        sediment_bottom=float(point.sediment_thickness) if point.sediment_on else 0.0,
        moho_depth=float(point.moho_depth),
        max_depth=float(point.max_depth),
        moho_contrast=contrast,
    )


# =========================
# VS DOMAIN
# =========================


def minimum_vs(point: InversionPoint, vc: VsConstraints) -> float:
    """Return the Fortran lower bound for this point's active model.

    The 0.5 km/s floor only applies when no shallow layer is active; water,
    sediment and ice keep the nonnegative domain.
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
    """Return executable Vs limits with the configured crustal prior cap."""

    if section not in _SECTIONS:
        raise ValueError(f"Unknown Vs section: {section}")
    lower = float(minimum)
    upper = float(vc.global_vs_max)
    if section == CRUST:
        upper = min(upper, float(vc.crust_vs_max))
    if not np.isfinite(lower) or not np.isfinite(upper) or lower > upper:
        raise ValueError(f"Invalid {section} Vs limits: lower={lower}, upper={upper}")
    return lower, upper


def _quantize_down(value: float, scale: float) -> float:
    return float(np.floor(value * scale + 1e-9) / scale)


def _quantize_up(value: float, scale: float) -> float:
    return float(np.ceil(value * scale - 1e-9) / scale)


def _apply_vs_limits(
    centers: np.ndarray,
    half_widths: np.ndarray,
    section: str,
    vc: VsConstraints,
    *,
    minimum: float,
    decimals: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Clip ``center +/- half_width`` to the executable's feasible interval.

    The requested half-width is never enlarged. Only if the entire requested
    window lies outside the feasible interval is it translated to the nearest
    feasible edge while retaining as much of its requested width as fits.
    Bounds are quantized directionally to the precision written to ``para.inp``.
    """

    vs_min, hard_max = _section_limits(section, vc, minimum=minimum)
    centers = np.asarray(centers, dtype=float)
    half_widths = np.asarray(half_widths, dtype=float)

    if centers.shape != half_widths.shape:
        raise ValueError("centers and half_widths must have identical shapes")
    if not np.isfinite(centers).all() or not np.isfinite(half_widths).all():
        raise ValueError("Vs centers and search half-widths must be finite")
    if np.any(half_widths < 0):
        raise ValueError("Vs search half-widths must be non-negative")

    scale = 10**decimals
    lower = np.empty_like(centers)
    upper = np.empty_like(centers)
    for index, (center, radius) in enumerate(zip(centers, half_widths, strict=True)):
        requested_width = 2.0 * float(radius)
        if center + radius < vs_min - 1e-12:
            low, high = vs_min, min(hard_max, vs_min + requested_width)
        elif center - radius > hard_max + 1e-12:
            high = hard_max
            low = max(vs_min, hard_max - requested_width)
        else:
            low = max(vs_min, center - radius)
            high = min(hard_max, center + radius)

        low = _quantize_up(low, scale)
        high = _quantize_down(high, scale)
        if low > high + 1e-12:
            raise ValueError(
                f"No representable {section} Vs interval for center={center:.6f}, "
                f"half_width={radius:.6f}"
            )
        lower[index] = low
        upper[index] = high
    return lower, upper


# =========================
# B-SPLINE CENTRES AND BOUNDS
# =========================


def _layer_centers(
    point: InversionPoint,
    settings: PriorSettings,
    *,
    z_top: float,
    z_bottom: float,
    section: str,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(greville_depths, reference_centers)`` for one layer."""

    vc = settings.vs_constraints
    n_coeff = settings.n_coeff_crust if section == CRUST else settings.n_coeff_mantle
    depths = greville_depths(n_coeff, z_top, z_bottom, settings.factor)
    centers = velocity_at_depths(
        point.vs_profile,
        depths,
        allow_shallow_extrapolation=vc.allow_shallow_extrapolation,
        max_shallow_extrapolation_km=vc.max_shallow_extrapolation_km,
    )
    return depths, centers


def _split_moho_centers(
    point: InversionPoint,
    settings: PriorSettings,
    crust_centers: np.ndarray,
    mantle_centers: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Separate the two Moho coefficients by the configured Vs contrast.

    The Fortran knot rule puts the last crustal and first mantle Greville depth
    on the shared Moho interface, so their reference centres are the same value.
    Splitting them symmetrically around the reference value turns the pair into
    an explicit contrast prior instead of leaving both windows effectively
    identical. ``moho_vs_jump = 0`` leaves the centres untouched.
    """

    jump = float(settings.vs_constraints.moho_vs_jump)
    if jump <= 0 or crust_centers.size == 0 or mantle_centers.size == 0:
        return crust_centers, mantle_centers

    base = float(
        velocity_at_depths(point.vs_profile, np.array([point.moho_depth]))[0]
    )
    crust = crust_centers.copy()
    mantle = mantle_centers.copy()
    crust[-1] = base - 0.5 * jump
    mantle[0] = base + 0.5 * jump
    return crust, mantle


def _bounds_from_centers(
    point: InversionPoint,
    settings: PriorSettings,
    *,
    section: str,
    z_top: float,
    z_bottom: float,
    depths: np.ndarray,
    centers: np.ndarray,
    minimum: float,
) -> tuple[PriorBound, ...]:
    """Clip per-coefficient windows and attach basis-geometry diagnostics."""

    vc = settings.vs_constraints
    half_widths = settings.half_widths(section)
    lower, upper = _apply_vs_limits(
        centers,
        half_widths,
        section,
        vc,
        minimum=minimum,
        decimals=settings.bound_decimals,
    )
    mass_fraction, centroid = basis_geometry(
        depths.size, z_top, z_bottom, settings.factor
    )

    return tuple(
        PriorBound(
            section=section,
            coefficient=index,
            representative_depth=float(depth),
            reference_vs=float(center),
            effective_center_vs=float(0.5 * (low + high)),
            search_radius=float(radius),
            lower=float(low),
            upper=float(high),
            shallow_extrapolated=bool(depth < point.vs_profile.min_depth),
            basis_centroid=float(centroid[index - 1]),
            basis_mass_fraction=float(mass_fraction[index - 1]),
        )
        for index, (depth, center, radius, low, high) in enumerate(
            zip(depths, centers, half_widths, lower, upper, strict=True),
            start=1,
        )
    )


# =========================
# CROSS-LAYER REPAIRS
# =========================


def _repair_moho_jump(
    minimum: float,
    settings: PriorSettings,
    crust: tuple[PriorBound, ...],
    mantle: tuple[PriorBound, ...],
) -> tuple[tuple[PriorBound, ...], tuple[PriorBound, ...]]:
    """Repair the mid-point Moho jump, retaining overlapping priors.

    Fortran initializes each coefficient at its interval midpoint, and its
    endpoint convention makes the last crust and first mantle coefficient the
    two Moho velocities. The two midpoints must therefore differ by at least
    ``moho_strict_margin``. The intervals themselves may keep overlapping; the
    executable still validates every proposed model.

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
        replace(
            last_crust,
            lower=cl / scale,
            upper=cu / scale,
            effective_center_vs=(cl + cu) / (2 * scale),
        ),
    )
    adjusted_mantle = (
        replace(
            first_mantle,
            lower=ml / scale,
            upper=mu / scale,
            effective_center_vs=(ml + mu) / (2 * scale),
        ),
    ) + mantle[1:]
    return adjusted_crust, adjusted_mantle


def _constrain_deepest(
    mantle: tuple[PriorBound, ...],
    settings: PriorSettings,
) -> tuple[PriorBound, ...]:
    """Intersect the deepest requested window with Vs >= deepest_vs_min.

    Translate only when the entire requested window misses the feasible domain.
    """

    if not mantle:
        return mantle
    vc = settings.vs_constraints
    deepest = mantle[-1]
    if deepest.lower >= vc.deepest_vs_min:
        return mantle

    lower, upper = _apply_vs_limits(
        np.array([deepest.reference_vs]),
        np.array([deepest.search_radius]),
        MANTLE,
        vc,
        minimum=max(float(vc.deepest_vs_min), deepest.lower),
        decimals=settings.bound_decimals,
    )
    return mantle[:-1] + (
        replace(
            deepest,
            lower=float(lower[0]),
            upper=float(upper[0]),
            effective_center_vs=float((lower[0] + upper[0]) / 2),
        ),
    )


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
