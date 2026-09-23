"""Fortran-compatible B-spline depth geometry.

The MCMC executable reads prior bounds for ``n_basis`` coefficients per layer
and evaluates a B-spline basis of its own construction. This module reproduces
that construction so the prior search centres can be placed at the coefficients'
Greville depths.

The original parameterization uses ``degBs = n_basis - 1`` as the spline order,
so the polynomial degree is ``p = n_basis - 2`` and the knot vector length is
``2 * n_basis - 1``. Endpoints are represented by small offsets rather than
repeated values because the Fortran evaluator applies its own endpoint
convention; see ``docs/api/mcmc.md`` for the derivation.
"""

from __future__ import annotations

import numpy as np

_ENDPOINT_OFFSET_FRACTION = 100000.0


def fortran_knot_vector(
    n_basis: int,
    z_top: float,
    z_bottom: float,
    factor: float,
) -> np.ndarray:
    """Reproduce the original Fortran knot construction.

    The vector holds ``n_basis - 1`` tightly spaced knots at each endpoint and
    exactly one interior knot at ``z_top + span / (1 + factor)``.
    """

    if n_basis < 3:
        raise ValueError("n_basis must be >= 3 for the Fortran B-spline rule")
    if z_bottom <= z_top:
        raise ValueError(f"Invalid spline interval: {z_top} -> {z_bottom}")
    if not np.isfinite(factor) or factor <= 0:
        raise ValueError(f"factor must be finite and > 0, got {factor}")

    spline_order = n_basis - 1
    n_knots = 2 * n_basis - 1
    span = z_bottom - z_top
    epsilon = span / _ENDPOINT_OFFSET_FRACTION

    knots = np.empty(n_knots, dtype=float)
    knots[:spline_order] = z_top + np.arange(spline_order) * epsilon
    knots[spline_order] = z_top + span / (factor + 1.0)
    knots[n_knots - spline_order :] = (
        z_bottom - np.arange(spline_order - 1, -1, -1) * epsilon
    )
    return knots


def greville_depths(
    n_basis: int,
    z_top: float,
    z_bottom: float,
    factor: float,
) -> np.ndarray:
    """Return the Greville depth of each Fortran B-spline coefficient.

    The ``i``-th Greville abscissa is the mean of ``p = n_basis - 2``
    consecutive knots: ``xi_i = mean(U[i + 1 : i + p + 1])``.

    Because the knot rule always produces exactly one interior knot, the first
    and last Greville abscissae sit on the layer endpoints by construction. Two
    adjacent layers that share an interface therefore receive identical endpoint
    depths; see :func:`basis_geometry` for a diagnostic that shows where each
    basis function actually carries its weight.
    """

    knots = fortran_knot_vector(n_basis, z_top, z_bottom, factor)
    degree = n_basis - 2
    return np.asarray(
        [np.mean(knots[j + 1 : j + degree + 1]) for j in range(n_basis)],
        dtype=float,
    )


def basis_matrix(
    n_basis: int,
    z_top: float,
    z_bottom: float,
    factor: float,
    depths: np.ndarray,
) -> np.ndarray:
    """Evaluate every Fortran B-spline basis function at ``depths``.

    Returns an array of shape ``(len(depths), n_basis)`` using the Cox-de Boor
    recursion on :func:`fortran_knot_vector`. The basis forms a partition of
    unity strictly inside the interval.
    """

    knots = fortran_knot_vector(n_basis, z_top, z_bottom, factor)
    degree = n_basis - 2
    x = np.asarray(depths, dtype=float)
    if x.ndim != 1:
        raise ValueError("depths must be a 1-D array")

    count = knots.size - degree - 1
    values = np.zeros((x.size, count))
    for i in range(count):
        if knots[i] < knots[i + 1]:
            values[:, i] = ((x >= knots[i]) & (x < knots[i + 1])).astype(float)

    # The final knot belongs to the last basis function by the half-open rule.
    right = x >= knots[-1]
    values[right, :] = 0.0
    values[right, -1] = 1.0

    for k in range(1, degree + 1):
        updated = np.zeros_like(values)
        for i in range(count):
            left_span = knots[i + k] - knots[i]
            if left_span > 0:
                updated[:, i] += (x - knots[i]) / left_span * values[:, i]
            if i + 1 < count:
                right_span = knots[i + k + 1] - knots[i + 1]
                if right_span > 0:
                    updated[:, i] += (
                        (knots[i + k + 1] - x) / right_span * values[:, i + 1]
                    )
        values = updated
    return values


def basis_geometry(
    n_basis: int,
    z_top: float,
    z_bottom: float,
    factor: float,
    *,
    samples: int = 513,
) -> tuple[np.ndarray, np.ndarray]:
    """Return ``(mass_fraction, centroid_depth)`` per basis function.

    Both arrays are diagnostics that complement :func:`greville_depths`: the
    mass fraction says how much of the layer each coefficient controls and the
    centroid says where that control actually sits. Midpoint quadrature keeps
    the integral exact for the partition-of-unity property.
    """

    if samples < 2:
        raise ValueError(f"samples must be >= 2, got {samples}")

    edges = np.linspace(z_top, z_bottom, samples + 1)
    depths = 0.5 * (edges[:-1] + edges[1:])
    step = (z_bottom - z_top) / samples
    values = basis_matrix(n_basis, z_top, z_bottom, factor, depths)

    mass = values.sum(axis=0) * step
    total = float(mass.sum())
    if total <= 0:
        raise ValueError("B-spline basis has no mass on the layer interval")
    centroid = (values * depths[:, None]).sum(axis=0) * step / mass
    return mass / total, centroid

