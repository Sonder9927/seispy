"""Diagnostic figures for dispersion curves and prior Vs models.

Matplotlib is imported inside the plotting functions so the rest of the MCMC
module stays importable without the optional ``plot`` extra.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np

from seispy.mcmc.dispersion import DispersionCurve
from seispy.mcmc.inversion import InversionPoint
from seispy.mcmc.priors import PointPriorBounds

if TYPE_CHECKING:
    from matplotlib.axes import Axes
    from matplotlib.figure import Figure

CRUST_COLOR = "#0072B2"
MANTLE_COLOR = "#D55E00"
SEDIMENT_COLOR = "#009E73"
WATER_COLOR = "#56B4E9"
DISPERSION_COLOR = "#0072B2"


def plot_dispersion(
    curve: DispersionCurve,
    ax: Axes | None = None,
    *,
    default_sigma: float | None = None,
    log_period: bool = False,
    label: str | None = None,
    color: str = DISPERSION_COLOR,
) -> Axes:
    """Plot phase velocity against period with one-sigma error bars.

    Non-finite or non-positive sigmas are replaced by ``default_sigma`` when it
    is given; otherwise those samples are drawn without error bars.
    """

    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots()

    periods = np.asarray(curve.periods, dtype=float)
    velocities = np.asarray(curve.velocities, dtype=float)
    sigmas = np.asarray(curve.sigmas, dtype=float)

    finite = np.isfinite(periods) & np.isfinite(velocities)
    periods, velocities, sigmas = periods[finite], velocities[finite], sigmas[finite]
    order = np.argsort(periods)
    periods, velocities, sigmas = periods[order], velocities[order], sigmas[order]

    if default_sigma is not None:
        fallback = float(default_sigma)
        if not np.isfinite(fallback) or fallback <= 0:
            raise ValueError(
                f"default_sigma must be positive and finite, got {default_sigma}"
            )
        sigmas = np.where(np.isfinite(sigmas) & (sigmas > 0), sigmas, fallback)
    errors = np.where(np.isfinite(sigmas) & (sigmas > 0), sigmas, np.nan)

    ax.errorbar(
        periods,
        velocities,
        yerr=errors,
        fmt="o-",
        color=color,
        ecolor=color,
        capsize=2,
        markersize=4,
        linewidth=0.8,
        label=label,
    )
    ax.set_xlabel("Period (s)")
    ax.set_ylabel("Phase velocity (km/s)")
    if log_period:
        ax.set_xscale("log")
    if label is not None:
        ax.legend()
    return ax


def plot_model(
    point: InversionPoint,
    bounds: PointPriorBounds,
    ax: Axes | None = None,
    *,
    show_reference: bool = True,
) -> Axes:
    """Plot Greville Vs search intervals against depth for one point.

    ``bounds`` must be the final, clipped bounds from
    :func:`seispy.mcmc.priors.compute_point_bounds`, so the figure matches the
    numbers written to ``para.inp``.
    """

    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots()

    for points, marker, color in (
        (bounds.crust, "o", CRUST_COLOR),
        (bounds.mantle, "s", MANTLE_COLOR),
    ):
        if not points:
            continue
        depth = np.array([bound.representative_depth for bound in points])
        center = np.array([bound.effective_center_vs for bound in points])
        lower = np.array([bound.lower for bound in points])
        upper = np.array([bound.upper for bound in points])
        ax.errorbar(
            center,
            depth,
            xerr=np.vstack((center - lower, upper - center)),
            fmt=marker,
            color=color,
            ecolor=color,
            capsize=2,
            markersize=4,
            linewidth=0.8,
            label=points[0].section,
        )

    if bounds.sediment:
        lower = np.array([low for low, _ in bounds.sediment])
        upper = np.array([high for _, high in bounds.sediment])
        center = 0.5 * (lower + upper)
        count = len(bounds.sediment)
        depths = (
            np.array([0.5 * bounds.sediment_bottom])
            if count == 1
            else np.linspace(0.0, bounds.sediment_bottom, count)
        )
        ax.errorbar(
            center,
            depths,
            xerr=np.vstack((center - lower, upper - center)),
            fmt="^",
            color=SEDIMENT_COLOR,
            ecolor=SEDIMENT_COLOR,
            capsize=2,
            markersize=5,
            linewidth=0.8,
            label="sediment",
        )
        if count > 1:
            ax.plot(center, depths, color=SEDIMENT_COLOR, linewidth=0.8, linestyle=":")

    if show_reference:
        ax.plot(
            np.asarray(point.vs_profile.vs, dtype=float),
            np.asarray(point.vs_profile.depth, dtype=float),
            color="0.5",
            linewidth=1.0,
            label="reference",
        )

    if point.water_on:
        ax.axhspan(0.0, point.water_depth, color=WATER_COLOR, alpha=0.20, zorder=0)
        ax.axhline(point.water_depth, color=WATER_COLOR, linewidth=1.0, linestyle="--")
    if point.sediment_on:
        ax.axhspan(
            0.0, point.sediment_thickness, color=SEDIMENT_COLOR, alpha=0.12, zorder=0
        )
        ax.axhline(
            point.sediment_thickness,
            color=SEDIMENT_COLOR,
            linewidth=1.0,
            linestyle="--",
        )
    ax.axhline(point.moho_depth, color="black", linewidth=1.2)
    ax.axhline(point.max_depth, color="0.3", linewidth=1.0, linestyle=":")

    for depth, text in (
        (point.water_depth if point.water_on else None, "water bottom"),
        (point.sediment_thickness if point.sediment_on else None, "sediment bottom"),
        (point.moho_depth, "Moho"),
        (point.max_depth, "model bottom"),
    ):
        if depth is None:
            continue
        ax.text(
            0.01,
            depth,
            text,
            transform=ax.get_yaxis_transform(),
            ha="left",
            va="bottom",
            fontsize=8,
            color="0.2",
        )

    limits = [bound.upper for bound in (*bounds.crust, *bounds.mantle)]
    limits += [high for _, high in bounds.sediment]
    if limits:
        ax.set_xlim(left=0.0, right=1.05 * max(limits))
    ax.invert_yaxis()
    ax.set_xlabel("Vs (km/s)")
    ax.set_ylabel("Depth (km)")
    ax.set_title(point.folder_name)
    if ax.get_legend_handles_labels()[0]:
        ax.legend(loc="lower right", fontsize=8)
    return ax


def plot_point(
    point: InversionPoint,
    curve: DispersionCurve,
    bounds: PointPriorBounds,
    *,
    default_sigma: float | None = None,
    log_period: bool = False,
    output_file: str | Path | None = None,
    dpi: int = 300,
) -> tuple[Figure, tuple[Axes, Axes]]:
    """Draw one point as a two-panel dispersion and Vs-model figure."""

    import matplotlib.pyplot as plt

    figure, (ax_dispersion, ax_model) = plt.subplots(
        1, 2, figsize=(11.0, 4.5), constrained_layout=True
    )
    plot_dispersion(
        curve, ax=ax_dispersion, default_sigma=default_sigma, log_period=log_period
    )
    plot_model(point, bounds, ax=ax_model)
    ax_dispersion.set_title("Dispersion")
    ax_model.set_title("Vs model")
    figure.suptitle(f"{point.folder_name}   lon={point.lon:.3f}   lat={point.lat:.3f}")
    if output_file is not None:
        figure.savefig(output_file, dpi=dpi)
    return figure, (ax_dispersion, ax_model)
