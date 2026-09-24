"""Read a written point directory back into plot-ready objects.

The readers use the same column schema as the writer, so the audit table and
its parser cannot drift apart.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np

from seispy.mcmc.dispersion import DispersionCurve
from seispy.mcmc.inversion import CRUST, MANTLE, InversionPoint
from seispy.mcmc.priors import LayerDiagnostics, PointPriorBounds, PriorBound
from seispy.mcmc.serialization import PRIOR_BOUND_COLUMNS
from seispy.mcmc.velocity import VsProfile


def read_phase_input(path: str | Path) -> DispersionCurve:
    """Read a Fortran phase.input file.

    Args:
        path: Path to phase.input.

    Returns:
        The parsed dispersion curve.
    """

    periods: list[float] = []
    velocities: list[float] = []
    sigmas: list[float] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        parts = line.split()
        if len(parts) >= 6 and parts[0] == "2":
            periods.append(float(parts[3]))
            velocities.append(float(parts[4]))
            sigmas.append(float(parts[5]))
    return DispersionCurve(
        periods=np.asarray(periods, dtype=float),
        velocities=np.asarray(velocities, dtype=float),
        sigmas=np.asarray(sigmas, dtype=float),
    )


def read_prior_bounds(
    path: str | Path,
) -> tuple[
    tuple[PriorBound, ...],
    tuple[PriorBound, ...],
    float | None,
    LayerDiagnostics,
    LayerDiagnostics,
]:
    """Read prior_bounds.csv into bounds and per-layer diagnostics.

    Args:
        path: Path to prior_bounds.csv.

    Returns:
        Crust bounds, mantle bounds, realized Moho contrast and the crust and
        mantle diagnostics.
    """

    crust: list[PriorBound] = []
    mantle: list[PriorBound] = []
    diagnostics: dict[str, LayerDiagnostics] = {}
    moho_contrast: float | None = None
    with Path(path).open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            values = {
                column.name: column.decode(row[column.name])
                for column in PRIOR_BOUND_COLUMNS
            }
            kwargs = {
                column.field: values[column.name]
                for column in PRIOR_BOUND_COLUMNS
                if column.field is not None
            }
            bound = PriorBound(**kwargs)
            if bound.section == CRUST:
                crust.append(bound)
            elif bound.section == MANTLE:
                mantle.append(bound)
            if bound.section not in diagnostics:
                projection_error = values["layer_projection_max_error_km_s"]
                window_scale = values["window_scale"]
                diagnostics[bound.section] = LayerDiagnostics(
                    projection_max_error=(
                        None if projection_error is None else float(projection_error)
                    ),
                    window_scale=1.0 if window_scale is None else float(window_scale),
                )
            if values["moho_contrast_km_s"] is not None:
                moho_contrast = float(values["moho_contrast_km_s"])
    return (
        tuple(crust),
        tuple(mantle),
        moho_contrast,
        diagnostics.get(CRUST, LayerDiagnostics()),
        diagnostics.get(MANTLE, LayerDiagnostics()),
    )


def load_point_plot_data(
    point_dir: str | Path,
) -> tuple[InversionPoint, PointPriorBounds, DispersionCurve]:
    """Reconstruct plot inputs from a written point directory.

    Reads point.json, prior_bounds.csv and phase.input only, so no source grids
    or configuration are required.

    Args:
        point_dir: Directory holding one written inversion point.

    Returns:
        The reconstructed point, its final bounds and its dispersion curve.
    """

    directory = Path(point_dir)
    meta = json.loads((directory / "point.json").read_text(encoding="utf-8"))
    profile = VsProfile(
        lon=meta["lon"],
        lat=meta["lat"],
        depth=np.asarray(meta["reference_depth_km"], dtype=float),
        vs=np.asarray(meta["reference_vs_km_s"], dtype=float),
    )
    point = InversionPoint(
        lon=float(meta["lon"]),
        lat=float(meta["lat"]),
        water_depth=float(meta["water_depth"]),
        sediment_thickness=float(meta["sediment_thickness"]),
        moho_depth=float(meta["moho_depth"]),
        max_depth=float(meta["max_depth"]),
        water_threshold=float(meta["water_threshold"]),
        sediment_threshold=float(meta["sediment_threshold"]),
        smooth_on=int(meta["smooth_on"]),
        ice_on=int(meta["ice_on"]),
        vs_profile=profile,
    )
    crust, mantle, moho_contrast, crust_diag, mantle_diag = read_prior_bounds(
        directory / "prior_bounds.csv"
    )
    bounds = PointPriorBounds(
        sediment=tuple((float(lo), float(hi)) for lo, hi in meta["sediment_bounds"]),
        crust=crust,
        mantle=mantle,
        water_bottom=float(point.water_depth) if point.water_on else 0.0,
        sediment_bottom=float(point.sediment_thickness) if point.sediment_on else 0.0,
        moho_depth=float(point.moho_depth),
        max_depth=float(point.max_depth),
        factor=float(meta["factor"]),
        moho_contrast=moho_contrast,
        crust_diagnostics=crust_diag,
        mantle_diagnostics=mantle_diag,
    )
    curve = read_phase_input(directory / "phase.input")
    return point, bounds, curve
