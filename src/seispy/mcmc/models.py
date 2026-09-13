"""Reference velocity models and MCMC parameterization."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from seispy.mcmc.configuration import Config
from seispy.mcmc.spatial import VsProfile


@dataclass
class VsModelLibrary:
    points: np.ndarray
    profiles: list[VsProfile]
    _tree: Any = None

    @classmethod
    def from_csv(cls, path: str | Path) -> "VsModelLibrary":
        df = pd.read_csv(path)
        required = {"x", "y", "z", "vs"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"vs_model_csv missing columns: {sorted(missing)}")

        points: list[tuple[float, float]] = []
        profiles: list[VsProfile] = []

        for (x, y), group in df.groupby(["x", "y"], sort=False):
            g = group.sort_values("z")
            depth = np.abs(g["z"].to_numpy(dtype=float))
            velocity = g["vs"].to_numpy(dtype=float)
            valid = np.isfinite(depth) & np.isfinite(velocity)
            depth = depth[valid]
            velocity = velocity[valid]
            if len(depth) < 2:
                continue
            order = np.argsort(depth)
            points.append((float(x), float(y)))
            profiles.append((depth[order], velocity[order]))

        if not profiles:
            raise ValueError(f"No valid Vs profiles found in {path}")

        lib = cls(points=np.asarray(points, dtype=float), profiles=profiles)
        lib._build_tree()
        return lib

    def _build_tree(self) -> None:
        try:
            from scipy.spatial import cKDTree

            self._tree = cKDTree(self.points)
        except Exception:
            self._tree = None

    def nearest_profile(self, lon: float, lat: float) -> VsProfile:
        query = np.asarray([lon, lat], dtype=float)
        if self._tree is not None:
            _, idx = self._tree.query(query)
            return self.profiles[int(idx)]

        dist2 = np.sum((self.points - query) ** 2, axis=1)
        return self.profiles[int(np.argmin(dist2))]


def velocity_at_depths(
    profile: VsProfile,
    depths: np.ndarray,
    deep_extrapolation_gradient: float = 0.001,
) -> np.ndarray:
    model_depth, model_vs = profile
    valid = np.isfinite(model_depth) & np.isfinite(model_vs)
    if valid.sum() < 2:
        raise ValueError("Vs profile must contain at least two valid depth points")

    z = model_depth[valid]
    vs = model_vs[valid]
    order = np.argsort(z)
    z = z[order]
    vs = vs[order]

    values = np.interp(depths, z, vs, left=vs[0], right=vs[-1])

    deeper = depths > z[-1]
    if np.any(deeper):
        values[deeper] = vs[-1] + deep_extrapolation_gradient * (depths[deeper] - z[-1])

    return values


# =========================
# FORTRAN-COMPATIBLE B-SPLINE DEPTHS
# =========================


def fortran_knot_vector(
    n_basis: int, z_top: float, z_bottom: float, factor: float
) -> np.ndarray:
    """Reproduce the knot vector construction in the original Fortran B-spline code."""

    if n_basis < 2:
        raise ValueError("n_basis must be >= 2")
    if z_bottom <= z_top:
        raise ValueError(f"Invalid spline interval: {z_top} -> {z_bottom}")

    spline_order = n_basis - 1
    n_knots = n_basis + spline_order
    span = z_bottom - z_top
    knots = np.empty(n_knots, dtype=float)

    for i in range(1, spline_order + 1):
        knots[i - 1] = z_top + (i - 1) * span / 100000.0

    n_temp = n_knots - 2 * spline_order + 1
    if n_temp <= 0:
        raise ValueError("Invalid knot configuration")

    if factor != 1:
        step0 = span * (factor - 1.0) / (factor**n_temp - 1.0)
    else:
        step0 = span / n_temp

    for i in range(spline_order + 1, n_knots - spline_order + 1):
        knots[i - 1] = step0 * factor ** (i - spline_order - 1) + z_top

    for i in range(n_knots - spline_order + 1, n_knots + 1):
        knots[i - 1] = z_bottom - span / 100000.0 * (n_knots - i)

    return knots


def greville_depths(
    n_basis: int, z_top: float, z_bottom: float, factor: float
) -> np.ndarray:
    knots = fortran_knot_vector(n_basis, z_top, z_bottom, factor)
    spline_order = n_basis - 1
    return np.asarray(
        [np.mean(knots[j + 1 : j + spline_order + 1]) for j in range(n_basis)],
        dtype=float,
    )


# =========================
# MODEL PARAMETERIZATION
# =========================


@dataclass(frozen=True)
class MCMCGrid:
    lon: float
    lat: float
    water_depth: float
    sediment_thickness: float
    moho_depth: float
    max_depth: float
    water_threshold: float
    sediment_threshold: float
    smooth_on: int
    ice_on: int
    vs_profile: VsProfile

    @property
    def folder_name(self) -> str:
        return f"{self.lon:.2f}_{self.lat:.2f}"

    @property
    def water_on(self) -> int:
        return int(self.water_depth > self.water_threshold)

    @property
    def sediment_on(self) -> int:
        # Water and sediment are kept mutually exclusive for the Fortran input format.
        if self.water_on:
            return 0
        return int(self.sediment_thickness > self.sediment_threshold)

    @property
    def shallow_interface_depth(self) -> float:
        if self.water_on:
            return self.water_depth
        if self.sediment_on:
            return self.sediment_thickness
        return 0.0

    @property
    def crustal_spline_top(self) -> float:
        return self.shallow_interface_depth

    def validate(self) -> None:
        if self.water_on and self.sediment_on:
            raise ValueError("water_on and sediment_on must not both be 1")
        if self.moho_depth <= self.crustal_spline_top:
            raise ValueError(
                f"Moho depth ({self.moho_depth}) must be deeper than crustal spline top "
                f"({self.crustal_spline_top})"
            )
        if self.max_depth <= self.moho_depth:
            raise ValueError(
                f"Max depth ({self.max_depth}) must be deeper than Moho ({self.moho_depth})"
            )


def make_mcmc_grid(
    lon: float,
    lat: float,
    topo: float,
    sediment: float,
    moho: float,
    vs_profile: VsProfile,
    cfg: Config,
) -> MCMCGrid:
    return MCMCGrid(
        lon=float(lon),
        lat=float(lat),
        smooth_on=cfg.sm_on,
        ice_on=cfg.ice_on,
        water_depth=max(0.0, -float(topo) / 1000.0),
        sediment_thickness=max(0.0, float(sediment)),
        moho_depth=abs(float(moho)),
        max_depth=float(cfg.zmax_Bs),
        water_threshold=float(cfg.water_threshold),
        sediment_threshold=float(cfg.sediment_threshold),
        vs_profile=vs_profile,
    )
