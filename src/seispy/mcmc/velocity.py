"""Reference shear-velocity models and interpolation.

A reference model is a regular ``(depth, y, x)`` cube of Vs samples. Profiles
are aligned to the inversion grid, then interpolated at the Greville depths used
by the prior constructor. Depth is always positive downward in km and Vs is in
km/s.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from seispy.mcmc.gridding import TargetGrid, align_to_grid, long_to_grid
from seispy.mcmc.inputs import (
    COORD_NDIGITS,
    DEPTH_NAMES,
    LAT_NAMES,
    LON_NAMES,
    NETCDF_SUFFIXES,
    VS_NAMES,
    coordinate_pair_key,
    netcdf_variable_frame,
    pick_name,
    read_table,
)


@dataclass(frozen=True, eq=False)
class VsProfile:
    """One reference shear-wave velocity profile at a fixed lon/lat point."""

    lon: float
    lat: float
    depth: np.ndarray
    vs: np.ndarray

    def __post_init__(self) -> None:
        object.__setattr__(self, "depth", np.asarray(self.depth, dtype=float))
        object.__setattr__(self, "vs", np.asarray(self.vs, dtype=float))
        self.validate()

    def validate(self) -> None:
        lon = float(self.lon)
        lat = float(self.lat)
        depth = self.depth
        vs = self.vs

        if not np.isfinite(lon) or not np.isfinite(lat):
            raise ValueError("Vs profile coordinates must be finite")
        if not -90.0 <= lat <= 90.0:
            raise ValueError(f"Invalid Vs profile latitude: {lat}")
        if depth.ndim != 1 or vs.ndim != 1 or depth.size != vs.size:
            raise ValueError(
                "Vs profile depth and velocity must be equal-length 1-D arrays"
            )
        if depth.size < 2:
            raise ValueError("Vs profile must contain at least two depth samples")
        if not np.isfinite(depth).all() or not np.isfinite(vs).all():
            raise ValueError("Vs profile contains non-finite depth or velocity values")
        if np.any(depth < 0):
            raise ValueError("Vs profile depth must be positive downward (z >= 0 km)")
        if np.any(np.diff(depth) <= 0):
            raise ValueError("Vs profile depths must be strictly increasing")

    @property
    def min_depth(self) -> float:
        return float(self.depth[0])

    @property
    def max_depth(self) -> float:
        return float(self.depth[-1])


@dataclass(frozen=True, eq=False)
class VsModelLibrary:
    """Reference Vs profiles indexed by exact inversion-grid lon/lat coordinates.

    Reference models may be stored as CSV, Parquet, or NetCDF. Coordinate
    aliases ``x/lon/longitude``, ``y/lat/latitude``, and ``z/depth`` are
    accepted; the Vs variable may be named ``vs``, ``vsv``, or another alias
    defined by :mod:`seispy.mcmc.inputs`.
    """

    profiles: dict[tuple[float, float], VsProfile]

    @classmethod
    def from_file(
        cls,
        path: str | Path,
        *,
        target: TargetGrid,
        vs_scale: float = 1.0,
    ) -> "VsModelLibrary":
        """Load and align reference Vs profiles, scaling Vs into km/s.

        The source must be a regular lon/lat/depth grid. Its horizontal grid is
        aligned to the inversion grid with the shared three-case strategy
        (direct copy when equal and aligned, interpolation when coarser,
        conservative averaging when finer) before profiles are built.
        """

        if not np.isfinite(vs_scale) or vs_scale <= 0:
            raise ValueError(f"vs_scale must be finite and > 0, got {vs_scale}")

        path = Path(path)
        suffix = path.suffix.lower()
        if suffix in {".csv", ".parquet"}:
            df = read_table(path)
        elif suffix in NETCDF_SUFFIXES:
            df = netcdf_variable_frame(path, VS_NAMES, name="reference Vs model")
        else:
            raise ValueError(
                "Reference Vs model must be a CSV, NetCDF, or Parquet file, "
                f"got: {path.suffix or '<no suffix>'}"
            )

        data = _canonical_frame(df)
        data["vs"] *= float(vs_scale)

        aligned = align_to_grid(
            long_to_grid(data, "vs", "z", extra_name="z"),
            target,
            on_missing="nan",
        )

        depths = np.asarray(aligned["z"].values, dtype=float)
        profiles: dict[tuple[float, float], VsProfile] = {}
        for j, lat in enumerate(np.asarray(aligned["y"].values, dtype=float)):
            for i, lon in enumerate(np.asarray(aligned["x"].values, dtype=float)):
                column = np.asarray(aligned.values[:, j, i], dtype=float)
                valid = np.isfinite(column)
                if valid.sum() < 2:
                    continue
                profile = VsProfile(
                    lon=float(lon),
                    lat=float(lat),
                    depth=depths[valid].copy(),
                    vs=column[valid].copy(),
                )
                profiles[coordinate_pair_key(profile.lon, profile.lat)] = profile

        if not profiles:
            raise ValueError(
                "Reference Vs model has no finite profile on the inversion grid"
            )
        return cls(profiles=profiles)

    def profile_at(self, lon: float, lat: float) -> VsProfile:
        """Return the reference profile at an exact inversion-grid coordinate."""

        key = coordinate_pair_key(lon, lat)
        try:
            return self.profiles[key]
        except KeyError as exc:
            raise KeyError(
                "Reference Vs model has no profile at inversion-grid coordinate "
                f"({float(lon):.{COORD_NDIGITS}f}, {float(lat):.{COORD_NDIGITS}f})"
            ) from exc


def _canonical_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Select and validate the lon/lat/z/vs columns of a reference model."""

    lon_col = pick_name(df.columns, LON_NAMES)
    lat_col = pick_name(df.columns, LAT_NAMES)
    depth_col = pick_name(df.columns, DEPTH_NAMES)
    vs_col = pick_name(df.columns, VS_NAMES)
    missing = [
        name
        for name, column in (
            ("longitude", lon_col),
            ("latitude", lat_col),
            ("depth", depth_col),
            ("Vs", vs_col),
        )
        if column is None
    ]
    if missing:
        raise ValueError(
            f"Reference Vs model missing {', '.join(missing)} columns; "
            f"got {list(df.columns)}"
        )

    data = df[[lon_col, lat_col, depth_col, vs_col]].copy()
    data.columns = ["lon", "lat", "z", "vs"]
    for col in ("lon", "lat", "z", "vs"):
        data[col] = pd.to_numeric(data[col], errors="coerce")

    if data.isna().any().any():
        counts = data.isna().sum()
        raise ValueError(
            "Reference Vs model contains NaN/non-numeric values: "
            + ", ".join(f"{col}={int(n)}" for col, n in counts.items() if n)
        )
    if not np.isfinite(data.to_numpy(dtype=float)).all():
        raise ValueError("Reference Vs model contains non-finite values")
    if (data["z"] < 0).any():
        zmin = float(data["z"].min())
        raise ValueError(
            "Reference Vs model must use positive depth (z >= 0 km); "
            f"found minimum z={zmin:.3f} km"
        )

    duplicated = data.duplicated(subset=["lon", "lat", "z"], keep=False)
    if duplicated.any():
        example = data.loc[duplicated, ["lon", "lat", "z"]].head(5).to_dict("records")
        raise ValueError(
            "Reference Vs model contains duplicate (lon, lat, z) rows; "
            f"examples: {example}"
        )
    return data


def velocity_at_depths(
    profile: VsProfile,
    depths: np.ndarray,
    *,
    allow_shallow_extrapolation: bool = True,
    max_shallow_extrapolation_km: float | None = 5.0,
) -> np.ndarray:
    """Interpolate Vs at requested depths with optional shallow extrapolation.

    Targets below the first reference sample are estimated from the straight
    line through the two shallowest samples when shallow extrapolation is
    enabled. Targets deeper than the last reference sample are always rejected.
    ``max_shallow_extrapolation_km`` limits the largest shallow gap; ``None``
    means no explicit gap limit.
    """

    z = profile.depth
    vs = profile.vs
    targets = np.asarray(depths, dtype=float)

    if not np.isfinite(targets).all():
        raise ValueError("Requested interpolation depths contain non-finite values")
    if np.any(targets < 0):
        raise ValueError("Requested interpolation depths must be >= 0 km")

    if max_shallow_extrapolation_km is not None:
        max_gap: float | None = float(max_shallow_extrapolation_km)
        if not np.isfinite(max_gap) or max_gap < 0:
            raise ValueError(
                "max_shallow_extrapolation_km must be finite and >= 0, or None"
            )
    else:
        max_gap = None

    shallow = targets < z[0]
    if np.any(shallow):
        gap = float(z[0] - targets[shallow].min())
        if not allow_shallow_extrapolation:
            raise ValueError(
                "Requested Vs interpolation depths include a shallow gap: "
                f"profile starts at {z[0]:.3f} km, "
                f"requested {targets[shallow].min():.3f} km"
            )
        if max_gap is not None and gap > max_gap:
            raise ValueError(
                "Requested shallow Vs extrapolation exceeds the configured limit: "
                f"gap={gap:.3f} km, max={max_gap:.3f} km; profile starts at "
                f"{z[0]:.3f} km"
            )

    if np.any(targets > z[-1]):
        raise ValueError(
            "Requested Vs interpolation depths exceed reference profile coverage: "
            f"requested {targets.min():.3f}-{targets.max():.3f} km, "
            f"profile covers {z[0]:.3f}-{z[-1]:.3f} km at "
            f"({profile.lon:.6f}, {profile.lat:.6f})"
        )

    result = np.interp(np.maximum(targets, z[0]), z, vs)
    if np.any(shallow):
        slope = (vs[1] - vs[0]) / (z[1] - z[0])
        result[shallow] = vs[0] + slope * (targets[shallow] - z[0])
        if not np.isfinite(result[shallow]).all() or np.any(result[shallow] <= 0):
            raise ValueError(
                "Shallow Vs extrapolation produced non-positive or non-finite values"
            )
    return result
