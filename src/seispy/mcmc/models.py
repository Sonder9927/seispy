"""Reference velocity models and MCMC parameterization."""

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from seispy.mcmc.configuration import Config

_COORD_NDIGITS = 6


def _coord_key(lon: float, lat: float) -> tuple[float, float]:
    """Return a stable exact-match key for regular geographic grids."""

    return (round(float(lon), _COORD_NDIGITS), round(float(lat), _COORD_NDIGITS))


@dataclass(frozen=True)
class VsProfile:
    """One reference shear-wave velocity profile at a fixed lon/lat point.

    Depth is positive downward in km and Vs is in km/s.
    """

    lon: float
    lat: float
    depth: np.ndarray
    vs: np.ndarray

    def validate(self) -> None:
        lon = float(self.lon)
        lat = float(self.lat)
        depth = np.asarray(self.depth, dtype=float)
        vs = np.asarray(self.vs, dtype=float)

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
        return float(np.asarray(self.depth, dtype=float)[0])

    @property
    def max_depth(self) -> float:
        return float(np.asarray(self.depth, dtype=float)[-1])


@dataclass
class VsModelLibrary:
    """Collection of reference Vs profiles indexed by exact lon/lat coordinates.

    The reference model may be stored as CSV or Parquet with columns
    ``x, y, z, vs``. ``z`` is positive depth in km and ``vs`` is km/s.
    """

    profiles: dict[tuple[float, float], VsProfile]

    @classmethod
    def from_file(cls, path: str | Path) -> "VsModelLibrary":
        path = Path(path)
        suffix = path.suffix.lower()

        if suffix == ".parquet":
            df = pd.read_parquet(path)
        elif suffix == ".csv":
            df = pd.read_csv(path)
        else:
            raise ValueError(
                "Reference Vs model must be a .csv or .parquet file, "
                f"got: {path.suffix or '<no suffix>'}"
            )

        required = {"x", "y", "z", "vs"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Reference Vs model missing columns: {sorted(missing)}")

        data = df[["x", "y", "z", "vs"]].copy()
        for col in ("x", "y", "z", "vs"):
            data[col] = pd.to_numeric(data[col], errors="coerce")

        if data[["x", "y", "z", "vs"]].isna().any().any():
            counts = data[["x", "y", "z", "vs"]].isna().sum()
            raise ValueError(
                "Reference Vs model contains NaN/non-numeric values: "
                + ", ".join(f"{col}={int(n)}" for col, n in counts.items() if n)
            )

        if not np.isfinite(data[["x", "y", "z", "vs"]].to_numpy(dtype=float)).all():
            raise ValueError("Reference Vs model contains non-finite values")

        if (data["z"] < 0).any():
            zmin = float(data["z"].min())
            raise ValueError(
                "Reference Vs model must use positive depth (z >= 0 km); "
                f"found minimum z={zmin:.3f} km"
            )

        duplicated = data.duplicated(subset=["x", "y", "z"], keep=False)
        if duplicated.any():
            example = data.loc[duplicated, ["x", "y", "z"]].head(5).to_dict("records")
            raise ValueError(
                f"Reference Vs model contains duplicate (x, y, z) rows; examples: {example}"
            )

        profiles: dict[tuple[float, float], VsProfile] = {}
        for (x, y), group in data.groupby(["x", "y"], sort=False):
            g = group.sort_values("z")
            profile = VsProfile(
                lon=float(x),
                lat=float(y),
                depth=g["z"].to_numpy(dtype=float),
                vs=g["vs"].to_numpy(dtype=float),
            )
            profile.validate()

            key = _coord_key(profile.lon, profile.lat)
            if key in profiles:
                raise ValueError(
                    f"Coordinate-key collision after rounding at ({profile.lon}, {profile.lat})"
                )
            profiles[key] = profile

        if not profiles:
            raise ValueError(f"No valid Vs profiles found in {path}")

        return cls(profiles=profiles)

    def profile_at(self, lon: float, lat: float) -> VsProfile:
        """Return the reference profile at an exact inversion-grid coordinate."""

        key = _coord_key(lon, lat)
        try:
            return self.profiles[key]
        except KeyError as exc:
            raise KeyError(
                "Reference Vs model has no profile at inversion-grid coordinate "
                f"({float(lon):.{_COORD_NDIGITS}f}, {float(lat):.{_COORD_NDIGITS}f})"
            ) from exc


def velocity_at_depths(profile: VsProfile, depths: np.ndarray) -> np.ndarray:
    """Linearly interpolate Vs at requested depths without extrapolation."""

    profile.validate()
    z = np.asarray(profile.depth, dtype=float)
    vs = np.asarray(profile.vs, dtype=float)
    targets = np.asarray(depths, dtype=float)

    if not np.isfinite(targets).all():
        raise ValueError("Requested interpolation depths contain non-finite values")

    if np.any(targets < z[0]) or np.any(targets > z[-1]):
        raise ValueError(
            "Requested Vs interpolation depths exceed reference profile coverage: "
            f"requested {targets.min():.3f}–{targets.max():.3f} km, "
            f"profile covers {z[0]:.3f}–{z[-1]:.3f} km at "
            f"({profile.lon:.6f}, {profile.lat:.6f})"
        )

    return np.interp(targets, z, vs)


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
        scalar_values = {
            "lon": self.lon,
            "lat": self.lat,
            "water_depth": self.water_depth,
            "sediment_thickness": self.sediment_thickness,
            "moho_depth": self.moho_depth,
            "max_depth": self.max_depth,
            "water_threshold": self.water_threshold,
            "sediment_threshold": self.sediment_threshold,
        }
        for name, value in scalar_values.items():
            if not np.isfinite(float(value)):
                raise ValueError(
                    f"{self.folder_name}: {name} must be finite, got {value}"
                )

        if not -90.0 <= self.lat <= 90.0:
            raise ValueError(f"{self.folder_name}: invalid latitude {self.lat}")
        if self.water_depth < 0:
            raise ValueError(f"{self.folder_name}: water_depth must be >= 0 km")
        if self.sediment_thickness < 0:
            raise ValueError(f"{self.folder_name}: sediment_thickness must be >= 0 km")
        if self.moho_depth <= 0:
            raise ValueError(f"{self.folder_name}: moho_depth must be > 0 km")
        if self.max_depth <= 0:
            raise ValueError(f"{self.folder_name}: max_depth must be > 0 km")
        if self.water_threshold < 0 or self.sediment_threshold < 0:
            raise ValueError(
                f"{self.folder_name}: water/sediment thresholds must be non-negative"
            )
        if self.water_depth >= self.moho_depth:
            raise ValueError(
                f"{self.folder_name}: water depth ({self.water_depth:.3f} km) must be "
                f"shallower than Moho ({self.moho_depth:.3f} km)"
            )
        if self.sediment_thickness >= self.moho_depth:
            raise ValueError(
                f"{self.folder_name}: sediment thickness ({self.sediment_thickness:.3f} km) "
                f"must be smaller than Moho depth ({self.moho_depth:.3f} km)"
            )
        if self.water_on and self.sediment_on:
            raise ValueError("water_on and sediment_on must not both be 1")
        if self.moho_depth <= self.crustal_spline_top:
            raise ValueError(
                f"{self.folder_name}: Moho depth ({self.moho_depth}) must be deeper than "
                f"crustal spline top ({self.crustal_spline_top})"
            )
        if self.max_depth <= self.moho_depth:
            raise ValueError(
                f"{self.folder_name}: max depth ({self.max_depth}) must be deeper than "
                f"Moho ({self.moho_depth})"
            )

        self.vs_profile.validate()
        profile_key = _coord_key(self.vs_profile.lon, self.vs_profile.lat)
        grid_key = _coord_key(self.lon, self.lat)
        if profile_key != grid_key:
            raise ValueError(
                f"{self.folder_name}: reference Vs profile coordinate "
                f"({self.vs_profile.lon:.6f}, {self.vs_profile.lat:.6f}) does not match "
                f"grid coordinate ({self.lon:.6f}, {self.lat:.6f})"
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
    """Construct one MCMC grid point from standardized source values.

    ``topo`` is elevation in meters (negative below sea level), while
    ``sediment`` and ``moho`` are positive thickness/depth values in km.
    """

    lon = float(lon)
    lat = float(lat)
    topo = float(topo)
    sediment = float(sediment)
    moho = float(moho)

    raw_values = {
        "lon": lon,
        "lat": lat,
        "topo": topo,
        "sediment": sediment,
        "moho": moho,
    }
    for name, value in raw_values.items():
        if not np.isfinite(value):
            raise ValueError(f"Grid source value {name} must be finite, got {value}")

    if sediment < 0:
        raise ValueError(
            f"Sediment thickness must be positive in km, got {sediment} at ({lon}, {lat})"
        )
    if moho <= 0:
        raise ValueError(
            f"Moho must be positive depth in km, got {moho} at ({lon}, {lat})"
        )

    grid = MCMCGrid(
        lon=lon,
        lat=lat,
        smooth_on=cfg.sm_on,
        ice_on=cfg.ice_on,
        water_depth=max(0.0, -topo / 1000.0),
        sediment_thickness=sediment,
        moho_depth=moho,
        max_depth=float(cfg.zmax_Bs),
        water_threshold=float(cfg.water_threshold),
        sediment_threshold=float(cfg.sediment_threshold),
        vs_profile=vs_profile,
    )
    grid.validate()
    return grid
