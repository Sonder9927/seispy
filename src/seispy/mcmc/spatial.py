"""Spatial grids and phase-dispersion data."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Tuple

import numpy as np
import pandas as pd
import xarray as xr

from seispy.mcmc.configuration import Config

VsProfile = Tuple[np.ndarray, np.ndarray]


@dataclass(frozen=True)
class PhaseCurve:
    periods: np.ndarray
    velocities: np.ndarray
    sigmas: np.ndarray

    def __len__(self) -> int:
        return len(self.periods)

    def rows(self) -> Iterable[tuple[float, float, float]]:
        return zip(self.periods, self.velocities, self.sigmas, strict=True)

    def valid_rows(self, default_sigma: float) -> list[tuple[float, float, float]]:
        """Return only rows that can be safely written to phase.input.

        Invalid periods or velocities are skipped.  Invalid sigmas are replaced
        by ``default_sigma`` so that phase.input never contains NaN/Inf values.
        """

        if not np.isfinite(default_sigma) or default_sigma <= 0:
            raise ValueError(
                f"default_sigma must be positive and finite, got {default_sigma}"
            )

        valid: list[tuple[float, float, float]] = []
        for period, velocity, sigma in self.rows():
            if not np.isfinite(period) or not np.isfinite(velocity):
                continue
            if not np.isfinite(sigma) or sigma <= 0:
                sigma = default_sigma
            valid.append((float(period), float(velocity), float(sigma)))
        return valid


@dataclass(frozen=True)
class GridData:
    lon: np.ndarray
    lat: np.ndarray
    topo: np.ndarray
    sediment: np.ndarray
    moho: np.ndarray

    @property
    def shape(self) -> tuple[int, int]:
        return self.topo.shape

    @property
    def size(self) -> int:
        return self.topo.size

    def flat_values(self):
        return zip(
            self.lon.ravel(),
            self.lat.ravel(),
            self.topo.ravel(),
            self.sediment.ravel(),
            self.moho.ravel(),
            strict=True,
        )


@dataclass(frozen=True)
class PhaseCube:
    periods: np.ndarray
    velocities: np.ndarray  # shape: (n_period, ny, nx)
    sigmas: np.ndarray  # shape: (n_period, ny, nx)

    def curve_at_flat_index(self, k: int) -> PhaseCurve:
        return PhaseCurve(
            periods=self.periods.copy(),
            velocities=self.velocities.reshape(len(self.periods), -1)[:, k].copy(),
            sigmas=self.sigmas.reshape(len(self.periods), -1)[:, k].copy(),
        )


# =========================
# GRID AND INTERPOLATION
# =========================


def grid_coordinates(
    region: list[float], spacing: float, shape: tuple[int, int]
) -> tuple[np.ndarray, np.ndarray]:
    xmin, xmax, ymin, ymax = region
    ny, nx = shape
    lon_values = np.linspace(xmin, xmax, nx)
    lat_values = np.linspace(ymin, ymax, ny)
    return np.meshgrid(lon_values, lat_values)


def surface_grid(xyz, region, spacing, method: str = "surface"):
    """Interpolate xyz values onto a regular grid with PyGMT.

    Parameters
    ----------
    method
        ``surface`` uses GMT surface after blockmean. ``xyz2grd`` directly
        grids the block-averaged values. ``auto`` uses surface unless the grid
        is too small.
    """

    import pygmt

    xyz_block = pygmt.blockmean(data=xyz, region=region, spacing=spacing)

    xmin, xmax, ymin, ymax = region
    nx = int((xmax - xmin) / spacing + 1)
    ny = int((ymax - ymin) / spacing + 1)

    if method not in {"surface", "xyz2grd", "auto"}:
        raise ValueError("method must be 'surface', 'xyz2grd', or 'auto'")

    use_xyz2grd = method == "xyz2grd" or (method == "auto" and (nx < 4 or ny < 4))
    if use_xyz2grd:
        return pygmt.xyz2grd(data=xyz_block, region=region, spacing=spacing)

    if nx < 4 or ny < 4:
        print(f"[WARN] small grid ({nx}x{ny}), fallback xyz2grd")
        return pygmt.xyz2grd(data=xyz_block, region=region, spacing=spacing)

    return pygmt.surface(data=xyz_block, region=region, spacing=spacing, tension=0.35)


def load_etopo_xyz(path: str | Path, region: list[float]) -> np.ndarray:
    ds = xr.open_dataset(path)
    da = ds["z"].sel(lon=slice(region[0], region[1]), lat=slice(region[2], region[3]))
    df = da.to_dataframe(name="z").reset_index()
    return df[["lon", "lat", "z"]].values


def build_spatial_grid(cfg: Config) -> GridData:
    region = cfg.region
    spacing = cfg.grid_spacing

    topo = surface_grid(
        load_etopo_xyz(cfg.paths.etopo_nc, region), region, spacing, method="surface"
    ).values
    sediment = surface_grid(
        np.loadtxt(cfg.paths.sed_xyz), region, spacing, method="surface"
    ).values
    moho = surface_grid(
        np.loadtxt(cfg.paths.moho_xyz), region, spacing, method="surface"
    ).values

    lon, lat = grid_coordinates(region, spacing, topo.shape)
    return GridData(lon=lon, lat=lat, topo=topo, sediment=sediment, moho=moho)


# =========================
# DISPERSION
# =========================


def _pick_column(df: pd.DataFrame, candidates: tuple[str, ...], name: str) -> str:
    for col in candidates:
        if col in df.columns:
            return col
    raise ValueError(
        f"phase_dispersion_csv missing {name} column. "
        f"Expected one of {candidates}, got {list(df.columns)}"
    )


def read_phase_dispersion_csv(path: str | Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    lon_col = _pick_column(df, ("lon", "longitude", "x"), "longitude")
    lat_col = _pick_column(df, ("lat", "latitude", "y"), "latitude")
    period_col = _pick_column(df, ("period", "T"), "period")
    phv_col = _pick_column(
        df, ("phv", "phase_velocity", "c", "vel", "velocity"), "phase velocity"
    )
    std_col = next(
        (col for col in ("std", "sigma", "uncertainty", "error") if col in df.columns),
        None,
    )

    columns = [lon_col, lat_col, period_col, phv_col] + ([std_col] if std_col else [])
    out = df[columns].copy()
    rename = {lon_col: "lon", lat_col: "lat", period_col: "period", phv_col: "phv"}
    if std_col:
        rename[std_col] = "std"
    out = out.rename(columns=rename)

    for col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["lon", "lat", "period", "phv"])

    if "std" not in out.columns:
        out["std"] = np.nan
    if out.empty:
        raise ValueError(f"No valid phase-dispersion data found in {path}")
    return out


def _coord_key(value: float, ndigits: int = 6) -> float:
    """Stable key for matching CSV coordinates to the inversion grid."""

    return round(float(value), ndigits)


def build_phase_cube(cfg: Config, grid_data: GridData) -> PhaseCube:
    """Build a PhaseCube by direct lon-lat-period mapping, without interpolation.

    The merged phase_dispersion.csv is treated as the final dispersion data
    source.  A phase value is assigned to a grid point only when the CSV
    contains the same lon/lat coordinate.  Missing coordinates remain NaN and
    will not be written to phase.input.

    The CSV std column is optional.  If present, it is scaled by
    cfg.phase_sigma_scale.  If absent or invalid for a valid phase velocity,
    the value remains NaN here and is replaced by cfg.default_phase_sigma only
    when phase.input is written.
    """

    csv_path = cfg.paths.phase_dispersion_csv
    if csv_path is None:
        raise ValueError(
            "paths.phase_dispersion_csv is required. "
            "Provide the merged phase_dispersion.csv in config.json."
        )

    df = read_phase_dispersion_csv(csv_path)
    periods = np.asarray(sorted(df["period"].dropna().unique()), dtype=float)
    if len(periods) == 0:
        raise ValueError(f"No valid periods found in {csv_path}")

    ny, nx = grid_data.shape
    velocities = np.full((len(periods), ny, nx), np.nan, dtype=float)
    sigmas = np.full((len(periods), ny, nx), np.nan, dtype=float)

    lon_values = np.asarray(grid_data.lon[0, :], dtype=float)
    lat_values = np.asarray(grid_data.lat[:, 0], dtype=float)
    lon_to_ix = {_coord_key(lon): ix for ix, lon in enumerate(lon_values)}
    lat_to_iy = {_coord_key(lat): iy for iy, lat in enumerate(lat_values)}
    period_to_ip = {_coord_key(period): ip for ip, period in enumerate(periods)}

    data = df.copy()
    data["lon_key"] = data["lon"].map(_coord_key)
    data["lat_key"] = data["lat"].map(_coord_key)
    data["period_key"] = data["period"].map(_coord_key)

    data = data[
        data["lon_key"].isin(lon_to_ix)
        & data["lat_key"].isin(lat_to_iy)
        & data["period_key"].isin(period_to_ip)
    ].copy()

    if data.empty:
        raise ValueError(
            "No phase-dispersion rows match the inversion grid coordinates. "
            "Check region/grid_spacing and phase_dispersion.csv lon/lat values."
        )

    # If duplicate rows exist at the same lon-lat-period, average them.
    grouped = data.groupby(["period_key", "lat_key", "lon_key"], as_index=False).agg(
        phv=("phv", "mean"), std=("std", "mean")
    )

    sigma_scale = float(cfg.phase_sigma_scale)
    if not np.isfinite(sigma_scale) or sigma_scale <= 0:
        raise ValueError(
            f"phase_sigma_scale must be positive and finite, got {sigma_scale}"
        )

    for row in grouped.itertuples(index=False):
        ip = period_to_ip[row.period_key]
        iy = lat_to_iy[row.lat_key]
        ix = lon_to_ix[row.lon_key]

        if np.isfinite(row.phv):
            velocities[ip, iy, ix] = float(row.phv)

        if np.isfinite(row.std) and row.std > 0:
            sigmas[ip, iy, ix] = float(row.std) * sigma_scale

    print(
        f"Loaded phase dispersion directly from {csv_path}: "
        f"{len(periods)} periods, {len(grouped)} lon-lat-period records mapped."
    )

    return PhaseCube(periods=periods, velocities=velocities, sigmas=sigmas)
