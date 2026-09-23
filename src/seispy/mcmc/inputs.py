"""Shared readers and column-name resolution for MCMC input products."""

from pathlib import Path
from typing import Iterable

import pandas as pd

LON_NAMES = ("x", "lon", "longitude")
LAT_NAMES = ("y", "lat", "latitude")
DEPTH_NAMES = ("z", "depth")
VS_NAMES = ("vs", "vsv", "shear_velocity", "shear_vel", "velocity")
PERIOD_NAMES = ("period", "periods", "T")
PHASE_VELOCITY_NAMES = (
    "phv",
    "phase_velocity",
    "phase_vel",
    "c",
    "vel",
    "velocity",
)
SIGMA_NAMES = ("std", "sigma", "uncertainty", "error")
NETCDF_SUFFIXES = {".nc", ".nc4", ".netcdf"}
COORD_NDIGITS = 6


def pick_name(available: Iterable[str], candidates: tuple[str, ...]) -> str | None:
    """Return the first known alias present in ``available``."""

    names = set(available)
    return next((candidate for candidate in candidates if candidate in names), None)


def coordinate_key(value: float, ndigits: int = COORD_NDIGITS) -> float:
    """Return the canonical key used for grid-coordinate matching."""

    return round(float(value), ndigits)


def coordinate_pair_key(
    lon: float,
    lat: float,
    ndigits: int = COORD_NDIGITS,
) -> tuple[float, float]:
    """Return the canonical key for one longitude/latitude pair."""

    return coordinate_key(lon, ndigits), coordinate_key(lat, ndigits)


def read_table(path: str | Path) -> pd.DataFrame:
    """Read a CSV or Parquet table without imposing column names."""

    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Expected CSV or Parquet input, got: {path}")


def netcdf_variable_frame(
    path: str | Path,
    value_names: tuple[str, ...],
    *,
    name: str,
) -> pd.DataFrame:
    """Read one NetCDF variable and expose its coordinates as columns."""

    import xarray as xr

    path = Path(path)
    with xr.open_dataset(path) as ds:
        value_name = pick_name(ds.data_vars, value_names)
        if value_name is None:
            raise ValueError(
                f"{name}: cannot determine NetCDF value variable. "
                f"Preferred names={value_names}, data variables={list(ds.data_vars)}"
            )
        return ds[value_name].to_dataframe(name=value_name).reset_index()
