"""Phase-dispersion products aligned to the inversion grid.

A dispersion product is a regular lon/lat grid with one row per
``(lon, lat, period)`` or a NetCDF ``(period, lat, lon)`` cube. Periods are
shared by every inversion point, so the aligned product is stored once as a
``(period, y, x)`` cube and sliced per grid point.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from seispy.mcmc.config import Config
from seispy.mcmc.gridding import TargetGrid, align_to_grid, long_to_grid
from seispy.mcmc.inputs import (
    LAT_NAMES,
    LON_NAMES,
    NETCDF_SUFFIXES,
    PERIOD_NAMES,
    PHASE_VELOCITY_NAMES,
    SIGMA_NAMES,
    pick_name,
    read_table,
)


@dataclass(frozen=True)
class DispersionCurve:
    """Observed phase velocity and one-sigma uncertainty against period."""

    periods: np.ndarray
    velocities: np.ndarray
    sigmas: np.ndarray

    def __len__(self) -> int:
        return len(self.periods)

    def valid_rows(self, default_sigma: float) -> list[tuple[float, float, float]]:
        """Return rows that can be safely written to ``phase.input``.

        A row with a non-finite period or velocity is dropped. A non-finite or
        non-positive sigma is replaced by ``default_sigma``.
        """

        if not np.isfinite(default_sigma) or default_sigma <= 0:
            raise ValueError(
                f"default_sigma must be positive and finite, got {default_sigma}"
            )

        valid: list[tuple[float, float, float]] = []
        for period, velocity, sigma in zip(
            self.periods, self.velocities, self.sigmas, strict=True
        ):
            if not np.isfinite(period) or not np.isfinite(velocity):
                continue
            if not np.isfinite(sigma) or sigma <= 0:
                sigma = default_sigma
            valid.append((float(period), float(velocity), float(sigma)))
        return valid


@dataclass(frozen=True, eq=False)
class DispersionGrid:
    """Aligned dispersion cube with shape ``(n_period, ny, nx)``."""

    periods: np.ndarray
    velocities: np.ndarray
    sigmas: np.ndarray

    def __post_init__(self) -> None:
        object.__setattr__(self, "periods", np.asarray(self.periods, dtype=float))
        object.__setattr__(self, "velocities", np.asarray(self.velocities, dtype=float))
        object.__setattr__(self, "sigmas", np.asarray(self.sigmas, dtype=float))

    @property
    def n_periods(self) -> int:
        return self.periods.size

    def curve_at(self, k: int) -> DispersionCurve:
        """Return the curve at row-major flattened grid index ``k``."""

        if not 0 <= k < self.velocities[0].size:
            raise IndexError(
                f"grid index {k} is outside [0, {self.velocities[0].size})"
            )
        return DispersionCurve(
            periods=self.periods.copy(),
            velocities=self.velocities.reshape(self.n_periods, -1)[:, k].copy(),
            sigmas=self.sigmas.reshape(self.n_periods, -1)[:, k].copy(),
        )


# =========================
# DISPERSION INPUT
# =========================


def _pick_column(df: pd.DataFrame, candidates: tuple[str, ...], name: str) -> str:
    column = pick_name(df.columns, candidates)
    if column is not None:
        return column
    raise ValueError(
        f"phase dispersion input missing {name} column. "
        f"Expected one of {candidates}, got {list(df.columns)}"
    )


def _read_netcdf_phase_dispersion(path: Path) -> pd.DataFrame:
    """Read phase velocity and optional uncertainty from a NetCDF dataset."""

    with xr.open_dataset(path) as ds:
        coordinate_names = set(ds.coords) | set(ds.dims)
        lon_col = pick_name(coordinate_names, LON_NAMES)
        lat_col = pick_name(coordinate_names, LAT_NAMES)
        period_col = pick_name(coordinate_names, PERIOD_NAMES)
        phv_name = pick_name(ds.data_vars, PHASE_VELOCITY_NAMES)
        std_name = pick_name(ds.data_vars, SIGMA_NAMES)

        missing = []
        if lon_col is None:
            missing.append("longitude")
        if lat_col is None:
            missing.append("latitude")
        if period_col is None:
            missing.append("period")
        if phv_name is None:
            missing.append("phase velocity")
        if missing:
            raise ValueError(
                f"phase dispersion NetCDF input missing {', '.join(missing)}; "
                f"coordinates={list(coordinate_names)}, "
                f"data variables={list(ds.data_vars)}"
            )

        phv = ds[phv_name]
        required_dims = {lon_col, lat_col, period_col}
        if not required_dims.issubset(phv.dims):
            raise ValueError(
                f"phase velocity variable {phv_name!r} must depend on "
                f"{sorted(required_dims)}, got dims={phv.dims}"
            )

        frame = phv.to_dataframe(name="phv").reset_index()
        if std_name is not None:
            sigma = ds[std_name]
            if not required_dims.issubset(sigma.dims):
                raise ValueError(
                    f"phase uncertainty variable {std_name!r} must depend on "
                    f"{sorted(required_dims)}, got dims={sigma.dims}"
                )
            sigma_frame = sigma.to_dataframe(name="std").reset_index()
            frame = frame.merge(
                sigma_frame[[lon_col, lat_col, period_col, "std"]],
                on=[lon_col, lat_col, period_col],
                how="left",
                validate="one_to_one",
            )
        else:
            frame["std"] = np.nan

        return frame.rename(
            columns={lon_col: "lon", lat_col: "lat", period_col: "period"}
        )[["lon", "lat", "period", "phv", "std"]]


def read_dispersion_data(path: str | Path) -> pd.DataFrame:
    """Read a dispersion table with canonical ``lon/lat/period/phv/std`` columns."""

    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in {".csv", ".parquet"}:
        df = read_table(path)
    elif suffix in NETCDF_SUFFIXES:
        df = _read_netcdf_phase_dispersion(path)
    else:
        raise ValueError(
            f"Phase dispersion file must be CSV, NetCDF, or Parquet, got: {path}"
        )

    lon_col = _pick_column(df, LON_NAMES, "longitude")
    lat_col = _pick_column(df, LAT_NAMES, "latitude")
    period_col = _pick_column(df, PERIOD_NAMES, "period")
    phv_col = _pick_column(df, PHASE_VELOCITY_NAMES, "phase velocity")
    std_col = pick_name(df.columns, SIGMA_NAMES)

    columns = [lon_col, lat_col, period_col, phv_col]
    if std_col:
        columns.append(std_col)
    out = df[columns].rename(
        columns={
            lon_col: "lon",
            lat_col: "lat",
            period_col: "period",
            phv_col: "phv",
            **({std_col: "std"} if std_col else {}),
        }
    )

    for col in out.columns:
        out[col] = pd.to_numeric(out[col], errors="coerce")
    out = out.dropna(subset=["lon", "lat", "period", "phv"])
    if out.empty:
        raise ValueError(f"No valid phase-dispersion data found in {path}")
    if "std" not in out.columns:
        out["std"] = np.nan
    return out


def build_dispersion_grid(cfg: Config, target: TargetGrid) -> DispersionGrid:
    """Align phase dispersion to ``target`` and return a period cube.

    The optional std column is converted to km/s using
    ``cfg.input_units.phase_std`` and used only when finite and positive;
    otherwise it stays NaN and is replaced by ``cfg.default_phase_std`` only
    when ``phase.input`` is written. Cells without source coverage remain NaN.
    """

    phase_file = cfg.paths.phase_dispersion_file
    df = read_dispersion_data(phase_file)
    df["phv"] *= cfg.input_units.phase_velocity_to_km_s

    phv_cube = align_to_grid(
        long_to_grid(df, "phv", "period", extra_name="period"),
        target,
        on_missing="nan",
    )
    std_cube = align_to_grid(
        long_to_grid(df, "std", "period", extra_name="period"),
        target,
        on_missing="nan",
    )

    periods = np.asarray(phv_cube["period"].values, dtype=float)
    velocities = np.asarray(phv_cube.values, dtype=float)
    if velocities.shape[1:] != target.shape:
        raise ValueError(
            "Aligned phase dispersion does not match the inversion grid: "
            f"{velocities.shape[1:]} != {target.shape}"
        )

    sigmas = (
        np.asarray(std_cube.values, dtype=float) * cfg.input_units.phase_std_to_km_s
    )
    sigmas = np.where(np.isfinite(sigmas) & (sigmas > 0), sigmas, np.nan)

    print(
        f"Aligned phase dispersion from {phase_file}: "
        f"{periods.size} periods, {int(np.isfinite(velocities).sum())} finite records."
    )
    return DispersionGrid(periods=periods, velocities=velocities, sigmas=sigmas)
