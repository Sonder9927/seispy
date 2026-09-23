"""Spatial scalar fields (topography, sediment, Moho) on the inversion grid.

This module reads one geographic scalar product, normalizes its physical sign
convention and unit, and aligns it to the authoritative inversion grid. Fixed
fields must cover the whole region: missing cells, non-uniform axes and
out-of-range targets are rejected rather than extrapolated.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

import numpy as np
import pandas as pd
import xarray as xr

from seispy.mcmc.config import Config
from seispy.mcmc.gridding import (
    TargetGrid,
    align_to_grid,
    regular_grid_from_xyz,
    validate_regular_axis,
)
from seispy.mcmc.inputs import (
    LAT_NAMES,
    LON_NAMES,
    NETCDF_SUFFIXES,
    pick_name,
    read_table,
)

ScalarConvention = Literal["raw", "positive_depth", "positive_thickness"]


@dataclass(frozen=True)
class ScalarFieldSpec:
    """Describe how one spatial scalar field should be read and normalized.

    Parameters
    ----------
    name
        Human-readable field name used in diagnostics.
    convention
        Internal physical convention:

        - ``raw``: keep the original sign.
        - ``positive_depth``: depth must be positive downward.
        - ``positive_thickness``: thickness must be non-negative.

        For the two positive conventions, an input dataset whose non-zero values
        are consistently negative is automatically multiplied by -1. Mixed
        positive and negative non-zero values are rejected.
    value_columns
        Preferred scalar-value column or variable names for table and NetCDF
        inputs. If none is found, a unique remaining value column/variable is
        accepted automatically.
    allow_zero
        Whether zero is physically valid after normalization.
    """

    name: str
    convention: ScalarConvention = "raw"
    value_columns: tuple[str, ...] = ()
    allow_zero: bool = True


@dataclass(frozen=True, eq=False)
class SpatialFields:
    """Topography, sediment thickness and Moho depth on one target grid."""

    target: TargetGrid
    topo: np.ndarray
    sediment: np.ndarray
    moho: np.ndarray

    def __post_init__(self) -> None:
        expected = self.target.shape
        for name in ("topo", "sediment", "moho"):
            values = np.asarray(getattr(self, name), dtype=float)
            if values.shape != expected:
                raise ValueError(
                    f"{name} grid shape {values.shape} does not match target {expected}"
                )
            object.__setattr__(self, name, values)

    @property
    def shape(self) -> tuple[int, int]:
        return self.topo.shape

    @property
    def size(self) -> int:
        return self.topo.size

    def flat_points(self) -> Iterable[tuple[float, float, float, float, float]]:
        """Yield ``(lon, lat, topo, sediment, moho)`` in target-flattened order."""

        lon, lat = self.target.flat_lonlat()
        return zip(
            lon,
            lat,
            self.topo.ravel(),
            self.sediment.ravel(),
            self.moho.ravel(),
            strict=True,
        )


# =========================
# SCALAR-FIELD INPUT
# =========================


def _validate_scalar_xyz(xyz: np.ndarray, name: str) -> np.ndarray:
    """Validate and clean an ``[x, y, value]`` scalar-field array."""

    data = np.asarray(xyz, dtype=float)

    if data.ndim != 2 or data.shape[1] != 3:
        raise ValueError(
            f"{name} must be an N x 3 array containing x, y, value; "
            f"got shape={data.shape}"
        )
    if len(data) == 0:
        raise ValueError(f"{name} contains no valid finite data rows")
    if not np.isfinite(data).all():
        raise ValueError(
            f"{name} contains NaN or Inf; regular grids must have valid values "
            "at every node"
        )

    lat = data[:, 1]
    if np.any((lat < -90.0) | (lat > 90.0)):
        bad = lat[(lat < -90.0) | (lat > 90.0)][0]
        raise ValueError(f"{name} contains invalid latitude: {bad}")

    return data


def normalize_scalar_values(
    values: np.ndarray,
    *,
    convention: ScalarConvention,
    name: str,
    allow_zero: bool,
) -> np.ndarray:
    """Normalize scalar values to the requested internal convention.

    ``raw`` values are returned unchanged.

    For ``positive_depth`` and ``positive_thickness``, the sign convention is
    inferred from all non-zero values:

    - all positive -> unchanged;
    - all negative -> multiplied by -1;
    - mixed positive/negative -> error.

    This is intentionally different from applying ``abs`` element by element:
    mixed signs are treated as inconsistent source data instead of being
    silently repaired.
    """

    values = np.asarray(values, dtype=float)

    if values.ndim != 1:
        raise ValueError(f"{name} values must be a 1-D array")
    if not np.isfinite(values).all():
        raise ValueError(f"{name} contains NaN or Inf values")
    if not allow_zero and np.any(values == 0):
        raise ValueError(f"{name} contains zero values, which are not valid")
    if convention == "raw":
        return values.copy()
    if convention not in {"positive_depth", "positive_thickness"}:
        raise ValueError(f"Unknown scalar convention: {convention!r}")

    nonzero = values[values != 0]
    if nonzero.size == 0:
        # allow_zero is necessarily True here: the zero check above would have
        # rejected an all-zero field otherwise.
        return values.copy()

    if np.all(nonzero > 0):
        print(f"[INFO] {name}: input already uses positive-value convention")
        return values.copy()
    if np.all(nonzero < 0):
        print(
            f"[INFO] {name}: input uses negative-value convention; "
            "converting to positive values"
        )
        return -values

    n_positive = int(np.count_nonzero(nonzero > 0))
    n_negative = int(np.count_nonzero(nonzero < 0))
    raise ValueError(
        f"{name} contains mixed positive and negative values "
        f"({n_positive} positive, {n_negative} negative). "
        "Cannot determine a consistent sign convention."
    )


def _dataframe_to_scalar_xyz(df: pd.DataFrame, spec: ScalarFieldSpec) -> np.ndarray:
    lon_col = pick_name(df.columns, LON_NAMES)
    lat_col = pick_name(df.columns, LAT_NAMES)

    if lon_col is None:
        raise ValueError(
            f"{spec.name}: longitude column not found. "
            f"Expected one of {LON_NAMES}, got {list(df.columns)}"
        )
    if lat_col is None:
        raise ValueError(
            f"{spec.name}: latitude column not found. "
            f"Expected one of {LAT_NAMES}, got {list(df.columns)}"
        )

    value_col = pick_name(df.columns, spec.value_columns)
    if value_col is None:
        remaining = [
            column for column in df.columns if column not in {lon_col, lat_col}
        ]
        if len(remaining) != 1:
            raise ValueError(
                f"{spec.name}: cannot determine the scalar-value column. "
                f"Preferred names={spec.value_columns}, remaining columns={remaining}"
            )
        value_col = remaining[0]

    out = df[[lon_col, lat_col, value_col]].copy()
    out.columns = ["x", "y", "value"]
    for column in out.columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out.to_numpy(dtype=float)


def _slice_for_coordinate(
    coordinate: xr.DataArray,
    lower: float,
    upper: float,
) -> slice:
    """Create a selection slice that respects ascending/descending coordinates."""

    values = np.asarray(coordinate.values, dtype=float)
    if values.ndim != 1:
        raise ValueError("Source grid coordinates must be one-dimensional")
    ascending = values.size > 1 and values[0] < values[-1]
    ordered = values if ascending else values[::-1]
    validate_regular_axis(ordered, str(coordinate.name))
    if lower < ordered[0] - 1e-9 or upper > ordered[-1] + 1e-9:
        raise ValueError(
            "Source grid does not cover the target region; extrapolation is disabled"
        )
    # Keep the bracketing source nodes, including outside the target extent.
    first = max(0, np.searchsorted(ordered, lower, side="right") - 1)
    last = min(ordered.size - 1, np.searchsorted(ordered, upper, side="left"))
    if ascending:
        return slice(ordered[first], ordered[last])
    return slice(ordered[last], ordered[first])


def _read_netcdf_scalar(
    path: Path,
    spec: ScalarFieldSpec,
    region: list[float] | None,
) -> np.ndarray:
    """Read one geographic scalar field from NetCDF/xarray."""

    with xr.open_dataset(path) as ds:
        coordinate_names = set(ds.coords) | set(ds.dims)
        lon_name = pick_name(coordinate_names, LON_NAMES)
        lat_name = pick_name(coordinate_names, LAT_NAMES)

        if lon_name is None or lat_name is None:
            raise ValueError(
                f"{spec.name}: NetCDF must contain longitude/latitude "
                f"coordinates compatible with {LON_NAMES} / {LAT_NAMES}"
            )

        value_name = pick_name(ds.data_vars, spec.value_columns)
        if value_name is None:
            data_vars = list(ds.data_vars)
            if len(data_vars) != 1:
                raise ValueError(
                    f"{spec.name}: cannot determine NetCDF value variable. "
                    f"Preferred names={spec.value_columns}, data variables={data_vars}"
                )
            value_name = data_vars[0]

        da = ds[value_name]
        if set(da.dims) != {lon_name, lat_name}:
            raise ValueError(
                f"{spec.name}: variable {value_name!r} must depend on "
                f"{lon_name!r} and {lat_name!r}; got dims={da.dims}"
            )

        if region is not None:
            xmin, xmax, ymin, ymax = region
            da = da.sel(
                {
                    lon_name: _slice_for_coordinate(ds[lon_name], xmin, xmax),
                    lat_name: _slice_for_coordinate(ds[lat_name], ymin, ymax),
                }
            )

        frame = da.to_dataframe(name="value").reset_index()
        return frame[[lon_name, lat_name, "value"]].to_numpy(dtype=float)


def read_spatial_scalar(
    path: str | Path,
    spec: ScalarFieldSpec,
    *,
    region: list[float] | None = None,
) -> np.ndarray:
    """Read a spatial scalar field and return ``[x, y, value]``.

    Supported formats
    -----------------
    CSV
        Geographic coordinates may be named ``x/y``, ``lon/lat``, or
        ``longitude/latitude``.
    Parquet
        Uses the same column rules as CSV.
    NetCDF
        Geographic coordinate names are inferred from the same coordinate
        aliases. A preferred value variable is selected from
        ``spec.value_columns``; otherwise the dataset must contain exactly one
        data variable.
    Other suffixes
        Treated as whitespace-separated text with at least three columns.
        The first three columns are interpreted as x, y, value.

    After reading, the requested physical sign convention is applied before
    gridding.
    """

    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"{spec.name} file not found: {path}")

    suffix = path.suffix.lower()
    if suffix in {".csv", ".parquet"}:
        xyz = _dataframe_to_scalar_xyz(read_table(path), spec)
    elif suffix in NETCDF_SUFFIXES:
        xyz = _read_netcdf_scalar(path, spec, region)
    else:
        data = np.loadtxt(path)
        if data.ndim == 1:
            data = data.reshape(1, -1)
        if data.ndim != 2 or data.shape[1] < 3:
            raise ValueError(
                f"{spec.name}: text input must contain at least three columns "
                f"x, y, value; got shape={data.shape}"
            )
        xyz = np.asarray(data[:, :3], dtype=float)

    xyz = _validate_scalar_xyz(xyz, spec.name)
    xyz[:, 2] = normalize_scalar_values(
        xyz[:, 2],
        convention=spec.convention,
        name=spec.name,
        allow_zero=spec.allow_zero,
    )
    print(f"Loaded {spec.name} from {path}: {len(xyz)} valid x-y-value records")
    return xyz


# =========================
# ALIGNMENT AND TARGET FIELDS
# =========================


def interpolate_regular_grid(xyz: np.ndarray, target: TargetGrid) -> xr.DataArray:
    """Align a complete x/y/value grid to the requested inversion nodes.

    The source must reconstruct to a complete regular grid. Unordered rows and
    ascending or descending source axes are accepted. Equal and aligned grids
    are selected directly, coarser sources are linearly interpolated, and finer
    sources are averaged over the target cell. Missing cells, degenerate or
    non-uniform axes and targets outside source coverage are rejected. Longitude
    wrapping across the dateline is not performed automatically.
    """

    source = regular_grid_from_xyz(xyz)
    return align_to_grid(source, target, on_missing="raise")


def _validate_positive_grid(
    values: np.ndarray,
    *,
    name: str,
    allow_zero: bool,
) -> None:
    """Validate a grid that must already satisfy its physical sign constraint."""

    values = np.asarray(values, dtype=float)
    if not np.isfinite(values).all():
        raise ValueError(f"{name} grid contains NaN or Inf values")

    invalid = values < 0 if allow_zero else values <= 0
    requirement = "non-negative" if allow_zero else "positive"
    if np.any(invalid):
        bad = float(values[invalid][0])
        raise ValueError(
            f"{name} grid must remain {requirement} after interpolation; found {bad}"
        )


def build_target_fields(cfg: Config, target: TargetGrid) -> SpatialFields:
    """Build topography, sediment-thickness and Moho grids on ``target``.

    Each path may point to CSV, Parquet, NetCDF, or whitespace-separated XYZ
    text. Internal conventions
    --------------------------
    topography
        Raw elevation; positive and negative values are both allowed.
    sediment
        Non-negative thickness.
    moho
        Positive depth below the surface.
    """

    region = list(np.asarray(cfg.region, dtype=float))
    units = cfg.input_units

    topo_xyz = read_spatial_scalar(
        cfg.paths.topography_file,
        ScalarFieldSpec(
            name="topography",
            convention="raw",
            value_columns=("z", "topo", "topography", "elevation"),
            allow_zero=True,
        ),
        region=region,
    )
    topo_xyz[:, 2] *= units.topography_to_m

    sediment_xyz = read_spatial_scalar(
        cfg.paths.sediment_file,
        ScalarFieldSpec(
            name="sediment thickness",
            convention="positive_thickness",
            value_columns=("sediment", "sediment_thickness", "thickness", "sed", "z"),
            allow_zero=True,
        ),
        region=region,
    )
    sediment_xyz[:, 2] *= units.sediment_to_km

    moho_xyz = read_spatial_scalar(
        cfg.paths.moho_file,
        ScalarFieldSpec(
            name="Moho depth",
            convention="positive_depth",
            value_columns=("moho", "moho_depth", "depth", "z"),
            allow_zero=False,
        ),
        region=region,
    )
    moho_xyz[:, 2] *= units.moho_to_km

    topo = interpolate_regular_grid(topo_xyz, target).values
    sediment = interpolate_regular_grid(sediment_xyz, target).values
    moho = interpolate_regular_grid(moho_xyz, target).values

    _validate_positive_grid(sediment, name="sediment thickness", allow_zero=True)
    # Moho depth, in contrast, must remain strictly positive. A non-positive
    # value is treated as an invalid grid rather than silently repaired.
    _validate_positive_grid(moho, name="Moho depth", allow_zero=False)

    return SpatialFields(target=target, topo=topo, sediment=sediment, moho=moho)
