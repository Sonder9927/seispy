"""Spatial scalar fields, regular grids, and phase-dispersion data."""

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

import numpy as np
import pandas as pd
import xarray as xr

from seispy.mcmc.configuration import Config

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
        """Return rows that can be safely written to ``phase.input``."""

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
# SPATIAL SCALAR-FIELD INPUT
# =========================


_LON_COLUMNS = ("x", "lon", "longitude")
_LAT_COLUMNS = ("y", "lat", "latitude")
_NETCDF_SUFFIXES = {".nc", ".nc4", ".netcdf"}


def _pick_name(
    available: Iterable[str],
    candidates: tuple[str, ...],
) -> str | None:
    """Return the first candidate present in ``available``."""

    names = set(available)
    return next((candidate for candidate in candidates if candidate in names), None)


def _validate_scalar_xyz(xyz: np.ndarray, name: str) -> np.ndarray:
    """Validate and clean an ``[x, y, value]`` scalar-field array."""

    data = np.asarray(xyz, dtype=float)

    if data.ndim != 2 or data.shape[1] != 3:
        raise ValueError(
            f"{name} must be an N x 3 array containing x, y, value; "
            f"got shape={data.shape}"
        )

    finite = np.isfinite(data).all(axis=1)
    if not np.all(finite):
        removed = int((~finite).sum())
        print(f"[WARN] {name}: dropping {removed} rows containing NaN or Inf")
        data = data[finite]

    if len(data) == 0:
        raise ValueError(f"{name} contains no valid finite data rows")

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

    if convention == "raw":
        return values.copy()

    if convention not in {"positive_depth", "positive_thickness"}:
        raise ValueError(f"Unknown scalar convention: {convention!r}")

    if not allow_zero and np.any(values == 0):
        raise ValueError(f"{name} contains zero values, which are not valid")

    nonzero = values[values != 0]
    if nonzero.size == 0:
        if allow_zero:
            return values.copy()
        raise ValueError(f"{name} contains only zero values")

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


def _dataframe_to_scalar_xyz(
    df: pd.DataFrame,
    spec: ScalarFieldSpec,
) -> np.ndarray:
    """Convert a CSV/Parquet DataFrame to ``[x, y, value]``."""

    lon_col = _pick_name(df.columns, _LON_COLUMNS)
    lat_col = _pick_name(df.columns, _LAT_COLUMNS)

    if lon_col is None:
        raise ValueError(
            f"{spec.name}: longitude column not found. "
            f"Expected one of {_LON_COLUMNS}, got {list(df.columns)}"
        )
    if lat_col is None:
        raise ValueError(
            f"{spec.name}: latitude column not found. "
            f"Expected one of {_LAT_COLUMNS}, got {list(df.columns)}"
        )

    value_col = _pick_name(df.columns, spec.value_columns)

    if value_col is None:
        remaining = [
            column for column in df.columns if column not in {lon_col, lat_col}
        ]

        if len(remaining) != 1:
            raise ValueError(
                f"{spec.name}: cannot determine the scalar-value column. "
                f"Preferred names={spec.value_columns}, "
                f"remaining columns={remaining}"
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
    if values.size < 2 or values[0] <= values[-1]:
        return slice(lower, upper)
    return slice(upper, lower)


def _read_netcdf_scalar(
    path: Path,
    spec: ScalarFieldSpec,
    region: list[float] | None,
) -> np.ndarray:
    """Read one geographic scalar field from NetCDF/xarray."""

    with xr.open_dataset(path) as ds:
        coordinate_names = set(ds.coords) | set(ds.dims)

        lon_name = _pick_name(coordinate_names, _LON_COLUMNS)
        lat_name = _pick_name(coordinate_names, _LAT_COLUMNS)

        if lon_name is None or lat_name is None:
            raise ValueError(
                f"{spec.name}: NetCDF must contain longitude/latitude "
                f"coordinates compatible with {_LON_COLUMNS} / {_LAT_COLUMNS}"
            )

        value_name = _pick_name(ds.data_vars, spec.value_columns)
        if value_name is None:
            data_vars = list(ds.data_vars)
            if len(data_vars) != 1:
                raise ValueError(
                    f"{spec.name}: cannot determine NetCDF value variable. "
                    f"Preferred names={spec.value_columns}, "
                    f"data variables={data_vars}"
                )
            value_name = data_vars[0]

        da = ds[value_name]

        if lon_name not in da.dims or lat_name not in da.dims:
            raise ValueError(
                f"{spec.name}: variable {value_name!r} must depend on "
                f"{lon_name!r} and {lat_name!r}; got dims={da.dims}"
            )

        if region is not None:
            xmin, xmax, ymin, ymax = region
            da = da.sel(
                {
                    lon_name: _slice_for_coordinate(
                        ds[lon_name],
                        xmin,
                        xmax,
                    ),
                    lat_name: _slice_for_coordinate(
                        ds[lat_name],
                        ymin,
                        ymax,
                    ),
                }
            )

        frame = da.to_dataframe(name="value").reset_index()
        return frame[[lon_name, lat_name, "value"]].to_numpy(dtype=float)


def read_scalar_field(
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

    if suffix == ".csv":
        xyz = _dataframe_to_scalar_xyz(pd.read_csv(path), spec)
    elif suffix == ".parquet":
        xyz = _dataframe_to_scalar_xyz(pd.read_parquet(path), spec)
    elif suffix in _NETCDF_SUFFIXES:
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
# GRID AND INTERPOLATION
# =========================


def grid_coordinates(
    region: list[float],
    spacing: float,
    shape: tuple[int, int],
) -> tuple[np.ndarray, np.ndarray]:
    xmin, xmax, ymin, ymax = region
    ny, nx = shape
    lon_values = np.linspace(xmin, xmax, nx)
    lat_values = np.linspace(ymin, ymax, ny)
    return np.meshgrid(lon_values, lat_values)


def surface_grid(
    xyz: np.ndarray,
    region: list[float],
    spacing: float,
    method: str = "surface",
):
    """Interpolate xyz values onto a regular grid with PyGMT.

    Parameters
    ----------
    method
        ``surface`` uses GMT ``surface`` after ``blockmean``.
        ``xyz2grd`` directly grids the block-averaged values.
        ``auto`` uses ``surface`` unless the output grid is too small.
    """

    import pygmt

    xyz_block = pygmt.blockmean(
        data=xyz,
        region=region,
        spacing=spacing,
    )

    xmin, xmax, ymin, ymax = region
    nx = int((xmax - xmin) / spacing + 1)
    ny = int((ymax - ymin) / spacing + 1)

    if method not in {"surface", "xyz2grd", "auto"}:
        raise ValueError("method must be 'surface', 'xyz2grd', or 'auto'")

    use_xyz2grd = method == "xyz2grd" or (method == "auto" and (nx < 4 or ny < 4))
    if use_xyz2grd:
        return pygmt.xyz2grd(
            data=xyz_block,
            region=region,
            spacing=spacing,
        )

    if nx < 4 or ny < 4:
        print(f"[WARN] small grid ({nx}x{ny}), fallback xyz2grd")
        return pygmt.xyz2grd(
            data=xyz_block,
            region=region,
            spacing=spacing,
        )

    return pygmt.surface(
        data=xyz_block,
        region=region,
        spacing=spacing,
        tension=0.35,
    )


def _clip_nonnegative_grid(
    values: np.ndarray,
    *,
    name: str,
) -> np.ndarray:
    """Clip negative interpolation overshoot to the physical lower bound zero."""

    values = np.asarray(values, dtype=float).copy()

    if not np.isfinite(values).all():
        raise ValueError(f"{name} grid contains NaN or Inf values")

    negative = values < 0
    if np.any(negative):
        n_negative = int(np.count_nonzero(negative))
        min_value = float(values[negative].min())

        print(
            f"[WARN] {name}: {n_negative} interpolated grid cells are below zero "
            f"(minimum={min_value:.3f}); clipping them to 0"
        )

        values[negative] = 0.0

    return values


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

    if allow_zero:
        invalid = values < 0
        requirement = "non-negative"
    else:
        invalid = values <= 0
        requirement = "positive"

    if np.any(invalid):
        bad = float(values[invalid][0])
        raise ValueError(
            f"{name} grid must remain {requirement} after interpolation; found {bad}"
        )


def build_spatial_grid(cfg: Config) -> GridData:
    """Build topography, sediment-thickness, and Moho grids.

    The existing ``Paths`` field names are preserved for compatibility with the
    current configuration model, but each path may now point to CSV, Parquet,
    NetCDF, or whitespace-separated XYZ text.

    Internal conventions
    --------------------
    topography
        Raw elevation; positive and negative values are both allowed.
    sediment
        Non-negative thickness.
    moho
        Positive depth below the surface.
    """

    region = cfg.region
    spacing = cfg.grid_spacing

    topo_xyz = read_scalar_field(
        cfg.paths.topography_file,
        ScalarFieldSpec(
            name="topography",
            convention="raw",
            value_columns=("z", "topo", "topography", "elevation"),
            allow_zero=True,
        ),
        region=region,
    )

    sediment_xyz = read_scalar_field(
        cfg.paths.sediment_file,
        ScalarFieldSpec(
            name="sediment thickness",
            convention="positive_thickness",
            value_columns=(
                "sediment",
                "sediment_thickness",
                "thickness",
                "sed",
                "z",
            ),
            allow_zero=True,
        ),
        region=region,
    )

    moho_xyz = read_scalar_field(
        cfg.paths.moho_file,
        ScalarFieldSpec(
            name="Moho depth",
            convention="positive_depth",
            value_columns=("moho", "moho_depth", "depth", "z"),
            allow_zero=False,
        ),
        region=region,
    )

    topo = surface_grid(topo_xyz, region, spacing, method="surface").values

    sediment = surface_grid(sediment_xyz, region, spacing, method="surface").values

    moho = surface_grid(moho_xyz, region, spacing, method="surface").values

    # GMT surface can overshoot below zero near sharp boundaries even when all
    # source sediment-thickness values are non-negative. Since zero thickness is
    # physically valid, clip those interpolation artefacts after gridding.
    sediment = _clip_nonnegative_grid(sediment, name="sediment thickness")

    # Moho depth, in contrast, must remain strictly positive. A non-positive
    # value is treated as an invalid grid rather than silently repaired.
    _validate_positive_grid(moho, name="Moho depth", allow_zero=False)

    if topo.shape != sediment.shape or topo.shape != moho.shape:
        raise ValueError(
            "Spatial grids have inconsistent shapes: "
            f"topography={topo.shape}, sediment={sediment.shape}, "
            f"moho={moho.shape}"
        )

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


def read_phase_dispersion(path: str | Path) -> pd.DataFrame:
    path = Path(path)

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)

    elif path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)

    else:
        raise ValueError(f"Phase dispersion file must be CSV or Parquet, got: {path}")

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

    columns = [lon_col, lat_col, period_col, phv_col]
    if std_col:
        columns.append(std_col)

    out = df[columns].copy()
    rename = {
        lon_col: "lon",
        lat_col: "lat",
        period_col: "period",
        phv_col: "phv",
    }
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

    The merged phase-dispersion CSV is treated as the final dispersion source.
    A phase value is assigned only when the CSV contains the same lon/lat
    coordinate as the inversion grid. Missing coordinates remain NaN and are
    not written to ``phase.input``.

    The CSV std column is optional. If present, it is scaled by
    ``cfg.phase_sigma_scale``. Missing or invalid sigma values remain NaN here
    and are replaced by ``cfg.default_phase_sigma`` only when ``phase.input`` is
    written.
    """

    phase_file = cfg.paths.phase_dispersion_file
    df = read_phase_dispersion(phase_file)

    periods = np.asarray(
        sorted(df["period"].dropna().unique()),
        dtype=float,
    )
    if len(periods) == 0:
        raise ValueError(f"No valid periods found in {phase_file}")

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
            "No phase-dispersion rows match the inversion-grid coordinates. "
            "Check region/grid_spacing and phase-dispersion lon/lat values."
        )

    grouped = data.groupby(
        ["period_key", "lat_key", "lon_key"],
        as_index=False,
    ).agg(phv=("phv", "mean"), std=("std", "mean"))

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
        f"Loaded phase dispersion directly from {phase_file}: "
        f"{len(periods)} periods, "
        f"{len(grouped)} lon-lat-period records mapped."
    )

    return PhaseCube(periods=periods, velocities=velocities, sigmas=sigmas)
