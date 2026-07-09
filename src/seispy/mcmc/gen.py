"""Generate MCMC input files on a regular geographic grid.

Main workflow
-------------
1. Read config.json.
2. Interpolate topography, sediment thickness, and Moho depth onto the
   inversion grid.  Phase-dispersion data are mapped directly from
   phase_dispersion.csv without spatial interpolation.
3. Attach the nearest reference Vs(z) profile to each grid point.
4. Write phase.input, para.inp, and input_DRAM_T.dat for every grid point.

The dispersion workflow assumes that TPWT, ANT, or other measurements have
already been merged into one CSV file, e.g. phase_dispersion.csv.

The B-spline Vs search bounds are constrained by the top-level
``vs_constraints`` block in config.json.
"""

from __future__ import annotations

import json
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable, List, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm

# =========================
# CONFIG
# =========================


@dataclass(frozen=True)
class Paths:
    etopo_nc: str
    sed_xyz: str
    moho_xyz: str
    vs_model_csv: str
    output_dir: str
    phase_dispersion_csv: str


@dataclass(frozen=True)
class VsConstraints:
    """Physical constraints for Vs-related search bounds in para.inp.

    Values are in km/s, except ``deep_vs_gradient`` which is in
    (km/s)/km and is used only when the reference Vs model is shallower
    than the requested B-spline representative depth.

    ``*_soft_max`` and ``*_hard_max`` define a two-level upper-bound
    strategy.  If the reference Vs center is below the soft maximum, the
    normal search interval is used.  If it lies between the soft and hard
    maxima, the upper bound is clipped at the hard maximum.  If it exceeds
    the hard maximum, the search window is shifted below the hard maximum
    so that the original search width is largely preserved.
    """

    sediment_max: float = 3.0
    crust_soft_max: float = 3.9
    crust_hard_max: float = 4.0
    mantle_soft_max: float = 4.9
    mantle_hard_max: float = 5.0
    deep_vs_gradient: float = 0.001
    mantle_not_slower_than_crust: bool = True
    min_vs_bound_width: float = 0.05


@dataclass(frozen=True)
class PhaseConstraints:
    """Quality-control settings for writing phase.input."""

    minimum_periods: int = 5
    skip_if_insufficient: bool = True


@dataclass(frozen=True)
class Config:
    region: List[float]
    grid_spacing: float
    search_radius: dict
    mcmc_params: dict
    paths: Paths
    water_threshold: float
    sediment_threshold: float
    sediment_vs: List[List[float]]
    n_coeff_crust: int
    n_coeff_mantle: int
    sm_on: int
    ice_on: int
    factor: float
    zmax_Bs: float
    NPTS_cBs: int
    NPTS_mBs: int
    reference_model: str
    reference_water_model: str
    # phase_dispersion.csv usually stores std in m/s, while phase.input expects km/s.
    # Missing std values are replaced only when writing phase.input.
    default_phase_sigma: float = 0.03
    phase_sigma_scale: float = 0.001
    min_dispersion_points: int = 3
    vs_constraints: VsConstraints = field(default_factory=VsConstraints)
    phase_constraints: PhaseConstraints = field(default_factory=PhaseConstraints)


def load_config(path: str | Path) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    raw["paths"] = Paths(**raw["paths"])

    # Backward compatibility: older config files may still contain
    # crust_max/mantle_max.  Treat them as hard maxima and infer soft maxima.
    vs_raw = dict(raw.get("vs_constraints", {}))
    if "crust_max" in vs_raw and "crust_hard_max" not in vs_raw:
        vs_raw["crust_hard_max"] = vs_raw.pop("crust_max")
        vs_raw.setdefault("crust_soft_max", float(vs_raw["crust_hard_max"]) - 0.1)
    if "mantle_max" in vs_raw and "mantle_hard_max" not in vs_raw:
        vs_raw["mantle_hard_max"] = vs_raw.pop("mantle_max")
        vs_raw.setdefault("mantle_soft_max", float(vs_raw["mantle_hard_max"]) - 0.1)
    # Ignore stale/experimental keys in config.json, e.g. soft_margin.

    valid_vs_keys = {f.name for f in fields(VsConstraints)}
    vs_raw = {k: v for k, v in vs_raw.items() if k in valid_vs_keys}
    raw["vs_constraints"] = VsConstraints(**vs_raw)

    phase_raw = dict(raw.get("phase_constraints", {}))
    valid_phase_keys = {f.name for f in fields(PhaseConstraints)}
    phase_raw = {k: v for k, v in phase_raw.items() if k in valid_phase_keys}
    raw["phase_constraints"] = PhaseConstraints(**phase_raw)

    # periods are now a data property inferred from phase_dispersion.csv.
    raw.pop("periods", None)
    return Config(**raw)


# =========================
# BASIC DATA CONTAINERS
# =========================


VsProfile = Tuple[np.ndarray, np.ndarray]


@dataclass(frozen=True)
class PhaseCurve:
    periods: np.ndarray
    velocities: np.ndarray
    sigmas: np.ndarray

    def __len__(self) -> int:
        return len(self.periods)

    def rows(self) -> Iterable[tuple[float, float, float]]:
        return zip(self.periods, self.velocities, self.sigmas)

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


# =========================
# VS REFERENCE MODEL
# =========================


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


# =========================
# WRITER
# =========================


class GridWriter:
    def __init__(self, base_dir: Path, cfg: Config):
        self.base_dir = base_dir
        self.cfg = cfg

    def write(self, grid: MCMCGrid, phase: PhaseCurve) -> bool:
        grid.validate()
        rows = phase.valid_rows(default_sigma=float(self.cfg.default_phase_sigma))
        minimum_periods = int(self.cfg.phase_constraints.minimum_periods)
        if (
            self.cfg.phase_constraints.skip_if_insufficient
            and len(rows) < minimum_periods
        ):
            print(
                f"[SKIP] {grid.folder_name}: only {len(rows)} valid dispersion points "
                f"(< {minimum_periods})"
            )
            return False

        out = self.base_dir / grid.folder_name
        out.mkdir(parents=True, exist_ok=True)

        self._write_phase(out, rows)
        self._write_para(out, grid)
        self._write_dram(out)
        return True

    def _write_phase(self, out: Path, rows: list[tuple[float, float, float]]) -> None:
        lines = [f"1 {len(rows)}"]
        for period, velocity, sigma in rows:
            lines.append(f"2 1 1 {period:>3g} {velocity:.4f} {sigma:.4f}")

        lines += ["0", "0"]
        (out / "phase.input").write_text("\n".join(lines), encoding="utf-8")

    def _vs_perturbation(self, section: str, n_coeff: int) -> np.ndarray:
        candidates: list[Any] = [
            self.cfg.search_radius.get(f"{section}_vs"),
            self.cfg.search_radius.get(section),
            self.cfg.search_radius.get("vs"),
        ]
        default = 0.30 if section == "crust" else 0.20
        value = next((v for v in candidates if v is not None), default)

        if isinstance(value, dict):
            value = value.get("vs", default)

        arr = np.asarray(value, dtype=float)
        if arr.ndim == 0:
            return np.full(n_coeff, float(arr))
        if arr.size != n_coeff:
            raise ValueError(
                f"search_radius for {section} has {arr.size} values, expected {n_coeff}"
            )
        return arr

    def _deep_vs_gradient(self) -> float:
        return float(self.cfg.vs_constraints.deep_vs_gradient)

    def _section_vs_limits(self, section: str) -> tuple[float, float, float]:
        """Return lower, soft upper, and hard upper limits for a Vs section."""

        vc = self.cfg.vs_constraints
        if section == "sediment":
            lower, soft, hard = 0.0, float(vc.sediment_max), float(vc.sediment_max)
        elif section == "crust":
            lower, soft, hard = 0.0, float(vc.crust_soft_max), float(vc.crust_hard_max)
        elif section == "mantle":
            lower, soft, hard = (
                0.0,
                float(vc.mantle_soft_max),
                float(vc.mantle_hard_max),
            )
        else:
            raise ValueError(f"Unknown Vs section: {section}")

        if (
            not all(np.isfinite(v) for v in (lower, soft, hard))
            or hard <= lower
            or soft > hard
        ):
            raise ValueError(
                f"Invalid {section} Vs limits: lower={lower}, soft_max={soft}, hard_max={hard}"
            )
        return lower, soft, hard

    def _repair_interval_width(
        self, lower: np.ndarray, upper: np.ndarray, vs_min: float, vs_max: float
    ) -> tuple[np.ndarray, np.ndarray]:
        """Ensure each interval has at least min_vs_bound_width when possible."""

        min_width = float(self.cfg.vs_constraints.min_vs_bound_width)
        if not np.isfinite(min_width) or min_width < 0:
            raise ValueError(
                f"min_vs_bound_width must be non-negative, got {min_width}"
            )
        if min_width == 0:
            return lower, upper

        too_narrow = (upper - lower) < min_width
        if np.any(too_narrow):
            upper[too_narrow] = np.minimum(vs_max, lower[too_narrow] + min_width)
            still_too_narrow = (upper - lower) < min_width
            lower[still_too_narrow] = np.maximum(
                vs_min, upper[still_too_narrow] - min_width
            )
        return lower, upper

    def _apply_vs_limits(
        self,
        centers: np.ndarray,
        half_widths: np.ndarray,
        section: str,
    ) -> tuple[np.ndarray, np.ndarray]:
        """Apply soft/hard physical limits to B-spline coefficient bounds.

        For centers below the soft maximum, the ordinary [center-r, center+r]
        interval is used.  For centers in the soft zone, only the upper side is
        clipped to the hard maximum.  For centers exceeding the hard maximum,
        the interval is shifted below the hard maximum so that the full width
        2*r is mostly retained.  For example, center=4.2, r=0.3, hard=4.0
        gives [3.4, 4.0].
        """

        vs_min, soft_max, hard_max = self._section_vs_limits(section)
        centers = np.asarray(centers, dtype=float)
        half_widths = np.asarray(half_widths, dtype=float)

        lower = centers - half_widths
        upper = centers + half_widths

        above_hard = centers > hard_max
        if np.any(above_hard):
            upper[above_hard] = hard_max
            lower[above_hard] = hard_max - 2.0 * half_widths[above_hard]

        soft_zone = (centers > soft_max) & (centers <= hard_max)
        if np.any(soft_zone):
            upper[soft_zone] = np.minimum(upper[soft_zone], hard_max)

        lower = np.clip(lower, vs_min, hard_max)
        upper = np.clip(upper, vs_min, hard_max)
        lower, upper = self._repair_interval_width(lower, upper, vs_min, hard_max)
        return lower, upper

    def _bspline_bounds(
        self,
        grid: MCMCGrid,
        z_top: float,
        z_bottom: float,
        n_coeff: int,
        section: str,
    ) -> list[tuple[float, float]]:
        rep_depths = greville_depths(n_coeff, z_top, z_bottom, self.cfg.factor)
        centers = velocity_at_depths(
            grid.vs_profile,
            rep_depths,
            deep_extrapolation_gradient=self._deep_vs_gradient(),
        )
        half_widths = self._vs_perturbation(section, n_coeff)
        lower, upper = self._apply_vs_limits(centers, half_widths, section)
        return list(zip(lower, upper))

    def _apply_mantle_crust_constraint(
        self,
        crust_bounds: list[tuple[float, float]],
        mantle_bounds: list[tuple[float, float]],
    ) -> list[tuple[float, float]]:
        """Optionally prevent the shallowest mantle from being slower than crust.

        This is deliberately mild: only the first mantle coefficient lower
        bound is required to be no smaller than the last crustal lower bound.
        It avoids an obvious Moho velocity reversal without forcing the whole
        mantle interval to be faster than every crustal coefficient.
        """

        if not self.cfg.vs_constraints.mantle_not_slower_than_crust:
            return mantle_bounds
        if not crust_bounds or not mantle_bounds:
            return mantle_bounds

        _, _, mantle_max = self._section_vs_limits("mantle")
        min_width = float(self.cfg.vs_constraints.min_vs_bound_width)
        crust_last_lower = float(crust_bounds[-1][0])

        adjusted = list(mantle_bounds)
        low, high = adjusted[0]
        low = max(float(low), crust_last_lower)
        high = max(float(high), low + min_width)
        high = min(high, mantle_max)
        if high - low < min_width:
            low = max(0.0, high - min_width)
        adjusted[0] = (low, high)
        return adjusted

    def _constrain_sediment_vs_bounds(self, bounds: list[float]) -> tuple[float, float]:
        """Constrain sediment Vs bounds using cfg.vs_constraints.sediment_max."""

        if len(bounds) != 2:
            raise ValueError(f"sediment_vs bounds must have two values, got {bounds}")
        vs_min, _, vs_max = self._section_vs_limits("sediment")
        low = float(np.clip(bounds[0], vs_min, vs_max))
        high = float(np.clip(bounds[1], vs_min, vs_max))
        if high <= low:
            min_width = float(self.cfg.vs_constraints.min_vs_bound_width)
            high = min(vs_max, low + min_width)
            low = max(vs_min, high - min_width)
        return low, high

    def _write_para(self, out: Path, grid: MCMCGrid) -> None:
        sr = self.cfg.search_radius

        lines = [str(grid.smooth_on), str(grid.ice_on), str(grid.water_on)]
        if grid.water_on:
            lines.append(f"{grid.water_depth:.3f}")

        lines.append(str(grid.sediment_on))
        lines += [
            f"{self.cfg.factor}",
            f"{self.cfg.zmax_Bs}",
            f"{self.cfg.NPTS_cBs}",
            f"{self.cfg.NPTS_mBs}",
        ]
        lines.append(
            self.cfg.reference_water_model
            if grid.water_on
            else self.cfg.reference_model
        )

        if grid.sediment_on:
            radius = float(sr.get("sediment", 0.0))
            low = max(0.0, grid.sediment_thickness - radius)
            high = grid.sediment_thickness + radius
            lines.append(f"0 0 {low:.2f} {high:.2f}")

        moho_radius = float(sr.get("moho", 0.0))
        lines.append(
            f"0 1 {grid.moho_depth - moho_radius:.2f} {grid.moho_depth + moho_radius:.2f}"
        )

        if grid.sediment_on:
            for i, bounds in enumerate(self.cfg.sediment_vs, start=1):
                low, high = self._constrain_sediment_vs_bounds(bounds)
                lines.append(f"10 {i} {low:.3f} {high:.3f}")

        crust_bounds = self._bspline_bounds(
            grid=grid,
            z_top=grid.crustal_spline_top,
            z_bottom=grid.moho_depth,
            n_coeff=self.cfg.n_coeff_crust,
            section="crust",
        )
        for i, (low, high) in enumerate(crust_bounds, start=1):
            lines.append(f"1 {i} {low:.3f} {high:.3f}")

        mantle_bounds = self._bspline_bounds(
            grid=grid,
            z_top=grid.moho_depth,
            z_bottom=grid.max_depth,
            n_coeff=self.cfg.n_coeff_mantle,
            section="mantle",
        )
        mantle_bounds = self._apply_mantle_crust_constraint(crust_bounds, mantle_bounds)
        for i, (low, high) in enumerate(mantle_bounds, start=1):
            lines.append(f"2 {i} {low:.3f} {high:.3f}")

        (out / "para.inp").write_text("\n".join(lines), encoding="utf-8")

    def _write_dram(self, out: Path) -> None:
        p = self.cfg.mcmc_params
        keys = [
            "mineos_on",
            "nsimu",
            "inm",
            "nc",
            "adaptint",
            "imat_fac",
            "verbo",
            "dodr",
            "sigma2",
            "DRscale",
            "iresetad",
            "id_run",
            "biasfac",
            "burn_in",
            "out_best",
        ]
        header = " | ".join(keys)
        values = " ".join(str(p[key]) for key in keys)
        (out / "input_DRAM_T.dat").write_text(header + "\n" + values, encoding="utf-8")


# =========================
# TASK BUILDING AND RUNNER
# =========================


def build_tasks(
    cfg: Config, grid_data: GridData, phase_cube: PhaseCube, vs_library: VsModelLibrary
):
    tasks = []
    for k, (lon, lat, topo, sediment, moho) in enumerate(grid_data.flat_values()):
        phase = phase_cube.curve_at_flat_index(k)
        vs_profile = vs_library.nearest_profile(float(lon), float(lat))
        tasks.append((lon, lat, topo, sediment, moho, phase, vs_profile, cfg))
    return tasks


def process_point(args) -> tuple[MCMCGrid, PhaseCurve]:
    lon, lat, topo, sediment, moho, phase, vs_profile, cfg = args
    grid = make_mcmc_grid(lon, lat, topo, sediment, moho, vs_profile, cfg)
    return grid, phase


def init_grids(config_path: str | Path, max_workers: int = 1) -> None:
    cfg = load_config(config_path)
    base_dir = Path(cfg.paths.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    grid_data = build_spatial_grid(cfg)
    phase_cube = build_phase_cube(cfg, grid_data)
    vs_library = VsModelLibrary.from_csv(cfg.paths.vs_model_csv)
    tasks = build_tasks(cfg, grid_data, phase_cube, vs_library)

    writer = GridWriter(base_dir, cfg)

    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            iterator = executor.map(process_point, tasks)
            written = 0
            for grid, phase in tqdm(iterator, total=len(tasks)):
                written += int(writer.write(grid, phase))
    else:
        written = 0
        for task in tqdm(tasks):
            grid, phase = process_point(task)
            written += int(writer.write(grid, phase))

    print(f"MCMC grids initialized. Written grids: {written}/{len(tasks)}")


if __name__ == "__main__":
    init_grids("data/mcmc/config.json", max_workers=4)
