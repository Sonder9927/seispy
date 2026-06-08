import json
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Tuple

import numpy as np
import pandas as pd
import xarray as xr
from tqdm import tqdm


# =========================
# CONFIG
# =========================


@dataclass
class Paths:
    etopo_nc: str
    sed_xyz: str
    moho_xyz: str
    tpwt_csv: str
    ant_csv: str
    vs_model_csv: str
    output_dir: str


@dataclass
class Config:
    region: List[float]
    grid_spacing: float
    periods: List[float]
    search_radius: dict
    mcmc_params: dict
    paths: Paths
    water_threshold: float
    sediment_threshold: float
    sediment_vs: List[List[float]]
    crust_vs: List[List[float]]
    mantle_vs: List[List[float]]
    sm_on: int
    ice_on: int
    factor: float
    zmax_Bs: float
    NPTS_cBs: int
    NPTS_mBs: int
    reference_model: str
    reference_water_model: str


def load_config(path: str | Path) -> Config:
    with open(path) as f:
        raw = json.load(f)
    raw["paths"] = Paths(**raw["paths"])
    return Config(**raw)


# =========================
# GRID
# =========================


def surface_grid(xyz, region, spacing):
    import pygmt

    xyz_block = pygmt.blockmean(data=xyz, region=region, spacing=spacing)

    xmin, xmax, ymin, ymax = region
    nx = int((xmax - xmin) / spacing + 1)
    ny = int((ymax - ymin) / spacing + 1)

    if nx < 4 or ny < 4:
        print(f"[WARN] small grid ({nx}x{ny}), fallback xyz2grd")
        return pygmt.xyz2grd(data=xyz_block, region=region, spacing=spacing)

    return pygmt.surface(
        data=xyz_block,
        region=region,
        spacing=spacing,
        tension=0.35,
    )


def load_etopo_xyz(path, region):
    ds = xr.open_dataset(path)
    da = ds["z"].sel(
        lon=slice(region[0], region[1]),
        lat=slice(region[2], region[3]),
    )
    df = da.to_dataframe(name="z").reset_index()
    return df[["lon", "lat", "z"]].values


# =========================
# DISPERSION
# =========================


def build_dispersion(csv_path, periods, region, spacing):
    df = pd.read_csv(csv_path)
    out = {}

    for period in periods:
        sub = df[df["period"] == period]

        if sub.empty:
            out[period] = (None, None)
            continue

        phv_grid = surface_grid(
            sub[["longitude", "latitude", "phv"]].values, region, spacing
        )

        if "std" in sub.columns:
            std_grid = surface_grid(
                sub[["longitude", "latitude", "std"]].values, region, spacing
            )
            std_values = std_grid.values * 0.001
        else:
            std_values = None

        out[period] = (phv_grid.values, std_values)

    return out


def safe_nanmean(arrays):
    arrays = [a for a in arrays if a is not None]
    if not arrays:
        return None
    stacked = np.asarray(arrays, dtype=float)
    if np.all(np.isnan(stacked)):
        return None
    return np.nanmean(stacked, axis=0)


def fuse_dispersion(tpwt, ant, periods):
    phv_out = {}
    std_out = {}

    for period in periods:
        phv1, std1 = tpwt.get(period, (None, None))
        phv2, std2 = ant.get(period, (None, None))
        phv_out[period] = safe_nanmean([phv1, phv2])
        std_out[period] = safe_nanmean([std1, std2])

    return phv_out, std_out


# =========================
# VS REFERENCE MODEL
# =========================


VsProfile = Tuple[np.ndarray, np.ndarray]


@dataclass
class VsModelLibrary:
    points: np.ndarray
    profiles: List[VsProfile]

    @classmethod
    def from_csv(cls, path: str | Path) -> "VsModelLibrary":
        df = pd.read_csv(path)
        required = {"x", "y", "z", "vs"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"vs_model_csv missing columns: {sorted(missing)}")

        points = []
        profiles = []
        for (x, y), group in df.groupby(["x", "y"], sort=False):
            g = group.sort_values("z")
            depth = np.abs(g["z"].to_numpy(dtype=float))
            velocity = g["vs"].to_numpy(dtype=float)
            order = np.argsort(depth)
            points.append((float(x), float(y)))
            profiles.append((depth[order], velocity[order]))

        if not profiles:
            raise ValueError(f"No Vs profiles found in {path}")

        return cls(points=np.asarray(points, dtype=float), profiles=profiles)

    def nearest_profile(self, lon: float, lat: float) -> VsProfile:
        dist2 = (self.points[:, 0] - lon) ** 2 + (self.points[:, 1] - lat) ** 2
        return self.profiles[int(np.argmin(dist2))]


# =========================
# FORTRAN-COMPATIBLE B-SPLINE DEPTHS
# =========================


def fortran_knot_vector(n_basis: int, z_top: float, z_bottom: float, factor: float) -> np.ndarray:
    """Reproduce the knot vector construction in lf_B_spline.f.

    In the original Fortran call, degBs = nBs - 1.  We keep the same
    variable name as spline_order to avoid changing the source logic.
    """

    if n_basis < 2:
        raise ValueError("n_basis must be >= 2")
    if z_bottom <= z_top:
        raise ValueError(f"Invalid spline interval: {z_top} -> {z_bottom}")

    spline_order = n_basis - 1
    n_knots = n_basis + spline_order
    span = z_bottom - z_top
    knots = np.empty(n_knots, dtype=float)

    # Left nearly repeated knots.
    for i in range(1, spline_order + 1):
        knots[i - 1] = z_top + (i - 1) * span / 100000.0

    # Interior knots with geometric spacing controlled by factor.
    n_temp = n_knots - 2 * spline_order + 1
    if n_temp <= 0:
        raise ValueError("Invalid knot configuration")

    if factor != 1:
        step0 = span * (factor - 1.0) / (factor**n_temp - 1.0)
    else:
        step0 = span / n_temp

    for i in range(spline_order + 1, n_knots - spline_order + 1):
        knots[i - 1] = step0 * factor ** (i - spline_order - 1) + z_top

    # Right nearly repeated knots.
    for i in range(n_knots - spline_order + 1, n_knots + 1):
        knots[i - 1] = z_bottom - span / 100000.0 * (n_knots - i)

    return knots


def greville_depths(n_basis: int, z_top: float, z_bottom: float, factor: float) -> np.ndarray:
    """Return Greville representative depths from Fortran-style knots."""

    knots = fortran_knot_vector(n_basis, z_top, z_bottom, factor)
    spline_order = n_basis - 1
    depths = []
    for j in range(n_basis):
        depths.append(np.mean(knots[j + 1 : j + spline_order + 1]))
    return np.asarray(depths, dtype=float)


def velocity_at_depths(
    profile: VsProfile,
    depths: np.ndarray,
    deep_extrapolation_gradient: float = 0.001,
) -> np.ndarray:
    """Interpolate/extrapolate Vs at requested depths.

    Depths inside the available Vs model are linearly interpolated.
    Depths deeper than the maximum depth of the Vs model are extrapolated
    from the deepest available Vs using a small positive gradient.  The
    actual cutoff depth is read from vs_model.csv for each profile, while
    requested depths come from the Greville depths defined by config.json
    intervals such as Moho -> zmax_Bs.
    """

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
# MODEL
# =========================


@dataclass
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
        # The original Fortran code cannot safely handle water_on=1 and sedi_on=1.
        # Therefore water has priority and sediment is disabled in water-covered cells.
        if self.water_on:
            return 0
        return int(self.sediment_thickness > self.sediment_threshold)

    @property
    def shallow_interface_depth(self) -> float:
        """Top of the crustal B-spline interval in the original Fortran logic."""
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


# =========================
# WRITER
# =========================


class GridWriter:
    def __init__(self, base_dir: Path, cfg: Config):
        self.base_dir = base_dir
        self.cfg = cfg

    def write(self, grid: MCMCGrid, phase):
        grid.validate()
        out = self.base_dir / grid.folder_name
        out.mkdir(parents=True, exist_ok=True)

        self._write_phase(out, phase)
        self._write_para(out, grid)
        self._write_dram(out)

    def _write_phase(self, out: Path, phase):
        lines = [f"1 {len(phase)}"]
        for period, velocity, sigma in phase:
            if velocity is None or not np.isfinite(velocity):
                lines.append(f"2 1 1 {period:>3} NaN NaN")
            else:
                lines.append(f"2 1 1 {period:>3} {velocity:.4f} {sigma:.4f}")
        lines += ["0", "0"]
        (out / "phase.input").write_text("\n".join(lines))

    def _vs_perturbation(self, section: str, n_coeff: int) -> np.ndarray:
        """Return search half-widths for B-spline coefficients."""
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
        """Vs gradient used only below the deepest depth in vs_model_csv."""
        return float(self.cfg.search_radius.get("deep_vs_gradient", 0.001))

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

        lower = np.maximum(0.0, centers - half_widths)
        upper = centers + half_widths
        return list(zip(lower, upper))

    def _write_para(self, out: Path, grid: MCMCGrid):
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
            self.cfg.reference_water_model if grid.water_on else self.cfg.reference_model
        )

        if grid.sediment_on:
            radius = float(sr.get("sediment", 0.0))
            low = max(0.0, grid.sediment_thickness - radius)
            high = grid.sediment_thickness + radius
            lines.append(f"0 0 {low:.2f} {high:.2f}")

        moho_radius = float(sr.get("moho", 0.0))
        lines.append(
            f"0 1 {grid.moho_depth - moho_radius:.2f} "
            f"{grid.moho_depth + moho_radius:.2f}"
        )

        if grid.sediment_on:
            for i, bounds in enumerate(self.cfg.sediment_vs, start=1):
                lines.append(f"10 {i} {bounds[0]:.3f} {bounds[1]:.3f}")

        crust_bounds = self._bspline_bounds(
            grid=grid,
            z_top=grid.crustal_spline_top,
            z_bottom=grid.moho_depth,
            n_coeff=len(self.cfg.crust_vs),
            section="crust",
        )
        for i, (low, high) in enumerate(crust_bounds, start=1):
            lines.append(f"1 {i} {low:.3f} {high:.3f}")

        mantle_bounds = self._bspline_bounds(
            grid=grid,
            z_top=grid.moho_depth,
            z_bottom=grid.max_depth,
            n_coeff=len(self.cfg.mantle_vs),
            section="mantle",
        )
        for i, (low, high) in enumerate(mantle_bounds, start=1):
            lines.append(f"2 {i} {low:.3f} {high:.3f}")

        (out / "para.inp").write_text("\n".join(lines))

    def _write_dram(self, out: Path):
        p = self.cfg.mcmc_params
        header = " | ".join(
            [
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
        )
        values = (
            f"{p['mineos_on']} {p['nsimu']} {p['inm']} {p['nc']} {p['adaptint']} "
            f"{p['imat_fac']} {p['verbo']} {p['dodr']} {p['sigma2']} "
            f"{p['DRscale']} {p['iresetad']} {p['id_run']} {p['biasfac']} "
            f"{p['burn_in']} {p['out_best']}"
        )
        (out / "input_DRAM_T.dat").write_text(header + "\n" + values)


# =========================
# RUNNER
# =========================


def normalize_phase_values(phase_values):
    phase = []
    for period, velocity, sigma in phase_values:
        if velocity is None or not np.isfinite(velocity):
            phase.append((period, None, None))
        else:
            if sigma is None or not np.isfinite(sigma):
                sigma = 0.02
            phase.append((period, float(velocity), float(sigma)))
    return phase


def process_point(args):
    lon, lat, topo, sediment, moho, phase_values, vs_profile, cfg = args

    grid = MCMCGrid(
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

    return grid, normalize_phase_values(phase_values)


def init_grids(config_path, max_workers=1):
    cfg = load_config(config_path)
    base_dir = Path(cfg.paths.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    topo = surface_grid(
        load_etopo_xyz(cfg.paths.etopo_nc, cfg.region), cfg.region, cfg.grid_spacing
    ).values
    sediment = surface_grid(
        np.loadtxt(cfg.paths.sed_xyz), cfg.region, cfg.grid_spacing
    ).values
    moho = surface_grid(
        np.loadtxt(cfg.paths.moho_xyz), cfg.region, cfg.grid_spacing
    ).values

    ny, nx = topo.shape
    lon_values = np.linspace(cfg.region[0], cfg.region[1], nx)
    lat_values = np.linspace(cfg.region[2], cfg.region[3], ny)
    lon_grid, lat_grid = np.meshgrid(lon_values, lat_values)

    tpwt = build_dispersion(cfg.paths.tpwt_csv, cfg.periods, cfg.region, cfg.grid_spacing)
    ant = build_dispersion(cfg.paths.ant_csv, cfg.periods, cfg.region, cfg.grid_spacing)
    phv, std = fuse_dispersion(tpwt, ant, cfg.periods)

    vs_library = VsModelLibrary.from_csv(cfg.paths.vs_model_csv)

    lon_flat = lon_grid.ravel()
    lat_flat = lat_grid.ravel()
    topo_flat = topo.ravel()
    sediment_flat = sediment.ravel()
    moho_flat = moho.ravel()

    tasks = []
    for k, (lon, lat) in enumerate(zip(lon_flat, lat_flat)):
        phase_values = []
        for period in cfg.periods:
            velocity_grid = phv.get(period)
            sigma_grid = std.get(period)

            if velocity_grid is None:
                phase_values.append((period, None, None))
            else:
                velocity = velocity_grid.ravel()[k]
                sigma = None if sigma_grid is None else sigma_grid.ravel()[k]
                phase_values.append((period, velocity, sigma))

        tasks.append(
            (
                lon,
                lat,
                topo_flat[k],
                sediment_flat[k],
                moho_flat[k],
                phase_values,
                vs_library.nearest_profile(lon, lat),
                cfg,
            )
        )

    writer = GridWriter(base_dir, cfg)

    if max_workers > 1:
        with ProcessPoolExecutor(max_workers) as executor:
            for grid, phase in tqdm(executor.map(process_point, tasks), total=len(tasks)):
                writer.write(grid, phase)
    else:
        for task in tqdm(tasks):
            grid, phase = process_point(task)
            writer.write(grid, phase)

    print("MCMC grids initialized.")


if __name__ == "__main__":
    init_grids("data/mcmc/config.json", max_workers=4)
