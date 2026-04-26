import json
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import List

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


def load_config(path: str) -> Config:
    with open(path) as f:
        raw = json.load(f)
    raw["paths"] = Paths(**raw["paths"])
    return Config(**raw)


# =========================
# GRID (统一 surface)
# =========================


def surface_grid(xyz, region, spacing):
    import pygmt
    # ---------- blockmean ----------
    xyz_block = pygmt.blockmean(data=xyz, region=region, spacing=spacing)

    # ---------- 判断网格大小 ----------
    xmin, xmax, ymin, ymax = region
    nx = int((xmax - xmin) / spacing + 1)
    ny = int((ymax - ymin) / spacing + 1)

    if nx < 4 or ny < 4:
        print(f"[WARN] small grid ({nx}x{ny}), fallback xyz2grd")
        return pygmt.xyz2grd(data=xyz_block, region=region, spacing=spacing)

    # ---------- surface ----------
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

    for p in periods:
        sub = df[df["period"] == p]

        if len(sub) == 0:
            out[p] = (None, None)
            continue

        phv = surface_grid(
            sub[["longitude", "latitude", "phv"]].values, region, spacing
        )

        if "std" in sub.columns:
            std = (
                surface_grid(
                    sub[["longitude", "latitude", "std"]].values, region, spacing
                )
                * 0.001
            )
        else:
            std = None

        out[p] = (phv.values, None if std is None else std.values)

    return out


def safe_nanmean(arr_list):
    if len(arr_list) == 0:
        return None
    arr = np.array(arr_list)
    if np.all(np.isnan(arr)):
        return None
    return np.nanmean(arr, axis=0)


def fuse_dispersion(tpwt, ant, periods):
    phv_out = {}
    std_out = {}

    for p in periods:
        g1, s1 = tpwt.get(p, (None, None))
        g2, s2 = ant.get(p, (None, None))

        phv = safe_nanmean([g for g in [g1, g2] if g is not None])
        std = safe_nanmean([s for s in [s1, s2] if s is not None])

        phv_out[p] = phv
        std_out[p] = std

    return phv_out, std_out


# =========================
# MODEL
# =========================


@dataclass
class MCMCGrid:
    lon: float
    lat: float
    water_depth: float
    sediment_thick: float
    moho_depth: float
    water_thres: float
    sedi_thres: float
    sm_on: int  # smooth
    ice_on: int

    @property
    def folder_name(self):
        return f"{self.lon:.2f}_{self.lat:.2f}"

    @property
    def water_on(self):
        return int(self.water_depth > self.water_thres)

    @property
    def sedi_on(self):
        return int(self.sediment_thick > self.sedi_thres)


# =========================
# WRITER
# =========================


class GridWriter:
    def __init__(self, base_dir: Path, cfg: Config):
        self.base_dir = base_dir
        self.cfg = cfg

    def write(self, grid: MCMCGrid, phase):
        out = self.base_dir / grid.folder_name
        out.mkdir(parents=True, exist_ok=True)

        self._phase(out, phase)
        self._para(out, grid)
        self._dram(out)

    def _phase(self, out, phase):
        lines = [f"1 {len(phase)}"]
        for p, v, s in phase:
            if v is None:
                lines.append("2 1 1 " + f"{p:>3} NaN NaN")
            else:
                lines.append("2 1 1 " + f"{p:>3} {v:.4f} {s:.4f}")
        lines += ["0", "0"]
        (out / "phase.input").write_text("\n".join(lines))

    def _para(self, out, grid):
        sr = self.cfg.search_radius

        lines = [str(grid.sm_on), str(grid.ice_on), str(grid.water_on)]
        if grid.water_on:
            lines.append(str(grid.water_depth))

        lines.append(str(grid.sedi_on))
        lines += [
            str(self.cfg.factor),
            str(self.cfg.zmax_Bs),
            str(self.cfg.NPTS_cBs),
            str(self.cfg.NPTS_mBs),
        ]
        if grid.water_on:
            lines.append(self.cfg.reference_water_model)
        else:
            lines.append(self.cfg.reference_model)

        if grid.sedi_on:
            r = sr["sediment"]
            lines.append(
                f"0 0 {max(0, grid.sediment_thick - r):.2f} {grid.sediment_thick + r:.2f}"
            )

        r = sr["moho"]
        lines.append(f"0 1 {grid.moho_depth - r:.2f} {grid.moho_depth + r:.2f}")

        if grid.sedi_on:
            lines.append("10 1 1.0 3.5")
            lines.append("10 2 1.0 3.5")

        for i, cvs in enumerate(self.cfg.crust_vs):
            lines.append(f"1 {i + 1} {cvs[0]} {cvs[1]}")
        for i, mvs in enumerate(self.cfg.mantle_vs):
            lines.append(f"2 {i + 1} {mvs[0]} {mvs[1]}")

        (out / "para.inp").write_text("\n".join(lines))

    def _dram(self, out):
        p = self.cfg.mcmc_params
        line1 = " | ".join(
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
        line2 = (
            f"{p['mineos_on']} {p['nsimu']} {p['inm']} {p['nc']} {p['adaptint']} "
            f"{p['imat_fac']} {p['verbo']} {p['dodr']} {p['sigma2']} "
            f"{p['DRscale']} {p['iresetad']} {p['id_run']} {p['biasfac']} "
            f"{p['burn_in']} {p['out_best']}"
        )
        (out / "input_DRAM_T.dat").write_text(line1 + "\n" + line2)


# =========================
# RUNNER
# =========================


def process_point(args):
    lon, lat, topo, sed, moho, phase_values, cfg = args

    grid = MCMCGrid(
        lon=lon,
        lat=lat,
        sm_on=cfg.sm_on,
        ice_on=cfg.ice_on,
        water_depth=max(0, -topo / 1000),
        sediment_thick=sed,
        moho_depth=abs(moho),
        water_thres=cfg.water_threshold,
        sedi_thres=cfg.sediment_threshold,
    )

    phase = []
    for p, v, s in phase_values:
        if v is None:
            phase.append((p, None, None))
        else:
            if s is None or np.isnan(s):
                s = 0.02
            phase.append((p, v, s))

    return grid, phase


def init_grids(config_path, max_workers=1):
    cfg = load_config(config_path)
    base = Path(cfg.paths.output_dir)
    base.mkdir(parents=True, exist_ok=True)

    # ---------- grid ----------
    topo = surface_grid(
        load_etopo_xyz(cfg.paths.etopo_nc, cfg.region), cfg.region, cfg.grid_spacing
    ).values

    sed = surface_grid(
        np.loadtxt(cfg.paths.sed_xyz), cfg.region, cfg.grid_spacing
    ).values

    moho = surface_grid(
        np.loadtxt(cfg.paths.moho_xyz), cfg.region, cfg.grid_spacing
    ).values

    ny, nx = topo.shape

    lon_vals = np.linspace(cfg.region[0], cfg.region[1], nx)
    lat_vals = np.linspace(cfg.region[2], cfg.region[3], ny)
    lon, lat = np.meshgrid(lon_vals, lat_vals)

    # ---------- dispersion ----------
    tpwt = build_dispersion(
        cfg.paths.tpwt_csv, cfg.periods, cfg.region, cfg.grid_spacing
    )
    ant = build_dispersion(cfg.paths.ant_csv, cfg.periods, cfg.region, cfg.grid_spacing)

    phv, std = fuse_dispersion(tpwt, ant, cfg.periods)

    # ---------- flatten ----------
    lon_f = lon.ravel()
    lat_f = lat.ravel()
    topo_f = topo.ravel()
    sed_f = sed.ravel()
    moho_f = moho.ravel()

    tasks = []

    for k in range(len(lon_f)):
        phase_values = []
        for p in cfg.periods:
            vgrid = phv.get(p)
            sgrid = std.get(p)

            if vgrid is None:
                phase_values.append((p, None, None))
            else:
                v = vgrid.ravel()[k]
                s = None if sgrid is None else sgrid.ravel()[k]
                phase_values.append((p, v, s))

        tasks.append(
            (lon_f[k], lat_f[k], topo_f[k], sed_f[k], moho_f[k], phase_values, cfg)
        )

    writer = GridWriter(base, cfg)

    if max_workers > 1:
        with ProcessPoolExecutor(max_workers) as exe:
            for grid, phase in tqdm(exe.map(process_point, tasks), total=len(tasks)):
                writer.write(grid, phase)
    else:
        for t in tqdm(tasks):
            grid, phase = process_point(t)
            writer.write(grid, phase)

    print("MCMC grids initialized.")


if __name__ == "__main__":
    init_grids("data/mcmc/mcmc-config.json", max_workers=4)
