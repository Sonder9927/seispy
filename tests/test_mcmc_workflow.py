import json

import numpy as np
import pandas as pd
import pytest

from seispy.mcmc.config import load_config
from seispy.mcmc.dispersion import DispersionGrid
from seispy.mcmc.gridding import TargetGrid
from seispy.mcmc.inputs import coordinate_pair_key
from seispy.mcmc.priors import PriorSettings
from seispy.mcmc.spatial import SpatialFields
from seispy.mcmc.velocity import VsModelLibrary, VsProfile
from seispy.mcmc.workflow import init_grids, plot_grids, prepare_points

REGION = [0.0, 1.0, 0.0, 1.0]
NODES = [(0.0, 0.0), (1.0, 0.0), (0.0, 1.0), (1.0, 1.0)]


def _profile(lon, lat, depths=(0.0, 10.0, 300.0), vs=(3.0, 3.4, 4.7)):
    return VsProfile(lon, lat, np.asarray(depths), np.asarray(vs))


def _stub_dispersion(target, value=3.5, sigma=0.03):
    shape = (1,) + target.shape
    return DispersionGrid(
        periods=np.array([10.0]),
        velocities=np.full(shape, value),
        sigmas=np.full(shape, sigma),
    )


def _write_synthetic_inputs(tmp_path):
    """Write a minimal complete 2x2 MCMC dataset and return its config path."""

    topography = tmp_path / "topography.csv"
    sediment = tmp_path / "sediment.csv"
    moho = tmp_path / "moho.csv"
    reference = tmp_path / "reference.csv"
    phase = tmp_path / "phase.csv"

    pd.DataFrame(
        {"x": [n[0] for n in NODES], "y": [n[1] for n in NODES], "z": np.zeros(4)}
    ).to_csv(topography, index=False)
    pd.DataFrame(
        {
            "x": [n[0] for n in NODES],
            "y": [n[1] for n in NODES],
            "sediment_thickness": np.zeros(4),
        }
    ).to_csv(sediment, index=False)
    pd.DataFrame(
        {
            "x": [n[0] for n in NODES],
            "y": [n[1] for n in NODES],
            "moho": np.full(4, 40.0),
        }
    ).to_csv(moho, index=False)

    # Geologically plausible layered profile: upper crust, lower crust and a
    # Moho step into the uppermost mantle.
    layers = ((0.0, 3.0), (10.0, 3.4), (40.0, 3.9), (40.1, 4.5), (300.0, 4.6))
    rows = [(n[0], n[1], z, vs) for n in NODES for z, vs in layers]
    pd.DataFrame(rows, columns=["lon", "lat", "z", "vs"]).to_csv(reference, index=False)

    periods = np.arange(5.0, 11.0)
    phase_rows = [(period, n[0], n[1], 3.5, 0.03) for n in NODES for period in periods]
    pd.DataFrame(phase_rows, columns=["period", "lon", "lat", "phv", "std"]).to_csv(
        phase, index=False
    )

    config = {
        "region": REGION,
        "grid_spacing": 1.0,
        "paths": {
            "topography_file": str(topography),
            "sediment_file": str(sediment),
            "moho_file": str(moho),
            "vs_model_file": str(reference),
            "phase_dispersion_file": str(phase),
            "output_dir": str(tmp_path / "grids"),
        },
        "search_radius": {
            "sediment": 0.2,
            "moho": 1.0,
            "crust_vs": 0.1,
            "mantle_vs": 0.1,
        },
        "mcmc_params": {
            "mineos_on": 0,
            "nsimu": 100,
            "inm": 1,
            "nc": 1,
            "adaptint": 10,
            "imat_fac": 1.0,
            "verbo": 0,
            "dodr": 1,
            "sigma2": 1.0,
            "DRscale": 2.0,
            "iresetad": 0,
            "id_run": 1,
            "biasfac": 0.0,
            "burn_in": 10,
            "out_best": 1,
        },
        "water_threshold": 1.0,
        "sediment_threshold": 2.0,
        "sediment_vs": [[0.2, 2.5], [0.5, 3.0]],
        "n_coeff_crust": 4,
        "n_coeff_mantle": 5,
        "sm_on": 0,
        "ice_on": 0,
        "factor": 2.0,
        "zmax_Bs": 300.0,
        "NPTS_cBs": 21,
        "NPTS_mBs": 41,
        "reference_model": "prem_noocean.txt",
        "reference_water_model": "prem_ocean.txt",
        "phase_constraints": {"minimum_periods": 5, "skip_if_insufficient": True},
    }
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path


def test_init_grids_writes_one_directory_per_point(tmp_path):
    config_path = _write_synthetic_inputs(tmp_path)
    init_grids(config_path)

    for lon, lat in NODES:
        point_dir = tmp_path / "grids" / f"{lon:.2f}_{lat:.2f}"
        assert sorted(p.name for p in point_dir.iterdir()) == [
            "input_DRAM_T.dat",
            "para.inp",
            "phase.input",
            "point.json",
            "prior_bounds.csv",
        ]
        assert (point_dir / "phase.input").read_text().count("\n2 1 1") == 6
        para = (point_dir / "para.inp").read_text().splitlines()
        assert para[0] == "0" and para[1] == "0" and para[2] == "0"
        assert para[3] == "0"


def test_init_grids_skips_points_with_too_few_periods(tmp_path):
    config_path = _write_synthetic_inputs(tmp_path)
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["phase_constraints"]["minimum_periods"] = 50
    config_path.write_text(json.dumps(raw), encoding="utf-8")

    init_grids(config_path)
    assert not (tmp_path / "grids").exists() or not any((tmp_path / "grids").iterdir())


def test_init_grids_writes_per_point_figure(tmp_path):
    pytest.importorskip("matplotlib")
    config_path = _write_synthetic_inputs(tmp_path)
    init_grids(config_path, plot=True)
    figure = tmp_path / "grids" / "0.00_0.00" / "point.png"
    assert figure.is_file() and figure.stat().st_size > 0


def test_plot_grids_redraws_from_written_directories(tmp_path):
    pytest.importorskip("matplotlib")
    config_path = _write_synthetic_inputs(tmp_path)
    init_grids(config_path)
    figure = tmp_path / "grids" / "0.00_0.00" / "point.png"
    assert not figure.exists()

    assert plot_grids(tmp_path / "grids") == len(NODES)
    assert figure.is_file() and figure.stat().st_size > 0


def test_plot_grids_reports_a_broken_point_without_losing_inputs(tmp_path, monkeypatch):
    pytest.importorskip("matplotlib")
    config_path = _write_synthetic_inputs(tmp_path)
    init_grids(config_path)

    def boom(directory, *, dpi=300):
        raise RuntimeError("boom")

    monkeypatch.setattr("seispy.mcmc.plotting.plot_point_dir", boom)
    assert plot_grids(tmp_path / "grids") == 0
    assert (tmp_path / "grids" / "0.00_0.00" / "para.inp").is_file()


def _stub_fields(target, moho=40.0):
    shape = target.shape
    return SpatialFields(
        target=target,
        topo=np.zeros(shape),
        sediment=np.zeros(shape),
        moho=np.full(shape, moho),
    )


def test_prepare_points_reports_colliding_folder_names(write_mcmc_config):
    cfg = load_config(
        write_mcmc_config(region=[0.001, 0.004, 0.001, 0.004], grid_spacing=0.003)
    )
    target = TargetGrid.from_region(cfg.region, cfg.grid_spacing)
    lons, lats = target.flat_lonlat()
    profiles = {
        coordinate_pair_key(float(lon), float(lat)): _profile(float(lon), float(lat))
        for lon, lat in zip(lons, lats, strict=True)
    }
    library = VsModelLibrary(profiles=profiles)
    settings = PriorSettings.from_config(cfg)

    with pytest.raises(ValueError, match="collide after two-decimal formatting"):
        prepare_points(
            _stub_fields(target), library, _stub_dispersion(target), cfg, settings
        )


def test_prepare_points_reports_missing_profiles(write_mcmc_config):
    cfg = load_config(write_mcmc_config())
    target = TargetGrid.from_region(cfg.region, cfg.grid_spacing)
    library = VsModelLibrary(
        profiles={coordinate_pair_key(0.0, 0.0): _profile(0.0, 0.0)}
    )
    settings = PriorSettings.from_config(cfg)

    with pytest.raises(ValueError, match="missing 3 inversion-grid profiles"):
        prepare_points(
            _stub_fields(target), library, _stub_dispersion(target), cfg, settings
        )


def test_prepare_points_reports_shallow_profiles(write_mcmc_config):
    cfg = load_config(write_mcmc_config())
    target = TargetGrid.from_region(cfg.region, cfg.grid_spacing)
    lons, lats = target.flat_lonlat()
    library = VsModelLibrary(
        profiles={
            coordinate_pair_key(float(lon), float(lat)): _profile(
                float(lon), float(lat), depths=(0.0, 10.0), vs=(3.0, 3.4)
            )
            for lon, lat in zip(lons, lats, strict=True)
        }
    )
    settings = PriorSettings.from_config(cfg)

    with pytest.raises(ValueError, match="below zmax_Bs"):
        prepare_points(
            _stub_fields(target), library, _stub_dispersion(target), cfg, settings
        )
