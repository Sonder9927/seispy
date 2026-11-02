"""Configuration contracts at the JSON and Fortran boundaries."""

import json

import numpy as np
import pytest

from seispy.mcmc.config import (
    SearchRadius,
    VsConstraints,
    expand_half_widths,
    load_config,
)


def test_configuration_loading(tmp_path, write_mcmc_config):
    path = write_mcmc_config()
    raw = json.loads(path.read_text())
    raw["paths"] = {key: f"{key}.csv" for key in raw["paths"]}
    raw["paths"]["output_dir"] = "output"
    raw["input_units"] = dict(
        topography="km", sediment="m", moho="m", vs="m/s", phase_velocity="m/s"
    )
    path.write_text(json.dumps(raw))
    cfg = load_config(path)
    assert cfg.paths.topography_file == tmp_path / "topography_file.csv"
    assert cfg.paths.output_dir == tmp_path / "output"
    assert cfg.input_units.topography_to_m == 1000
    for field in (
        "sediment_to_km",
        "moho_to_km",
        "vs_to_km_s",
        "phase_velocity_to_km_s",
    ):
        assert getattr(cfg.input_units, field) == 0.001


@pytest.mark.parametrize(
    "section,key,value,error",
    [
        (None, "grid_spacing", 0.3, "divisible"),
        (None, "grid_spcing", 0.5, "Unknown configuration keys"),
        (None, "n_coeff_crust", None, "missing required keys"),
        (None, "search_radius", [], "must be a JSON object"),
        ("search_radius", "crust", 0.3, "unknown keys"),
        ("paths", "topgraphy_file", "x.csv", "unknown keys"),
        ("paths", "moho_file", None, "missing required keys"),
        ("search_radius", "crust_vs", [0.1, 0.2], "has 2 values"),
        (None, "sediment_vs", [[0.5, 2.5], [0, 1]], "strictly increasing"),
        ("mcmc_params", "burn_in", 200, "burn_in must be smaller"),
        ("mcmc_params", "nsimu", 10.5, "must be an integer"),
        ("input_units", "vs", "m", "input_units.vs"),
    ],
)
def test_invalid_configuration(write_mcmc_config, section, key, value, error):
    path = write_mcmc_config()
    raw = json.loads(path.read_text())
    target = raw if section is None else raw.setdefault(section, {})
    if value is None:
        del target[key]
    else:
        target[key] = value
    path.write_text(json.dumps(raw))
    with pytest.raises(ValueError, match=error):
        load_config(path)


@pytest.mark.parametrize(
    "constructor,options,error",
    [
        (SearchRadius, dict(sediment=-1, moho=1), "search_radius.sediment"),
        (
            SearchRadius,
            dict(sediment=0.2, moho=1, crust_vs=[[0.1]]),
            "search_radius.crust_vs",
        ),
        (
            SearchRadius,
            dict(sediment=0.2, moho=1, mantle_vs=float("nan")),
            "search_radius.mantle_vs",
        ),
        (VsConstraints, dict(global_vs_max=5), "Fortran limit"),
        (VsConstraints, dict(no_shallow_layers_vs_min=0.4), "Fortran limit"),
        (VsConstraints, dict(deepest_vs_min=3.9), "Fortran limit"),
    ],
)
def test_physical_constraints(constructor, options, error):
    with pytest.raises(ValueError, match=error):
        constructor(**options)


def test_fortran_parameters(write_mcmc_config):
    cfg = load_config(write_mcmc_config())
    assert [name for name, _ in cfg.mcmc_params.ordered_items()] == [
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
    for value, count, expected in [
        (0.3, 4, [0.3] * 4),
        ([0.1, 0.2, 0.3], 3, [0.1, 0.2, 0.3]),
    ]:
        np.testing.assert_allclose(expand_half_widths(value, count, "x"), expected)
    with pytest.raises(ValueError, match="has 2 values, expected 3"):
        expand_half_widths([0.1, 0.2], 3, "x")
