import json

import numpy as np
import pytest

from seispy.mcmc.config import (
    FORTRAN_DEEPEST_VS_MIN,
    FORTRAN_VS_MAX,
    FORTRAN_VS_MIN,
    InputUnits,
    SearchRadius,
    VsConstraints,
    expand_half_widths,
    load_config,
)


def test_load_config_resolves_paths_against_the_config_directory(
    tmp_path, write_mcmc_config
):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["paths"] = {
        "topography_file": "topography.csv",
        "sediment_file": "sediment.csv",
        "moho_file": "moho.csv",
        "vs_model_file": "reference.csv",
        "phase_dispersion_file": "phase.csv",
        "output_dir": "output",
    }
    raw["input_units"] = {
        "topography": "km",
        "sediment": "m",
        "moho": "m",
        "vs": "m/s",
        "phase_velocity": "m/s",
    }
    config_path.write_text(json.dumps(raw), encoding="utf-8")

    cfg = load_config(config_path)

    assert cfg.paths.topography_file == tmp_path / "topography.csv"
    assert cfg.paths.output_dir == tmp_path / "output"
    assert cfg.input_units.topography_to_m == 1000.0
    assert cfg.input_units.sediment_to_km == 0.001
    assert cfg.input_units.moho_to_km == 0.001
    assert cfg.input_units.vs_to_km_s == 0.001
    assert cfg.input_units.phase_velocity_to_km_s == 0.001


def test_input_units_factor_round_trips_and_rejects_unknown_units():
    assert InputUnits(topography="m").topography_to_m == 1.0
    with pytest.raises(ValueError, match="input_units.vs"):
        InputUnits(vs="m")


def test_load_config_rejects_non_divisible_spacing(write_mcmc_config):
    config_path = write_mcmc_config(grid_spacing=0.3)
    with pytest.raises(ValueError, match="divisible"):
        load_config(config_path)


def test_load_config_rejects_unknown_and_missing_keys(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["grid_spcing"] = 0.5
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="Unknown configuration keys: grid_spcing"):
        load_config(config_path)

    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    del raw["n_coeff_crust"]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required keys: n_coeff_crust"):
        load_config(config_path)


def test_load_config_rejects_unknown_section_key(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["search_radius"]["crust"] = 0.3
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="search_radius has unknown keys: crust"):
        load_config(config_path)


def test_load_config_rejects_unknown_and_missing_path_keys(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["paths"]["topgraphy_file"] = raw["paths"].pop("topography_file")
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="paths has unknown keys: topgraphy_file"):
        load_config(config_path)

    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    del raw["paths"]["moho_file"]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="paths is missing required keys: moho_file"):
        load_config(config_path)


def test_load_config_rejects_non_object_sections(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["search_radius"] = [0.2, 1.0]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="search_radius must be a JSON object"):
        load_config(config_path)


def test_deprecated_min_dispersion_points_is_now_an_unknown_key(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["min_dispersion_points"] = 5
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(
        ValueError, match="Unknown configuration keys: min_dispersion_points"
    ):
        load_config(config_path)


def test_search_radius_requires_finite_nonnegative_values():
    with pytest.raises(ValueError, match="search_radius.sediment"):
        SearchRadius(sediment=-1.0, moho=1.0)
    with pytest.raises(ValueError, match="search_radius.crust_vs"):
        SearchRadius(sediment=0.2, moho=1.0, crust_vs=[[0.1]])
    with pytest.raises(ValueError, match="search_radius.mantle_vs values"):
        SearchRadius(sediment=0.2, moho=1.0, mantle_vs=float("nan"))


def test_per_coefficient_half_width_length_is_validated_at_load(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["search_radius"]["crust_vs"] = [0.1, 0.2]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="search_radius.crust_vs has 2 values"):
        load_config(config_path)


def test_sediment_centres_must_increase_at_load(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["sediment_vs"] = [[0.5, 2.5], [0.0, 1.0]]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="search centres must be strictly increasing"):
        load_config(config_path)


@pytest.mark.parametrize("cap", [float("nan"), float("inf"), 0.0, 0.5, 5.0])
def test_invalid_crust_cap_is_rejected(cap):
    with pytest.raises(ValueError, match="crust_vs_max"):
        VsConstraints(crust_vs_max=cap)


def test_vs_constraints_cannot_relax_fortran_limits():
    assert VsConstraints().global_vs_max == FORTRAN_VS_MAX
    assert VsConstraints().no_shallow_layers_vs_min == FORTRAN_VS_MIN
    assert VsConstraints().deepest_vs_min == FORTRAN_DEEPEST_VS_MIN
    with pytest.raises(ValueError, match="Fortran limit"):
        VsConstraints(global_vs_max=5.0)
    with pytest.raises(ValueError, match="Fortran limit"):
        VsConstraints(no_shallow_layers_vs_min=0.4)
    with pytest.raises(ValueError, match="Fortran limit"):
        VsConstraints(deepest_vs_min=3.9)


def test_moho_vs_jump_defaults_to_zero_and_is_validated():
    assert VsConstraints().moho_vs_jump == 0.0
    for jump in (-0.1, float("nan"), FORTRAN_VS_MAX, 5.0):
        with pytest.raises(ValueError, match="moho_vs_jump"):
            VsConstraints(moho_vs_jump=jump)


def test_mcmc_params_validation(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["mcmc_params"]["burn_in"] = 200
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="burn_in must be smaller than nsimu"):
        load_config(config_path)

    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["mcmc_params"]["nsimu"] = 10.5
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="mcmc_params.nsimu must be an integer"):
        load_config(config_path)


def test_mcmc_params_preserve_fortran_order(write_mcmc_config):
    cfg = load_config(write_mcmc_config())
    names = [name for name, _ in cfg.mcmc_params.ordered_items()]
    assert names == [
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


def test_expand_half_widths():
    np.testing.assert_allclose(expand_half_widths(0.3, 4, "x"), np.full(4, 0.3))
    np.testing.assert_allclose(
        expand_half_widths([0.1, 0.2, 0.3], 3, "x"), [0.1, 0.2, 0.3]
    )
    with pytest.raises(ValueError, match="has 2 values, expected 3"):
        expand_half_widths([0.1, 0.2], 3, "x")
