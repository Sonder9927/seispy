"""Shared fixtures for the test suite."""

from __future__ import annotations

import importlib.util
import itertools
import json
import sys
from copy import deepcopy

import numpy as np
import pytest

# xarray discovers optional IO backends through entry points. Importing the
# ``gmt`` backend initializes PyGMT, and in environments where GMT cannot create
# its session directory that import leaves pyarrow's filesystem registry
# unusable, breaking unrelated parquet reads. No seispy feature uses PyGMT, so
# hide it from xarray's plugin scan for the whole session.
sys.modules.setdefault("pygmt", None)

BASE_CONFIG = {
    "region": [0.0, 1.0, 0.0, 1.0],
    "grid_spacing": 1.0,
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
    # Two sediment parameters, matching the reference executable and the
    # example project configuration.
    "sediment_vs": [[0.2, 2.5], [0.5, 3.0]],
    "n_coeff_crust": 4,
    "n_coeff_mantle": 5,
    # The writer emits no smoothing-weight or ice-layer records, so both
    # switches must stay 0 for the generated para.inp to be readable.
    "sm_on": 0,
    "ice_on": 0,
    "factor": 2.0,
    "zmax_Bs": 300.0,
    "NPTS_cBs": 20,
    "NPTS_mBs": 20,
    "reference_model": "reference.txt",
    "reference_water_model": "reference_water.txt",
}
# Optional blocks (``input_units``, ``vs_constraints``, ``phase_constraints``,
# ``default_phase_std``) are deliberately omitted so tests exercise the
# documented defaults. ``vs_constraints.moho_vs_jump`` therefore stays at its
# default 0 here; the Moho-contrast tests set it explicitly.

_DEFAULT_PATHS = {
    "topography_file": "topography.csv",
    "sediment_file": "sediment.csv",
    "moho_file": "moho.csv",
    "vs_model_file": "reference.csv",
    "phase_dispersion_file": "phase.csv",
}


@pytest.fixture
def write_mcmc_config(tmp_path):
    """Return a factory that writes a valid MCMC JSON configuration."""

    counter = itertools.count()

    def write(**overrides):
        config = deepcopy(BASE_CONFIG)
        paths = {
            name: str(tmp_path / filename) for name, filename in _DEFAULT_PATHS.items()
        }
        paths["output_dir"] = str(tmp_path / "output")
        paths.update(overrides.pop("paths", {}))
        config["paths"] = paths
        config.update(overrides)

        path = tmp_path / f"config_{next(counter)}.json"
        path.write_text(json.dumps(config), encoding="utf-8")
        return path

    return write


@pytest.fixture
def requires_pyarrow():
    """Skip the test when the optional Parquet engine is not installed."""

    if importlib.util.find_spec("pyarrow") is None:
        pytest.skip("pyarrow is not installed")


@pytest.fixture
def make_profile():
    """Return a factory for reference Vs profiles."""

    from seispy.mcmc.velocity import VsProfile

    def make(
        lon: float = 0.0,
        lat: float = 0.0,
        depths=(0.0, 5.0, 40.0, 300.0),
        vs=(3.0, 3.2, 4.2, 4.7),
    ) -> VsProfile:
        return VsProfile(
            lon, lat, np.asarray(depths, dtype=float), np.asarray(vs, dtype=float)
        )

    return make
