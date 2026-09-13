"""MCMC configuration models and loading."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


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

    raw["vs_constraints"] = VsConstraints(**raw.get("vs_constraints", {}))
    raw["phase_constraints"] = PhaseConstraints(**raw.get("phase_constraints", {}))
    return Config(**raw)
