"""MCMC configuration models and loading."""

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import numpy as np


@dataclass(frozen=True)
class Paths:
    topography_file: str
    sediment_file: str
    moho_file: str
    vs_model_file: str
    phase_dispersion_file: str
    output_dir: str


@dataclass(frozen=True)
class SearchRadius:
    """Search radii used when constructing per-grid MCMC priors.

    Depth radii are in km. ``crust_vs`` and ``mantle_vs`` are Vs half-widths
    in km/s and may be either one scalar shared by all coefficients or one
    value per coefficient.
    """

    sediment: float
    moho: float
    crust_vs: float | List[float]
    mantle_vs: float | List[float]

    def __post_init__(self) -> None:
        for name in ("sediment", "moho"):
            value = float(getattr(self, name))
            if not np.isfinite(value) or value < 0:
                raise ValueError(
                    f"search_radius.{name} must be finite and >= 0, got {value}"
                )

        for name in ("crust_vs", "mantle_vs"):
            arr = np.asarray(getattr(self, name), dtype=float)
            if arr.ndim > 1 or arr.size == 0:
                raise ValueError(
                    f"search_radius.{name} must be a scalar or 1-D sequence"
                )
            if not np.isfinite(arr).all() or np.any(arr < 0):
                raise ValueError(f"search_radius.{name} values must be finite and >= 0")


@dataclass(frozen=True)
class MCMCParams:
    """Parameters written to ``input_DRAM_T.dat`` in Fortran input order."""

    mineos_on: int
    nsimu: int
    inm: int
    nc: int
    adaptint: int
    imat_fac: float
    verbo: int
    dodr: int
    sigma2: float
    DRscale: float
    iresetad: int
    id_run: int
    biasfac: float
    burn_in: int
    out_best: int

    def ordered_items(self) -> list[tuple[str, int | float]]:
        names = (
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
        )
        return [(name, getattr(self, name)) for name in names]


@dataclass(frozen=True)
class VsConstraints:
    """Physical constraints for Vs-related search bounds in para.inp.

    Values are in km/s. The reference Vs model is required to cover the full
    MCMC inversion depth, so deep extrapolation is not used during bound
    construction.

    ``*_soft_max`` and ``*_hard_max`` define a two-level upper-bound strategy.
    If the reference Vs center is below the soft maximum, the normal search
    interval is used. If it lies between the soft and hard maxima, the upper
    bound is clipped at the hard maximum. If it exceeds the hard maximum, the
    search window is shifted below the hard maximum so that the original search
    width is largely preserved.
    """

    sediment_max: float = 3.0
    crust_soft_max: float = 3.9
    crust_hard_max: float = 4.0
    mantle_soft_max: float = 4.9
    mantle_hard_max: float = 5.0
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
    search_radius: SearchRadius
    mcmc_params: MCMCParams
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
    raw["search_radius"] = SearchRadius(**raw["search_radius"])
    raw["mcmc_params"] = MCMCParams(**raw["mcmc_params"])
    raw["vs_constraints"] = VsConstraints(**raw.get("vs_constraints", {}))
    raw["phase_constraints"] = PhaseConstraints(**raw.get("phase_constraints", {}))
    return Config(**raw)
