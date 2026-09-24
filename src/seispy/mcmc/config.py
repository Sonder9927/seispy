"""MCMC configuration models and JSON loading.

The configuration is a flat JSON document. Every value is validated eagerly so
a bad setup fails before any source data is read or any output directory is
created.
"""

from __future__ import annotations

import dataclasses
import json
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, get_type_hints

import numpy as np

from seispy.mcmc.gridding import target_shape

# Hard capabilities of the reference Fortran executable. These are input-domain
# limits of that program, not physical or geophysical constants.
FORTRAN_VS_MAX = 4.9
FORTRAN_VS_MIN = 0.5
FORTRAN_DEEPEST_VS_MIN = 4.0

# Unit scale relative to the smallest unit of each quantity. Conversions are
# computed as ``scale[source] / scale[target]``.
_UNIT_SCALES: dict[str, dict[str, float]] = {
    "topography": {"m": 1.0, "km": 1000.0},
    "sediment": {"m": 1.0, "km": 1000.0},
    "moho": {"m": 1.0, "km": 1000.0},
    "vs": {"m/s": 1.0, "km/s": 1000.0},
    "phase_velocity": {"m/s": 1.0, "km/s": 1000.0},
    "phase_std": {"m/s": 1.0, "km/s": 1000.0},
}


def expand_half_widths(
    value: float | Sequence[float],
    n_coeff: int,
    name: str,
) -> np.ndarray:
    """Expand a scalar or per-coefficient search half-width to ``n_coeff`` values."""

    arr = np.asarray(value, dtype=float)
    if arr.ndim == 0:
        return np.full(n_coeff, float(arr))
    if arr.ndim != 1 or arr.size != n_coeff:
        raise ValueError(f"{name} has {arr.size} values, expected {n_coeff}")
    return arr.copy()


@dataclass(frozen=True)
class Paths:
    """Resolved absolute paths to the four inputs and the output directory."""

    topography_file: Path
    sediment_file: Path
    moho_file: Path
    vs_model_file: Path
    phase_dispersion_file: Path
    output_dir: Path

    def __post_init__(self) -> None:
        for item in dataclasses.fields(self):
            value = Path(getattr(self, item.name)).expanduser()
            object.__setattr__(self, item.name, value)


@dataclass(frozen=True)
class InputUnits:
    """Units declared by the user-facing input products.

    Internal MCMC conventions are metres for topography, kilometres for
    sediment/Moho depths, and km/s for velocity models and dispersion
    uncertainties.
    """

    topography: str = "m"
    sediment: str = "km"
    moho: str = "km"
    vs: str = "km/s"
    phase_velocity: str = "km/s"
    phase_std: str = "m/s"

    def __post_init__(self) -> None:
        for name, allowed in _UNIT_SCALES.items():
            value = getattr(self, name)
            if value not in allowed:
                options = ", ".join(sorted(allowed))
                raise ValueError(f"input_units.{name} must be one of: {options}")

    def factor(self, name: str, target: str) -> float:
        """Return the multiplier that converts ``name`` from its declared unit."""

        return _UNIT_SCALES[name][getattr(self, name)] / _UNIT_SCALES[name][target]

    @property
    def topography_to_m(self) -> float:
        return self.factor("topography", "m")

    @property
    def sediment_to_km(self) -> float:
        return self.factor("sediment", "km")

    @property
    def moho_to_km(self) -> float:
        return self.factor("moho", "km")

    @property
    def vs_to_km_s(self) -> float:
        return self.factor("vs", "km/s")

    @property
    def phase_velocity_to_km_s(self) -> float:
        return self.factor("phase_velocity", "km/s")

    @property
    def phase_std_to_km_s(self) -> float:
        return self.factor("phase_std", "km/s")


@dataclass(frozen=True)
class SearchRadius:
    """Search radii used when constructing per-grid MCMC priors.

    Depth radii are in km. ``crust_vs`` and ``mantle_vs`` are Vs half-widths
    in km/s and may be either one scalar shared by all coefficients or one
    value per coefficient. Their defaults are 0.30 and 0.20 km/s respectively:
    starting choices to tune for reference-model uncertainty, not universal
    geophysical limits. Sediment and Moho depth radii remain explicit.
    """

    sediment: float
    moho: float
    crust_vs: float | list[float] = 0.30
    mantle_vs: float | list[float] = 0.20

    def __post_init__(self) -> None:
        for name in ("sediment", "moho"):
            value = float(getattr(self, name))
            if not np.isfinite(value) or value < 0:
                raise ValueError(
                    f"search_radius.{name} must be finite and >= 0, got {value}"
                )
            object.__setattr__(self, name, value)

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
        """Return ``(name, value)`` pairs in declaration (= Fortran) order."""

        return [
            (item.name, getattr(self, item.name)) for item in dataclasses.fields(self)
        ]

    def validate(self) -> None:
        hints = get_type_hints(type(self))
        for item in dataclasses.fields(self):
            value = getattr(self, item.name)
            if hints[item.name] is int and (
                isinstance(value, bool) or int(value) != value
            ):
                raise ValueError(f"mcmc_params.{item.name} must be an integer")
            if not np.isfinite(float(value)):
                raise ValueError(f"mcmc_params.{item.name} must be finite")

        if self.nsimu <= 0:
            raise ValueError("mcmc_params.nsimu must be > 0")
        if self.adaptint <= 0:
            raise ValueError("mcmc_params.adaptint must be > 0")
        if self.burn_in < 0:
            raise ValueError("mcmc_params.burn_in must be >= 0")
        if self.burn_in >= self.nsimu:
            raise ValueError("mcmc_params.burn_in must be smaller than nsimu")


@dataclass(frozen=True)
class VsConstraints:
    """Physical constraints for Vs-related search bounds in para.inp.

    Values are in km/s. The reference Vs model must cover the deep end of the
    MCMC inversion interval. A finite shallow gap may be filled by linear
    extrapolation from the two shallowest reference samples; deep extrapolation
    is never used during bound construction.

    These fields describe executable limits and the extrapolation policy.
    Half-widths remain in search_radius.

    ``moho_strict_margin`` is only the numerical separation the initial model
    must respect. ``moho_vs_jump`` is deprecated and ignored: the least-squares
    projection centres already reproduce the reference model's own Moho
    contrast, so no synthetic jump is needed. A nonzero value only emits a
    DeprecationWarning.
    """

    global_vs_max: float = FORTRAN_VS_MAX
    no_shallow_layers_vs_min: float = FORTRAN_VS_MIN
    deepest_vs_min: float = FORTRAN_DEEPEST_VS_MIN
    moho_strict_margin: float = 0.001
    moho_vs_jump: float = 0.0
    allow_shallow_extrapolation: bool = True
    max_shallow_extrapolation_km: float | None = 5.0

    def __post_init__(self) -> None:
        limits = (
            ("global_vs_max", self.global_vs_max),
            ("no_shallow_layers_vs_min", self.no_shallow_layers_vs_min),
            ("deepest_vs_min", self.deepest_vs_min),
            ("moho_strict_margin", self.moho_strict_margin),
        )
        for name, value in limits:
            value = float(value)
            if not np.isfinite(value) or value < 0:
                raise ValueError(f"vs_constraints.{name} must be finite and >= 0")
            object.__setattr__(self, name, value)

        if self.max_shallow_extrapolation_km is not None:
            value = float(self.max_shallow_extrapolation_km)
            if not np.isfinite(value) or value < 0:
                raise ValueError(
                    "vs_constraints.max_shallow_extrapolation_km must be finite "
                    ">= 0, or null"
                )
            object.__setattr__(self, "max_shallow_extrapolation_km", value)

        if self.global_vs_max <= 0:
            raise ValueError("vs_constraints.global_vs_max must be > 0")
        if self.global_vs_max > FORTRAN_VS_MAX:
            raise ValueError(
                "vs_constraints.global_vs_max cannot exceed the Fortran limit "
                f"{FORTRAN_VS_MAX}"
            )
        if self.no_shallow_layers_vs_min < FORTRAN_VS_MIN:
            raise ValueError(
                "vs_constraints.no_shallow_layers_vs_min cannot be below the "
                f"Fortran limit {FORTRAN_VS_MIN}"
            )
        if self.no_shallow_layers_vs_min >= self.global_vs_max:
            raise ValueError(
                "vs_constraints.no_shallow_layers_vs_min must be below global_vs_max"
            )
        if self.deepest_vs_min > self.global_vs_max:
            raise ValueError("vs_constraints.deepest_vs_min must be <= global_vs_max")
        if self.deepest_vs_min < FORTRAN_DEEPEST_VS_MIN:
            raise ValueError(
                "vs_constraints.deepest_vs_min cannot be below the Fortran limit "
                f"{FORTRAN_DEEPEST_VS_MIN}"
            )
        if self.moho_strict_margin <= 0:
            raise ValueError("vs_constraints.moho_strict_margin must be > 0")
        object.__setattr__(self, "moho_vs_jump", float(self.moho_vs_jump))
        if self.moho_vs_jump != 0:
            warnings.warn(
                "vs_constraints.moho_vs_jump is deprecated and ignored: the "
                "least-squares projection centres already reproduce the "
                "reference Moho contrast.",
                DeprecationWarning,
                stacklevel=2,
            )


@dataclass(frozen=True)
class PhaseConstraints:
    """Quality-control settings for writing phase.input."""

    minimum_periods: int = 5
    skip_if_insufficient: bool = True

    def __post_init__(self) -> None:
        if isinstance(self.minimum_periods, bool) or not isinstance(
            self.minimum_periods, int
        ):
            raise ValueError("phase_constraints.minimum_periods must be an integer")
        if self.minimum_periods <= 0:
            raise ValueError("phase_constraints.minimum_periods must be > 0")


@dataclass(frozen=True)
class Config:
    """Validated MCMC preparation configuration."""

    region: list[float]
    grid_spacing: float
    search_radius: SearchRadius
    mcmc_params: MCMCParams
    paths: Paths
    water_threshold: float
    sediment_threshold: float
    sediment_vs: list[list[float]]
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
    input_units: InputUnits = field(default_factory=InputUnits)
    default_phase_std: float = 0.03
    vs_constraints: VsConstraints = field(default_factory=VsConstraints)
    phase_constraints: PhaseConstraints = field(default_factory=PhaseConstraints)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        region = np.asarray(self.region, dtype=float)
        if region.shape != (4,) or not np.isfinite(region).all():
            raise ValueError(
                "region must contain four finite values: xmin, xmax, ymin, ymax"
            )
        xmin, xmax, ymin, ymax = region
        if ymin < -90.0 or ymax > 90.0:
            raise ValueError("region latitude bounds must be within [-90, 90]")

        spacing = float(self.grid_spacing)
        if not np.isfinite(spacing) or spacing <= 0:
            raise ValueError("grid_spacing must be finite and > 0")
        # Delegates extent/divisibility validation to the grid module so the
        # target-grid contract has exactly one implementation.
        target_shape(region, spacing)

        for name in ("water_threshold", "sediment_threshold"):
            value = float(getattr(self, name))
            if not np.isfinite(value) or value < 0:
                raise ValueError(f"{name} must be finite and >= 0")

        for name in ("n_coeff_crust", "n_coeff_mantle", "NPTS_cBs", "NPTS_mBs"):
            value = getattr(self, name)
            if isinstance(value, bool) or int(value) != value or int(value) <= 0:
                raise ValueError(f"{name} must be a positive integer")
        if self.n_coeff_crust < 3 or self.n_coeff_mantle < 3:
            raise ValueError("n_coeff_crust and n_coeff_mantle must be >= 3")

        for name, value in (("sm_on", self.sm_on), ("ice_on", self.ice_on)):
            if value not in (0, 1):
                raise ValueError(f"{name} must be 0 or 1")

        for name in ("factor", "zmax_Bs", "default_phase_std"):
            value = float(getattr(self, name))
            if not np.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be finite and > 0")

        # Validate per-coefficient Vs half-widths now that their lengths are known.
        self.crust_half_widths()
        self.mantle_half_widths()
        self.sediment_intervals()

        self.mcmc_params.validate()

    def crust_half_widths(self) -> np.ndarray:
        return expand_half_widths(
            self.search_radius.crust_vs, self.n_coeff_crust, "search_radius.crust_vs"
        )

    def mantle_half_widths(self) -> np.ndarray:
        return expand_half_widths(
            self.search_radius.mantle_vs,
            self.n_coeff_mantle,
            "search_radius.mantle_vs",
        )

    def sediment_intervals(self) -> tuple[tuple[float, float], ...]:
        """Return validated sediment intervals whose search centres increase."""

        intervals: list[tuple[float, float]] = []
        for bounds in self.sediment_vs:
            if len(bounds) != 2:
                raise ValueError("each sediment_vs entry must contain [lower, upper]")
            values = np.asarray(bounds, dtype=float)
            if not np.isfinite(values).all() or values[1] <= values[0]:
                raise ValueError("sediment_vs bounds must be finite with upper > lower")
            intervals.append((float(values[0]), float(values[1])))

        centres = [0.5 * (low + high) for low, high in intervals]
        for index in range(len(centres) - 1):
            if centres[index] >= centres[index + 1]:
                raise ValueError(
                    "sediment_vs search centres must be strictly increasing: "
                    f"centre {index + 1}={centres[index]:.3f} >= "
                    f"centre {index + 2}={centres[index + 1]:.3f}"
                )
        return tuple(intervals)


# =========================
# JSON LOADING
# =========================

_SECTIONS = {
    "paths": Paths,
    "search_radius": SearchRadius,
    "mcmc_params": MCMCParams,
    "input_units": InputUnits,
    "vs_constraints": VsConstraints,
    "phase_constraints": PhaseConstraints,
}
# Config fields that carry a default and may therefore be omitted.
_OPTIONAL_FIELDS = frozenset(
    item.name
    for item in dataclasses.fields(Config)
    if item.default is not dataclasses.MISSING
    or item.default_factory is not dataclasses.MISSING
)


def _build_section(cls, section: str, values: Any):
    if values is None:
        values = {}
    if not isinstance(values, Mapping):
        raise ValueError(f"{section} must be a JSON object")
    known = {item.name for item in dataclasses.fields(cls)}
    unknown = sorted(set(values) - known)
    if unknown:
        raise ValueError(
            f"{section} has unknown keys: {', '.join(unknown)}; "
            f"expected: {', '.join(sorted(known))}"
        )
    required = {
        item.name
        for item in dataclasses.fields(cls)
        if item.default is dataclasses.MISSING
        and item.default_factory is dataclasses.MISSING
    }
    missing = sorted(required - set(values))
    if missing:
        raise ValueError(f"{section} is missing required keys: {', '.join(missing)}")
    return cls(**values)


def _resolve_paths(values: Mapping[str, Any], base_dir: Path) -> Paths:
    if not isinstance(values, Mapping):
        raise ValueError("paths must be a JSON object")
    known = {item.name for item in dataclasses.fields(Paths)}
    unknown = sorted(set(values) - known)
    if unknown:
        raise ValueError(
            f"paths has unknown keys: {', '.join(unknown)}; "
            f"expected: {', '.join(sorted(known))}"
        )
    resolved: dict[str, Path] = {}
    for name in sorted(known & set(values)):
        path = Path(str(values[name])).expanduser()
        resolved[name] = path if path.is_absolute() else base_dir / path
    return _build_section(Paths, "paths", resolved)


def load_config(path: str | Path) -> Config:
    """Read and validate a JSON configuration file.

    Relative ``paths`` entries are resolved against the directory containing
    the configuration file. Unknown keys are rejected with the list of expected
    names so a misspelling fails immediately.
    """

    config_path = Path(path).expanduser().resolve()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(raw, Mapping):
        raise ValueError(f"Configuration must be a JSON object: {config_path}")

    known = {item.name for item in dataclasses.fields(Config)}
    unknown = sorted(set(raw) - known)
    if unknown:
        raise ValueError(
            f"Unknown configuration keys: {', '.join(unknown)}; "
            f"expected: {', '.join(sorted(known))}"
        )
    missing = sorted(known - _OPTIONAL_FIELDS - set(raw))
    if missing:
        raise ValueError(
            f"Configuration is missing required keys: {', '.join(missing)}"
        )

    values: dict[str, Any] = dict(raw)
    values["paths"] = _resolve_paths(raw["paths"], config_path.parent)
    for section, cls in _SECTIONS.items():
        if section == "paths":
            continue
        values[section] = _build_section(cls, section, raw.get(section))
    return Config(**values)
