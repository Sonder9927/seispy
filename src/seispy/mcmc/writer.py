"""Write validated per-grid MCMC input files."""

from pathlib import Path
from typing import Any

import numpy as np

from seispy.mcmc.configuration import Config
from seispy.mcmc.models import MCMCGrid, greville_depths, velocity_at_depths
from seispy.mcmc.spatial import PhaseCurve


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
        return list(zip(lower, upper, strict=True))

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
