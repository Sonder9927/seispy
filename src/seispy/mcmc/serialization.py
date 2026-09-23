"""Serialize one inversion point into the four Fortran input files.

The writer owns file layout and formatting only. Prior bounds are computed by
:mod:`seispy.mcmc.priors` and passed in, so the numbers written to ``para.inp``
and to the diagnostic figure come from exactly one computation.
"""

from __future__ import annotations

import csv
from pathlib import Path

from seispy.mcmc.config import Config
from seispy.mcmc.dispersion import DispersionCurve
from seispy.mcmc.inversion import CRUST, MANTLE, InversionPoint
from seispy.mcmc.priors import PointPriorBounds


def _format(value: float | None) -> str:
    """Format an optional diagnostic value for the audit CSV."""

    return "" if value is None else f"{value:.6f}"


class FortranInputWriter:
    """Write ``phase.input``, ``para.inp``, ``prior_bounds.csv`` and DRAM params."""

    def __init__(self, base_dir: Path, cfg: Config):
        self.base_dir = Path(base_dir)
        self.cfg = cfg

    def write_point(
        self,
        point: InversionPoint,
        phase: DispersionCurve,
        bounds: PointPriorBounds,
        *,
        plot: bool = False,
        dpi: int = 300,
    ) -> bool:
        """Write the Fortran inputs for one point.

        A point with too few valid dispersion rows is skipped before any
        directory is created, so skipped points leave no output folder. When
        ``plot`` is true a combined dispersion and Vs-model figure is written
        to the point directory as ``point.png``.
        """

        point.validate()
        rows = self._accepted_rows(point, phase)
        if rows is None:
            return False

        out = self.base_dir / point.folder_name
        out.mkdir(parents=True, exist_ok=True)

        self._write_phase(out, rows)
        self._write_para(out, point, bounds)
        self._write_prior_bounds(out, point, bounds)
        self._write_dram(out)
        if plot:
            self._write_point_plot(out / "point.png", point, phase, bounds, dpi=dpi)
        return True

    def _accepted_rows(
        self,
        point: InversionPoint,
        phase: DispersionCurve,
    ) -> list[tuple[float, float, float]] | None:
        """Return usable dispersion rows, or None when the point is skipped."""

        rows = phase.valid_rows(default_sigma=float(self.cfg.default_phase_std))
        minimum = int(self.cfg.phase_constraints.minimum_periods)
        if self.cfg.phase_constraints.skip_if_insufficient and len(rows) < minimum:
            print(
                f"[SKIP] {point.folder_name}: only {len(rows)} valid dispersion "
                f"points (< {minimum})"
            )
            return None
        return rows

    def _write_phase(self, out: Path, rows: list[tuple[float, float, float]]) -> None:
        lines = [f"1 {len(rows)}"]
        for period, velocity, sigma in rows:
            lines.append(f"2 1 1 {period:>3g} {velocity:.4f} {sigma:.4f}")
        lines += ["0", "0"]
        (out / "phase.input").write_text("\n".join(lines), encoding="utf-8")

    def _write_para(
        self,
        out: Path,
        point: InversionPoint,
        bounds: PointPriorBounds,
    ) -> None:
        cfg = self.cfg
        sr = cfg.search_radius

        lines = [str(point.smooth_on), str(point.ice_on), str(point.water_on)]
        if point.water_on:
            lines.append(f"{point.water_depth:.3f}")

        lines.append(str(point.sediment_on))
        lines += [
            f"{cfg.factor}",
            f"{cfg.zmax_Bs}",
            f"{cfg.NPTS_cBs}",
            f"{cfg.NPTS_mBs}",
        ]
        lines.append(
            cfg.reference_water_model if point.water_on else cfg.reference_model
        )

        if point.sediment_on:
            radius = float(sr.sediment)
            low = max(0.0, point.sediment_thickness - radius)
            high = point.sediment_thickness + radius
            lines.append(f"0 0 {low:.2f} {high:.2f}")

        moho_radius = float(sr.moho)
        lines.append(
            f"0 1 {point.moho_depth - moho_radius:.2f} "
            f"{point.moho_depth + moho_radius:.2f}"
        )

        for index, (low, high) in enumerate(bounds.sediment, start=1):
            lines.append(f"10 {index} {low:.3f} {high:.3f}")
        for bound in bounds.crust:
            lines.append(f"1 {bound.coefficient} {bound.lower:.3f} {bound.upper:.3f}")
        for bound in bounds.mantle:
            lines.append(f"2 {bound.coefficient} {bound.lower:.3f} {bound.upper:.3f}")

        (out / "para.inp").write_text("\n".join(lines), encoding="utf-8")

    def _write_prior_bounds(
        self,
        out: Path,
        point: InversionPoint,
        bounds: PointPriorBounds,
    ) -> None:
        """Write an auditable record of B-spline prior construction.

        Besides the numbers written to ``para.inp`` this records two basis
        diagnostics per coefficient (where the basis function actually sits and
        how much of the layer it controls) and the realized Moho contrast, which
        exposes the structural coincidence of the two interface coefficients.
        """

        fields = [
            "section",
            "coefficient",
            "representative_depth_km",
            "reference_vs_km_s",
            "effective_center_vs_km_s",
            "search_radius_km_s",
            "lower_km_s",
            "upper_km_s",
            "shallow_extrapolated",
            "basis_centroid_km",
            "basis_mass_fraction",
            "moho_contrast_km_s",
            "profile_lon",
            "profile_lat",
        ]

        boundary: set[tuple[str, int]] = set()
        if bounds.crust:
            boundary.add((CRUST, bounds.crust[-1].coefficient))
        if bounds.mantle:
            boundary.add((MANTLE, bounds.mantle[0].coefficient))

        with (out / "prior_bounds.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            for bound in [*bounds.crust, *bounds.mantle]:
                on_boundary = (bound.section, bound.coefficient) in boundary
                writer.writerow(
                    {
                        "section": bound.section,
                        "coefficient": bound.coefficient,
                        "representative_depth_km": f"{bound.representative_depth:.6f}",
                        "reference_vs_km_s": f"{bound.reference_vs:.6f}",
                        "effective_center_vs_km_s": f"{bound.effective_center_vs:.6f}",
                        "search_radius_km_s": f"{bound.search_radius:.6f}",
                        "lower_km_s": f"{bound.lower:.6f}",
                        "upper_km_s": f"{bound.upper:.6f}",
                        "shallow_extrapolated": int(bound.shallow_extrapolated),
                        "basis_centroid_km": _format(bound.basis_centroid),
                        "basis_mass_fraction": _format(bound.basis_mass_fraction),
                        "moho_contrast_km_s": (
                            _format(bounds.moho_contrast) if on_boundary else ""
                        ),
                        "profile_lon": f"{point.vs_profile.lon:.6f}",
                        "profile_lat": f"{point.vs_profile.lat:.6f}",
                    }
                )

    def _write_dram(self, out: Path) -> None:
        items = self.cfg.mcmc_params.ordered_items()
        header = " | ".join(name for name, _ in items)
        values = " ".join(str(value) for _, value in items)
        (out / "input_DRAM_T.dat").write_text(header + "\n" + values, encoding="utf-8")

    def _write_point_plot(
        self,
        path: Path,
        point: InversionPoint,
        phase: DispersionCurve,
        bounds: PointPriorBounds,
        *,
        dpi: int,
    ) -> None:
        import matplotlib.pyplot as plt

        from seispy.mcmc.plotting import plot_point

        figure, _ = plot_point(
            point,
            phase,
            bounds,
            default_sigma=float(self.cfg.default_phase_std),
            output_file=path,
            dpi=dpi,
        )
        plt.close(figure)
