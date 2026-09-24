"""Serialize one inversion point into its Fortran inputs and plot metadata.

The writer owns file layout and formatting only. Prior bounds are computed by
:mod:`seispy.mcmc.priors` and passed in, so the numbers written to
`para.inp`, `prior_bounds.csv` and `point.json` come from exactly one
computation. The same schema encodes and decodes the audit table, so writers and
readers cannot drift apart.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from seispy.mcmc.config import Config
from seispy.mcmc.dispersion import DispersionRow
from seispy.mcmc.inversion import CRUST, MANTLE, InversionPoint
from seispy.mcmc.priors import LayerDiagnostics, PointPriorBounds, PriorBound


def _format(value: float | None) -> str:
    """Format an optional diagnostic value for the audit CSV."""

    return "" if value is None else f"{value:.6f}"


def _optional_float(value: str) -> float | None:
    return float(value) if value not in ("",) else None


@dataclass(frozen=True)
class _RowContext:
    """Context shared by every row of one layer's audit records."""

    point: InversionPoint
    diagnostics: LayerDiagnostics
    on_boundary: bool
    moho_contrast: float | None


@dataclass(frozen=True)
class _Column:
    """One audit column: its CSV name, bound field and encode/decode pair."""

    name: str
    field: str | None
    encode: Callable[[PriorBound, _RowContext], str]
    decode: Callable[[str], object]


PRIOR_BOUND_COLUMNS: tuple[_Column, ...] = (
    _Column("section", "section", lambda b, c: b.section, str),
    _Column("coefficient", "coefficient", lambda b, c: str(b.coefficient), int),
    _Column(
        "greville_depth_km",
        "greville_depth",
        lambda b, c: f"{b.greville_depth:.6f}",
        float,
    ),
    _Column(
        "basis_centroid_km",
        "basis_centroid",
        lambda b, c: f"{b.basis_centroid:.6f}",
        float,
    ),
    _Column(
        "projection_vs_km_s",
        "projection_vs",
        lambda b, c: f"{b.projection_vs:.6f}",
        float,
    ),
    _Column("center_vs_km_s", "center_vs", lambda b, c: f"{b.center_vs:.6f}", float),
    _Column(
        "reference_vs_at_centroid_km_s",
        "reference_vs_at_centroid",
        lambda b, c: f"{b.reference_vs_at_centroid:.6f}",
        float,
    ),
    _Column(
        "search_radius_km_s",
        "search_radius",
        lambda b, c: f"{b.search_radius:.6f}",
        float,
    ),
    _Column("lower_km_s", "lower", lambda b, c: f"{b.lower:.6f}", float),
    _Column("upper_km_s", "upper", lambda b, c: f"{b.upper:.6f}", float),
    _Column(
        "initial_midpoint_vs_km_s",
        None,
        lambda b, c: f"{b.effective_center_vs:.6f}",
        float,
    ),
    _Column(
        "basis_mass_fraction",
        "basis_mass_fraction",
        lambda b, c: _format(b.basis_mass_fraction),
        _optional_float,
    ),
    _Column(
        "shallow_extrapolated",
        "shallow_extrapolated",
        lambda b, c: str(int(b.shallow_extrapolated)),
        lambda value: bool(int(value)),
    ),
    _Column(
        "layer_projection_max_error_km_s",
        None,
        lambda b, c: _format(c.diagnostics.projection_max_error),
        _optional_float,
    ),
    _Column(
        "window_scale",
        None,
        lambda b, c: f"{c.diagnostics.window_scale:.4f}",
        _optional_float,
    ),
    _Column(
        "moho_contrast_km_s",
        None,
        lambda b, c: _format(c.moho_contrast) if c.on_boundary else "",
        _optional_float,
    ),
    _Column("profile_lon", None, lambda b, c: f"{c.point.vs_profile.lon:.6f}", float),
    _Column("profile_lat", None, lambda b, c: f"{c.point.vs_profile.lat:.6f}", float),
)

PRIOR_BOUND_FIELDS: tuple[str, ...] = tuple(
    column.name for column in PRIOR_BOUND_COLUMNS
)


def prior_bound_rows(
    bounds: PointPriorBounds,
    point: InversionPoint,
) -> list[dict[str, str]]:
    """Return the audit rows for one point from the shared schema.

    Args:
        bounds: Final prior bounds for the point.
        point: The inversion point the rows describe.

    Returns:
        One CSV-ready mapping per crust and mantle coefficient.
    """

    boundary = set()
    if bounds.crust:
        boundary.add((CRUST, bounds.crust[-1].coefficient))
    if bounds.mantle:
        boundary.add((MANTLE, bounds.mantle[0].coefficient))

    rows: list[dict[str, str]] = []
    layers = (
        (bounds.crust, bounds.crust_diagnostics),
        (bounds.mantle, bounds.mantle_diagnostics),
    )
    for layer_bounds, diagnostics in layers:
        for bound in layer_bounds:
            context = _RowContext(
                point=point,
                diagnostics=diagnostics,
                on_boundary=(bound.section, bound.coefficient) in boundary,
                moho_contrast=bounds.moho_contrast,
            )
            rows.append(
                {
                    column.name: column.encode(bound, context)
                    for column in PRIOR_BOUND_COLUMNS
                }
            )
    return rows


class FortranInputWriter:
    """Write a point's Fortran inputs, audit table and plot metadata.

    Args:
        base_dir: Output directory that receives one sub-directory per point.
        cfg: Validated configuration supplying the DRAM parameters and paths.
    """

    def __init__(self, base_dir: Path, cfg: Config):
        self.base_dir = Path(base_dir)
        self.cfg = cfg

    def write_point(
        self,
        point: InversionPoint,
        rows: list[DispersionRow],
        bounds: PointPriorBounds,
    ) -> None:
        """Write the Fortran inputs and plot metadata for one point.

        The caller has already filtered the dispersion rows, so this method only
        serializes. It never imports matplotlib: the figure is written
        separately by plot_point_dir, which reads these files back.

        Args:
            point: Validated inversion point.
            rows: Usable dispersion rows for the point.
            bounds: Final prior bounds for the point.

        Raises:
            ValueError: If the point fails its own validation.
        """

        point.validate()
        out = self.base_dir / point.folder_name
        out.mkdir(parents=True, exist_ok=True)

        self._write_phase(out, rows)
        self._write_para(out, point, bounds)
        self._write_prior_bounds(out, point, bounds)
        self._write_dram(out)
        self._write_point_meta(out, point, bounds)

    def _write_phase(self, out: Path, rows: list[DispersionRow]) -> None:
        lines = [f"1 {len(rows)}"]
        lines.extend(
            f"2 1 1 {row.period:>3g} {row.velocity:.4f} {row.sigma:.4f}" for row in rows
        )
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
        with (out / "prior_bounds.csv").open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=list(PRIOR_BOUND_FIELDS))
            writer.writeheader()
            writer.writerows(prior_bound_rows(bounds, point))

    def _write_dram(self, out: Path) -> None:
        items = self.cfg.mcmc_params.ordered_items()
        header = " | ".join(name for name, _ in items)
        values = " ".join(str(value) for _, value in items)
        (out / "input_DRAM_T.dat").write_text(header + "\n" + values, encoding="utf-8")

    def _write_point_meta(
        self,
        out: Path,
        point: InversionPoint,
        bounds: PointPriorBounds,
    ) -> None:
        """Write the metadata needed to redraw point.png from disk."""

        profile = point.vs_profile
        payload = {
            "lon": float(point.lon),
            "lat": float(point.lat),
            "water_depth": float(point.water_depth),
            "sediment_thickness": float(point.sediment_thickness),
            "moho_depth": float(point.moho_depth),
            "max_depth": float(point.max_depth),
            "water_threshold": float(point.water_threshold),
            "sediment_threshold": float(point.sediment_threshold),
            "smooth_on": int(point.smooth_on),
            "ice_on": int(point.ice_on),
            "factor": float(bounds.factor),
            "sediment_bounds": [[float(lo), float(hi)] for lo, hi in bounds.sediment],
            "reference_depth_km": [float(value) for value in profile.depth],
            "reference_vs_km_s": [float(value) for value in profile.vs],
        }
        (out / "point.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
