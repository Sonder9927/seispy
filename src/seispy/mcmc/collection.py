"""Collect completed per-point inversions into analysis products.

Each grid directory is read independently. A malformed directory is reported in
the diagnostics table and does not stop the other points from being collected.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil

import pandas as pd

REQUIRED_OUTPUTS = ("mean_prob.lst", "Litmod_output.log", "moho.lst")
PROBABILITY_FIGURE = "probalCr.png"
_MISSING_LAB = -999.0
_LAB_DEPTH_KM = -50.0


@dataclass(frozen=True)
class GridResult:
    """Parsed outputs of one completed inversion directory."""

    lon: float
    lat: float
    profile: pd.DataFrame
    misfit: float
    moho: float
    lab: float

    def summary_row(self) -> list[float]:
        return [self.lon, self.lat, self.misfit, self.moho, self.lab]


@dataclass(frozen=True)
class CollectionSummary:
    """Result counts and the per-grid diagnostic table."""

    grid_count: int
    success_count: int
    failed_count: int
    velocity_rows: int
    probability_figures: int
    diagnostics: pd.DataFrame


def _copy_probability_figures(grids_path: Path, out_path: Path) -> int:
    out_path.mkdir(parents=True, exist_ok=True)
    count = 0
    for figure in grids_path.rglob(PROBABILITY_FIGURE):
        shutil.copy2(figure, out_path / f"prob_fig_{figure.parent.name}.png")
        count += 1
    return count


def _coordinates_from_name(grid_path: Path) -> tuple[float | None, float | None]:
    try:
        lon, lat = map(float, grid_path.name.split("_"))
    except (TypeError, ValueError):
        return None, None
    return lon, lat


def _landing_depth(profile: pd.DataFrame) -> float:
    """Return the depth of the strongest Vs drop below the reference depth.

    ``profile['z']`` is negative downward. Returns ``-999`` when the profile
    does not reach the reference depth or has too few samples to difference.
    """

    deep = profile["z"] <= _LAB_DEPTH_KM
    if not deep.any():
        return _MISSING_LAB
    drops = profile.loc[deep, "vs"].diff()
    if not drops.notna().any():
        return _MISSING_LAB
    return float(profile.loc[drops.idxmin(), "z"])


def read_grid_result(grid_path: Path) -> GridResult:
    """Read one completed grid directory.

    Raises
    ------
    FileNotFoundError
        If a required output is missing.
    ValueError
        If the directory name or an output file cannot be parsed.
    """

    missing = [name for name in REQUIRED_OUTPUTS if not (grid_path / name).is_file()]
    if missing:
        raise FileNotFoundError(
            f"Incomplete MCMC output in {grid_path}: missing {', '.join(missing)}"
        )

    lon, lat = _coordinates_from_name(grid_path)
    if lon is None or lat is None:
        raise ValueError(
            f"Invalid MCMC output directory name {grid_path.name!r}; "
            "expected '<lon>_<lat>'"
        )

    profile = pd.read_csv(
        grid_path / "mean_prob.lst",
        sep=r"\s+",
        usecols=[0, 1],
        header=None,
        names=["z", "vs"],
    )
    if profile.empty:
        raise ValueError(f"Empty mean_prob.lst in {grid_path}")
    profile["z"] = -pd.to_numeric(profile["z"], errors="raise")
    profile["vs"] = pd.to_numeric(profile["vs"], errors="raise")
    profile["x"], profile["y"] = lon, lat

    log_lines = (
        (grid_path / "Litmod_output.log").read_text(encoding="utf-8").splitlines()
    )
    if not log_lines or len(log_lines[-1].split()) <= 7:
        raise ValueError(f"Cannot parse misfit from {grid_path / 'Litmod_output.log'}")
    misfit = float(log_lines[-1].split()[7])

    moho_values = pd.read_csv(grid_path / "moho.lst", sep=r"\s+", header=None).iloc[
        :, 0
    ]
    moho = float(pd.to_numeric(moho_values, errors="raise").mean())

    return GridResult(
        lon=lon,
        lat=lat,
        profile=profile,
        misfit=misfit,
        moho=moho,
        lab=_landing_depth(profile),
    )


def collect_results(
    grids_dir: str | Path,
    out_dir: str | Path,
    *,
    diagnostics_path: str | Path | None = None,
) -> CollectionSummary:
    """Collect MCMC outputs while isolating failures to individual points.

    The established result products (``vs.csv``, ``misfit_moho_lab.csv``, and
    copied probability figures) are written to ``out_dir``. Every grid point is
    attempted independently; a malformed point is recorded as ``failed`` and
    does not prevent other points from being collected. The diagnostic table is
    printed to the terminal by default and is written only when
    ``diagnostics_path`` is explicitly supplied.
    """

    grids_path, out_path = Path(grids_dir), Path(out_dir)
    if not grids_path.is_dir():
        raise FileNotFoundError(f"MCMC grids directory not found: {grids_path}")

    grid_paths = [
        path
        for path in sorted(grids_path.iterdir())
        if path.is_dir() and not path.name.startswith(".") and path != out_path
    ]
    figure_count = _copy_probability_figures(grids_path, out_path / "mcmc_prob_figs")

    profiles: list[pd.DataFrame] = []
    summary_rows: list[list[float]] = []
    diagnostics: list[dict[str, object]] = []
    for grid_path in grid_paths:
        lon, lat = _coordinates_from_name(grid_path)
        try:
            result = read_grid_result(grid_path)
        except Exception as exc:  # one broken point must not stop collection
            diagnostics.append(
                {
                    "grid": grid_path.name,
                    "longitude": lon,
                    "latitude": lat,
                    "status": "failed",
                    "message": f"{type(exc).__name__}: {exc}",
                }
            )
            continue

        profiles.append(result.profile)
        summary_rows.append(result.summary_row())
        diagnostics.append(
            {
                "grid": grid_path.name,
                "longitude": lon,
                "latitude": lat,
                "status": "ok",
                "message": "",
            }
        )

    out_path.mkdir(parents=True, exist_ok=True)
    velocity = (
        pd.concat(profiles, ignore_index=True)
        if profiles
        else pd.DataFrame(columns=["z", "vs", "x", "y"])
    )
    velocity.to_csv(out_path / "vs.csv", index=False)
    pd.DataFrame(
        summary_rows,
        columns=["x", "y", "misfit", "moho", "lab"],
    ).to_csv(out_path / "misfit_moho_lab.csv", index=False)

    diagnostic_table = pd.DataFrame(
        diagnostics,
        columns=["grid", "longitude", "latitude", "status", "message"],
    )
    print("MCMC collection diagnostics:")
    if diagnostic_table.empty:
        print("  no grid directories found")
    else:
        print(diagnostic_table.to_string(index=False))

    if diagnostics_path is not None:
        diagnostics_file = Path(diagnostics_path)
        diagnostics_file.parent.mkdir(parents=True, exist_ok=True)
        diagnostic_table.to_csv(diagnostics_file, index=False)

    return CollectionSummary(
        grid_count=len(grid_paths),
        success_count=len(profiles),
        failed_count=len(grid_paths) - len(profiles),
        velocity_rows=len(velocity),
        probability_figures=figure_count,
        diagnostics=diagnostic_table,
    )
