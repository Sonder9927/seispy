"""Collect only mean Vs profiles, misfit, Moho and mean-profile LAB."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4
import shutil

import numpy as np
import pandas as pd

from seispy.mcmc.lab import LABSettings, estimate_lab

REQUIRED_OUTPUTS = ("mean_prob.lst", "Litmod_output.log", "moho.lst")


@dataclass(frozen=True)
class GridResult:
    lon: float
    lat: float
    profile: pd.DataFrame
    misfit: float
    moho: float


@dataclass(frozen=True)
class CollectionSummary:
    """Counts and errors; failed grids have no exported interface row."""

    grid_count: int
    success_count: int
    failed_count: int
    lab_nan_count: int
    errors: dict[str, str]
    output_paths: dict[str, Path]
    probability_figures: int = 0

    @property
    def lab_valid_count(self) -> int:
        return self.success_count - self.lab_nan_count


def _coordinates_from_name(grid_path: Path) -> tuple[float | None, float | None]:
    try:
        lon, lat = map(float, grid_path.name.split("_"))
    except (TypeError, ValueError):
        return None, None
    return lon, lat


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
    profile["z"] = pd.to_numeric(profile["z"], errors="raise")
    profile["vs"] = pd.to_numeric(profile["vs"], errors="raise")
    if not np.isfinite(profile[["z", "vs"]]).all().all():
        raise ValueError("Non-finite profile")
    if (profile["z"] < 0).any() or not np.all(np.diff(profile["z"]) > 0):
        raise ValueError("Profile depths must be increasing positive-downward in input")
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

    if not np.isfinite([lon, lat, misfit, moho]).all():
        raise ValueError("Non-finite grid summary")

    return GridResult(
        lon=lon,
        lat=lat,
        profile=profile,
        misfit=misfit,
        moho=moho,
    )


def collect_results(
    grids_dir: str | Path,
    out_dir: str | Path,
    *,
    lab_settings: LABSettings | None = None,
    collect_prob_figures: bool = False,
) -> CollectionSummary:
    """Silently write two Parquet tables from the three Fortran summaries.

    profiles.parquet contains x,y,z,vs; interfaces.parquet contains
    x,y,misfit,moho,lab. Vs, misfit and Moho keep their historical definitions.
    LAB is calculated once per grid from mean_prob.lst and the mean Moho.
    All depths are positive downward in kilometres. A missing or ambiguous
    descent gives NaN; it does not make collection fail.

    No retained chain, model reconstruction, posterior statistics or input
    snapshots are read or written. Set collect_prob_figures=True to copy existing
    probalCr.png files from successfully collected grids into a separate run
    directory returned as output_paths['prob_figures']. No images are generated.
    Copy failures are recorded by image path in errors without discarding values.
    Invalid grids are isolated in
    summary.errors. summary.lab_nan_count counts exported NaN interface rows;
    completely failed grids are counted separately in summary.failed_count.
    """
    settings = lab_settings or LABSettings()
    grids_path, out_path = Path(grids_dir).resolve(), Path(out_dir).resolve()
    if not grids_path.is_dir():
        raise FileNotFoundError(f"MCMC grids directory not found: {grids_path}")
    if grids_path == out_path or (out_path / "mean_prob.lst").exists():
        raise ValueError(
            "Output directory must differ from grids and point directories"
        )
    grid_paths = [
        p
        for p in sorted(grids_path.iterdir())
        if p.is_dir()
        and not p.name.startswith(".")
        and p != out_path
        and (_coordinates_from_name(p)[0] is not None or (p / "point.json").exists())
    ]
    profiles, rows, errors, successful_grids = [], [], {}, []
    for grid in grid_paths:
        try:
            result = read_grid_result(grid)
            lab = estimate_lab(
                result.profile.z, result.profile.vs, result.moho, settings
            )
        except (OSError, ValueError, IndexError, pd.errors.ParserError) as exc:
            errors[grid.name] = f"{type(exc).__name__}: {exc}"
            continue
        profiles.append(result.profile[["x", "y", "z", "vs"]])
        rows.append([result.lon, result.lat, result.misfit, result.moho, lab])
        successful_grids.append(grid)
    velocity = (
        pd.concat(profiles, ignore_index=True)
        if profiles
        else pd.DataFrame({k: pd.Series(dtype=float) for k in ("x", "y", "z", "vs")})
    )
    interfaces = pd.DataFrame(
        rows, columns=["x", "y", "misfit", "moho", "lab"], dtype=float
    )
    out_path.mkdir(parents=True, exist_ok=True)
    paths = {name: out_path / f"{name}.parquet" for name in ("profiles", "interfaces")}
    # Prepare both tables before replacing either published output.
    with TemporaryDirectory(prefix=".collect-", dir=out_path) as staging:
        for name, table in (("profiles", velocity), ("interfaces", interfaces)):
            table.to_parquet(Path(staging) / f"{name}.parquet", index=False)
        for name, path in paths.items():
            (Path(staging) / f"{name}.parquet").replace(path)
    figure_count = 0
    if collect_prob_figures:
        figures = out_path / "prob_figures" / uuid4().hex
        for grid in successful_grids:
            source = grid / "probalCr.png"
            if not source.is_file():
                continue
            try:
                figures.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, figures / f"{grid.name}.png")
                figure_count += 1
            except OSError as exc:
                errors[f"{grid.name}/probalCr.png"] = f"{type(exc).__name__}: {exc}"
        if figures.is_dir():
            paths["prob_figures"] = figures
    return CollectionSummary(
        len(grid_paths),
        len(rows),
        len(grid_paths) - len(rows),
        int(interfaces.lab.isna().sum()),
        errors,
        paths,
        figure_count,
    )
