"""Collect completed per-point inversions into analysis products.

Each grid directory is read independently. A malformed directory is reported in
the diagnostics table and does not stop the other points from being collected.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import shutil
import json
from dataclasses import asdict
from uuid import uuid4

import numpy as np

from seispy.mcmc.lab import LABSettings
from seispy.mcmc.posterior import collect_posterior

import pandas as pd

REQUIRED_OUTPUTS = ("mean_prob.lst", "Litmod_output.log", "moho.lst")
PROBABILITY_FIGURE = "probalCr.png"


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
    output_paths: dict[str, Path]

    @property
    def posterior_count(self) -> int:
        return int((self.diagnostics.status == "ok").sum())

    @property
    def partial_count(self) -> int:
        return int((self.diagnostics.status == "partial").sum())


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
    profile["z"] = -pd.to_numeric(profile["z"], errors="raise")
    profile["vs"] = pd.to_numeric(profile["vs"], errors="raise")
    if not np.isfinite(profile[["z", "vs"]]).all().all():
        raise ValueError("Non-finite profile")
    if (profile["z"] > 0).any() or not np.all(np.diff(profile["z"]) < 0):
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
        lab=float("nan"),
    )


def collect_results(
    grids_dir: str | Path,
    out_dir: str | Path,
    *,
    lab_settings: LABSettings | None = None,
) -> CollectionSummary:
    """Write silent, provenance-labelled Parquet results and diagnostics.

    Existing x/y/z/vs/misfit/moho values retain their original definitions.
    Full-chain statistics use explicit new column names. LAB is the negative
    of the detected-sample median depth (km), or NaN. Positive-downward LAB
    quantiles have explicit ``_km_`` names. Their intervals are conditional on
    unambiguous detection, not unconditional evidence for an interface.

    Read the per-point para.inp and input_DRAM_T.dat emitted by init_grids;
    no original source datasets or current global config are needed. Failed
    posterior reconstruction preserves the historical summaries with a partial
    diagnostic, but never substitutes an average-profile LAB.
    """
    import pyarrow  # fail before processing if the Parquet dependency is absent

    del pyarrow
    settings = lab_settings or LABSettings()
    grids_path, out_path = Path(grids_dir).resolve(), Path(out_dir).resolve()
    if not grids_path.is_dir():
        raise FileNotFoundError(f"MCMC grids directory not found: {grids_path}")
    if grids_path == out_path or (out_path / "para.inp").exists():
        raise ValueError("Output directory must differ from grids directory")
    grid_paths = [
        p
        for p in sorted(grids_path.iterdir())
        if p.is_dir()
        and not p.name.startswith(".")
        and p != out_path
        and (_coordinates_from_name(p)[0] is not None or (p / "point.json").exists())
    ]
    out_path.mkdir(parents=True, exist_ok=True)
    run_id = uuid4().hex
    run_dir = out_path / "posterior" / run_id
    profiles, summaries, diagnostics = [], [], []
    figure_count = 0
    for grid_path in grid_paths:
        lon, lat = _coordinates_from_name(grid_path)
        diagnostic = dict(
            grid=grid_path.name,
            longitude=lon,
            latitude=lat,
            status="failed",
            message="",
            posterior_path="",
        )
        try:
            result = read_grid_result(grid_path)
            # point.json is upstream provenance; filenames retain historical x/y.
            if (grid_path / "point.json").is_file():
                meta = json.loads((grid_path / "point.json").read_text())
                if f"{meta['lon']:.2f}_{meta['lat']:.2f}" != grid_path.name:
                    raise ValueError(
                        "point.json coordinates disagree with grid directory"
                    )
        except Exception as exc:
            diagnostic["message"] = f"{type(exc).__name__}: {exc}"
            diagnostics.append(diagnostic)
            continue
        row = dict(
            zip(("x", "y", "misfit", "moho", "lab"), result.summary_row(), strict=True)
        )
        row["posterior_status"] = "unavailable"
        diagnostic["status"] = "partial"
        try:
            extra, summary, samples, covariance, retained = collect_posterior(
                grid_path, -result.profile.z.to_numpy(), settings
            )
            point_out = run_dir / grid_path.name
            point_out.mkdir(parents=True)
            samples.to_parquet(point_out / "samples.parquet", index=False)
            covariance.to_parquet(point_out / "covariance.parquet", index=False)
            retained.to_parquet(point_out / "chain.parquet", index=False)
            for name in ("para.inp", "input_DRAM_T.dat", "point.json"):
                if (grid_path / name).is_file():
                    shutil.copy2(grid_path / name, point_out / name)
            # Preserve vs: it is the historical Vsv field, even for other modes.
            result.profile[extra.columns] = extra
            row.update(summary)
            row["lab"] = -summary["lab_depth_km_median"]
            row["posterior_status"] = "available"
            row["lab_status"] = (
                "conditional_estimate" if np.isfinite(row["lab"]) else "unresolved"
            )
            diagnostic.update(status="ok", posterior_path=str(point_out))
        except Exception as exc:
            diagnostic["message"] = f"{type(exc).__name__}: {exc}"
            row["lab_status"] = "posterior_unavailable"
        figure = grid_path / PROBABILITY_FIGURE
        if figure.is_file():
            figure_dir = out_path / "mcmc_prob_figs" / run_id
            try:
                figure_dir.mkdir(parents=True, exist_ok=True)
                shutil.copy2(figure, figure_dir / f"prob_fig_{grid_path.name}.png")
                figure_count += 1
            except OSError as exc:
                diagnostic["message"] += f"; figure copy: {exc}"
        profiles.append(result.profile)
        summaries.append(row)
        diagnostics.append(diagnostic)
    velocity = (
        pd.concat(profiles, ignore_index=True)
        if profiles
        else pd.DataFrame(
            {key: pd.Series(dtype=float) for key in ("x", "y", "z", "vs")}
        )
    )
    interfaces = (
        pd.DataFrame(summaries)
        if summaries
        else pd.DataFrame(
            {key: pd.Series(dtype=float) for key in ("x", "y", "misfit", "moho", "lab")}
        )
    )
    diagnostic_table = pd.DataFrame(
        diagnostics,
        columns=[
            "grid",
            "longitude",
            "latitude",
            "status",
            "message",
            "posterior_path",
        ],
    )
    paths = {}
    for name, table in (
        ("profiles", velocity),
        ("interfaces", interfaces),
        ("diagnostics", diagnostic_table),
    ):
        paths[name] = out_path / f"{name}.parquet"
        temporary = out_path / f".{name}.{run_id}.parquet"
        table.to_parquet(temporary, index=False)
        temporary.replace(paths[name])
    paths["metadata"] = out_path / "metadata.json"
    metadata = dict(
        schema_version=1,
        run_id=run_id,
        grids_dir=str(grids_path),
        lab_settings=asdict(settings),
        lab_method="smoothed_forward_mantle_nodes_negative_gradient",
        units={
            "velocity": "km/s",
            "depth": "km",
            "a": "dimensionless fraction",
            "xi": "(vsh/vsv)^2",
            "objective": "Fortran misfit including enabled smoothing, excluding separate prior",
        },
        preserved_fields=["x", "y", "z", "vs", "misfit", "moho"],
        original_source="mean_prob.lst, moho.lst, Litmod_output.log",
        posterior_source="fort.8010 retained iteration states including repeats",
        depth_convention="z/lab negative downward; moho and *_km* positive downward",
        intervals="pointwise equal-tailed; LAB conditional on unambiguous detection",
        diagnostics="No automatic convergence certification; retained chains saved for diagnostics",
        posterior_directory=str(run_dir),
    )
    paths["metadata"].write_text(json.dumps(metadata, indent=2) + "\n")
    return CollectionSummary(
        len(grid_paths),
        len(profiles),
        len(grid_paths) - len(profiles),
        len(velocity),
        figure_count,
        diagnostic_table,
        paths,
    )
