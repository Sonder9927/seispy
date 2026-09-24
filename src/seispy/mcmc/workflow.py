"""Orchestrate serial MCMC grid preparation.

Preparation is intentionally serial: typical studies contain only a few hundred
to roughly one thousand inversion points, while each point performs lightweight
array lookup, prior construction, and small-file I/O. Avoiding multiprocessing
removes process-startup and pickling overhead and makes failures directly
traceable to the current grid coordinate.

Preparation and plotting are separate phases. ``prepare_points`` computes and
filters every point, ``FortranInputWriter`` serializes it, and ``plot_grids``
redraws ``point.png`` from the written files. A plotting failure therefore
cannot lose inputs that were already written, and figures can be regenerated
later without the source grids.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import NamedTuple, Sequence

from seispy.mcmc.config import Config, load_config
from seispy.mcmc.dispersion import (
    DispersionGrid,
    DispersionRow,
    build_dispersion_grid,
    valid_dispersion_rows,
)
from seispy.mcmc.gridding import TargetGrid
from seispy.mcmc.inversion import InversionPoint, build_inversion_point
from seispy.mcmc.priors import (
    PointPriorBounds,
    PriorSettings,
    compute_point_bounds,
)
from seispy.mcmc.serialization import FortranInputWriter
from seispy.mcmc.spatial import SpatialFields, build_target_fields
from seispy.mcmc.velocity import VsModelLibrary
from seispy.progress import progress_iter


class PreparedPoint(NamedTuple):
    """One accepted inversion point ready to serialize.

    Attributes:
        point: Validated inversion point.
        bounds: Final prior bounds.
        rows: Filtered dispersion rows.
    """

    point: InversionPoint
    bounds: PointPriorBounds
    rows: list[DispersionRow]


def _collect_examples(items: Sequence[str], limit: int = 5) -> str:
    examples = ", ".join(items[:limit])
    suffix = "" if len(items) <= limit else f" (+{len(items) - limit} more)"
    return examples + suffix


def prepare_points(
    fields: SpatialFields,
    vs_library: VsModelLibrary,
    dispersion: DispersionGrid,
    cfg: Config,
    settings: PriorSettings,
) -> tuple[list[PreparedPoint], list[str]]:
    """Build, validate and dispersion-filter every inversion point.

    All geometry is resolved here, before any output directory is created, so a
    bad grid fails without leaving partial output. Points whose dispersion has
    too few valid periods are skipped and returned separately instead of being
    written. Accepted tuples already carry the rows to serialize.
    """

    prepared: list[PreparedPoint] = []
    skipped: list[str] = []
    missing: list[str] = []
    invalid: list[str] = []
    folder_names: dict[str, tuple[float, float]] = {}
    minimum = int(cfg.phase_constraints.minimum_periods)

    for index, (lon, lat, topo, sediment, moho) in enumerate(fields.flat_points()):
        lon, lat = float(lon), float(lat)
        folder_name = f"{lon:.2f}_{lat:.2f}"
        previous = folder_names.setdefault(folder_name, (lon, lat))
        if previous != (lon, lat):
            raise ValueError(
                "MCMC grid coordinates collide after two-decimal formatting: "
                f"{previous} and {(lon, lat)} both map to {folder_name!r}"
            )

        try:
            profile = vs_library.profile_at(lon, lat)
        except KeyError:
            missing.append(f"({lon:.6f}, {lat:.6f})")
            continue

        if profile.max_depth < cfg.zmax_Bs:
            invalid.append(
                f"({lon:.6f}, {lat:.6f}): profile ends at {profile.max_depth:.3f} km, "
                f"below zmax_Bs={cfg.zmax_Bs:.3f} km"
            )
            continue

        try:
            point = build_inversion_point(
                lon=lon,
                lat=lat,
                topo=topo,
                sediment=sediment,
                moho=moho,
                vs_profile=profile,
                cfg=cfg,
            )
            bounds = compute_point_bounds(point, settings)
        except ValueError as exc:
            invalid.append(f"({lon:.6f}, {lat:.6f}): {exc}")
            continue

        curve = dispersion.curve_at(index)
        rows = valid_dispersion_rows(curve, cfg)
        if rows is None:
            valid = len(curve.valid_rows(default_sigma=float(cfg.default_phase_std)))
            print(
                f"[SKIP] {folder_name}: only {valid} valid dispersion points "
                f"(< {minimum})"
            )
            skipped.append(folder_name)
            continue
        prepared.append(PreparedPoint(point, bounds, rows))

    if missing:
        raise ValueError(
            f"Reference Vs model is missing {len(missing)} inversion-grid profiles: "
            f"{_collect_examples(missing)}"
        )
    if invalid:
        raise ValueError(
            f"MCMC preflight failed at {len(invalid)} grid points: "
            f"{_collect_examples(invalid)}"
        )
    return prepared, skipped


def init_grids(
    config_path: str | Path,
    *,
    plot: bool = False,
    dpi: int = 300,
) -> None:
    """Generate per-point MCMC inputs from a JSON configuration.

    The reference Vs model may be CSV, NetCDF, or Parquet, must be a regular
    lon/lat grid, use positive depth, and cover the deep end through
    ``zmax_Bs``. Its horizontal grid is aligned to the inversion grid with the
    shared three-case strategy before profiles are built. A finite shallow gap
    may be filled by the configured shallow extrapolation policy.

    Every point is built and its prior bounds are validated before any output
    directory is created. A point whose dispersion has too few valid periods is
    then skipped without leaving a directory behind.

    Parameters
    ----------
    config_path
        JSON file matching :class:`seispy.mcmc.config.Config`.
    plot
        When true, also write a combined dispersion and Vs-model figure
        (``point.png``) into each written point directory.
    dpi
        Resolution used for the per-point figure when ``plot`` is true.

    Raises
    ------
    ValueError
        If configuration or source data fail validation, including missing
        reference profiles or insufficient depth coverage.
    """

    cfg = load_config(config_path)
    target = TargetGrid.from_region(cfg.region, cfg.grid_spacing)
    fields = build_target_fields(cfg, target)
    dispersion = build_dispersion_grid(cfg, target)
    vs_library = VsModelLibrary.from_file(
        cfg.paths.vs_model_file,
        target=target,
        vs_scale=cfg.input_units.vs_to_km_s,
    )

    settings = PriorSettings.from_config(cfg)
    prepared, skipped = prepare_points(fields, vs_library, dispersion, cfg, settings)

    base_dir = Path(cfg.paths.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    writer = FortranInputWriter(base_dir, cfg)

    for prepared_point in progress_iter(
        prepared, total=len(prepared), desc="Preparing MCMC", unit="grid"
    ):
        writer.write_point(
            prepared_point.point, prepared_point.rows, prepared_point.bounds
        )

    if plot:
        plot_grids(
            base_dir,
            folder_names=[entry.point.folder_name for entry in prepared],
            dpi=dpi,
        )

    written = len(prepared)
    summary = f"MCMC grids initialized. Written grids: {written}/{fields.size}"
    if skipped:
        summary += f" (skipped {len(skipped)})"
    print(summary)


def plot_grids(
    output_dir: str | Path,
    *,
    folder_names: Sequence[str] | None = None,
    dpi: int = 300,
) -> int:
    """Redraw every point.png from its written directory.

    Reads point.json, prior_bounds.csv and phase.input, so no source grids or
    configuration are needed. Passing folder_names limits the work to those
    directories; otherwise every directory holding a point.json is redrawn. A
    per-point failure is printed and does not stop the other points. Returns the
    number of figures written.
    """

    from seispy.mcmc.plotting import plot_point_dir

    base = Path(output_dir)
    if not base.is_dir():
        raise ValueError(f"Output directory does not exist: {base}")
    if folder_names is None:
        candidates = sorted(
            path.name for path in base.iterdir() if (path / "point.json").is_file()
        )
    else:
        candidates = list(folder_names)

    written = 0
    for name in progress_iter(
        candidates, total=len(candidates), desc="Plotting MCMC", unit="grid"
    ):
        directory = base / name
        if not (directory / "point.json").is_file():
            continue
        try:
            plot_point_dir(directory, dpi=dpi)
        except Exception as exc:
            print(f"[PLOT] {name}: {exc}")
            continue
        written += 1
    return written


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config_path", type=Path, help="MCMC JSON configuration")
    parser.add_argument("--plot", action="store_true", help="write a per-point figure")
    parser.add_argument("--dpi", type=int, default=300, help="figure resolution")
    args = parser.parse_args(argv)
    init_grids(args.config_path, plot=args.plot, dpi=args.dpi)


if __name__ == "__main__":
    main()
