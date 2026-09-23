"""Orchestrate serial MCMC grid preparation.

Grid initialization is intentionally serial. Typical studies contain only a few
hundred to roughly one thousand inversion points, while each point performs
lightweight array lookup, prior construction, and small-file I/O. Avoiding
multiprocessing removes process-startup and pickling overhead and makes failures
directly traceable to the current grid coordinate.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from seispy.mcmc.config import Config, load_config
from seispy.mcmc.dispersion import build_dispersion_grid
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

PreparedPoint = tuple[InversionPoint, PointPriorBounds]


def _collect_examples(items: Sequence[str], limit: int = 5) -> str:
    examples = ", ".join(items[:limit])
    suffix = "" if len(items) <= limit else f" (+{len(items) - limit} more)"
    return examples + suffix


def prepare_points(
    fields: SpatialFields,
    vs_library: VsModelLibrary,
    cfg: Config,
    settings: PriorSettings,
) -> list[PreparedPoint]:
    """Build and validate every inversion point before any output is written.

    Doing the work once here means a bad grid fails before a single directory is
    created, and the write loop only serializes values that already exist.
    """

    prepared: list[PreparedPoint] = []
    missing: list[str] = []
    invalid: list[str] = []
    folder_names: dict[str, tuple[float, float]] = {}

    for lon, lat, topo, sediment, moho in fields.flat_points():
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
            prepared.append((point, compute_point_bounds(point, settings)))
        except ValueError as exc:
            invalid.append(f"({lon:.6f}, {lat:.6f}): {exc}")

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
    return prepared


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
    prepared = prepare_points(fields, vs_library, cfg, settings)

    base_dir = Path(cfg.paths.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    writer = FortranInputWriter(base_dir, cfg)

    written = 0
    iterator = enumerate(prepared)
    for index, (point, bounds) in progress_iter(
        iterator, total=len(prepared), desc="Preparing MCMC", unit="grid"
    ):
        phase = dispersion.curve_at(index)
        written += int(writer.write_point(point, phase, bounds, plot=plot, dpi=dpi))

    skipped = len(prepared) - written
    summary = f"MCMC grids initialized. Written grids: {written}/{fields.size}"
    if skipped:
        summary += f" (skipped {skipped})"
    print(summary)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config_path", type=Path, help="MCMC JSON configuration")
    parser.add_argument("--plot", action="store_true", help="write a per-point figure")
    parser.add_argument("--dpi", type=int, default=300, help="figure resolution")
    args = parser.parse_args(argv)
    init_grids(args.config_path, plot=args.plot, dpi=args.dpi)


if __name__ == "__main__":
    main()
