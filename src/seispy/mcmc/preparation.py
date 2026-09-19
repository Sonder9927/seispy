"""Orchestrate serial MCMC grid preparation."""

from pathlib import Path

from seispy.mcmc.configuration import load_config
from seispy.mcmc.models import VsModelLibrary, make_mcmc_grid
from seispy.mcmc.spatial import build_phase_cube, build_spatial_grid
from seispy.mcmc.writer import GridWriter
from seispy.progress import progress_iter


def init_grids(config_path: str | Path) -> None:
    """Generate per-point MCMC inputs from a JSON configuration.

    Grid initialization is intentionally serial. Typical studies contain only
    a few hundred to roughly one thousand inversion points, while each point
    performs lightweight array lookup, prior construction, and small-file I/O.
    Avoiding multiprocessing removes process-startup and pickling overhead and
    makes failures directly traceable to the current grid coordinate.

    The reference Vs model may be CSV or Parquet. It must use the same lon/lat
    grid as the inversion, use positive depth, and cover the full interval from
    0 km through ``zmax_Bs``.

    Parameters
    ----------
    config_path
        JSON file matching :class:`seispy.mcmc.configuration.Config`.

    Raises
    ------
    ValueError
        If configuration or source data fail validation.
    KeyError
        If an inversion-grid coordinate is missing from the Vs reference model.
    """
    cfg = load_config(config_path)
    base_dir = Path(cfg.paths.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    grid_data = build_spatial_grid(cfg)
    phase_cube = build_phase_cube(cfg, grid_data)

    vs_library = VsModelLibrary.from_file(cfg.paths.vs_model_file)

    writer = GridWriter(base_dir, cfg)
    written = 0

    iterator = enumerate(grid_data.flat_values())
    for k, values in progress_iter(
        iterator, total=grid_data.size, desc="Preparing MCMC", unit="grid"
    ):
        lon, lat, topo, sediment, moho = values
        phase = phase_cube.curve_at_flat_index(k)
        vs_profile = vs_library.profile_at(float(lon), float(lat))
        grid = make_mcmc_grid(
            lon=lon,
            lat=lat,
            topo=topo,
            sediment=sediment,
            moho=moho,
            vs_profile=vs_profile,
            cfg=cfg,
        )
        written += int(writer.write(grid, phase))

    print(f"MCMC grids initialized. Written grids: {written}/{grid_data.size}")


if __name__ == "__main__":
    init_grids("data/mcmc/config.json")
