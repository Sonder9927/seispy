"""Orchestrate MCMC grid preparation."""

from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from tqdm import tqdm

from seispy.mcmc.configuration import Config, load_config
from seispy.mcmc.models import MCMCGrid, VsModelLibrary, make_mcmc_grid
from seispy.mcmc.spatial import (
    PhaseCube,
    PhaseCurve,
    GridData,
    build_phase_cube,
    build_spatial_grid,
)
from seispy.mcmc.writer import GridWriter


def build_tasks(
    cfg: Config, grid_data: GridData, phase_cube: PhaseCube, vs_library: VsModelLibrary
):
    tasks = []
    for k, (lon, lat, topo, sediment, moho) in enumerate(grid_data.flat_values()):
        phase = phase_cube.curve_at_flat_index(k)
        vs_profile = vs_library.nearest_profile(float(lon), float(lat))
        tasks.append((lon, lat, topo, sediment, moho, phase, vs_profile, cfg))
    return tasks


def process_point(args) -> tuple[MCMCGrid, PhaseCurve]:
    lon, lat, topo, sediment, moho, phase, vs_profile, cfg = args
    grid = make_mcmc_grid(lon, lat, topo, sediment, moho, vs_profile, cfg)
    return grid, phase


def init_grids(config_path: str | Path, max_workers: int = 1) -> None:
    """Generate per-point MCMC inputs from a JSON configuration.

    Args:
        config_path: JSON file matching :class:`Config`.
        max_workers: Maximum number of model-building worker processes.

    Raises:
        ValueError: If configuration or source data fail validation.

    Examples:
        ```python
        init_grids("config/mcmc.json", max_workers=4)
        ```
    """
    cfg = load_config(config_path)
    base_dir = Path(cfg.paths.output_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    grid_data = build_spatial_grid(cfg)
    phase_cube = build_phase_cube(cfg, grid_data)
    vs_library = VsModelLibrary.from_csv(cfg.paths.vs_model_csv)
    tasks = build_tasks(cfg, grid_data, phase_cube, vs_library)

    writer = GridWriter(base_dir, cfg)

    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            iterator = executor.map(process_point, tasks)
            written = 0
            for grid, phase in tqdm(iterator, total=len(tasks)):
                written += int(writer.write(grid, phase))
    else:
        written = 0
        for task in tqdm(tasks):
            grid, phase = process_point(task)
            written += int(writer.write(grid, phase))

    print(f"MCMC grids initialized. Written grids: {written}/{len(tasks)}")


if __name__ == "__main__":
    init_grids("data/mcmc/config.json", max_workers=4)
