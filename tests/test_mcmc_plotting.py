import numpy as np
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

from seispy.mcmc.config import load_config  # noqa: E402
from seispy.mcmc.dispersion import DispersionCurve  # noqa: E402
from seispy.mcmc.inversion import build_inversion_point  # noqa: E402
from seispy.mcmc.plotting import (  # noqa: E402
    plot_point,
    plot_point_dir,
)
from seispy.mcmc.priors import PriorSettings, compute_point_bounds  # noqa: E402
from seispy.mcmc.dispersion import valid_dispersion_rows  # noqa: E402
from seispy.mcmc.serialization import FortranInputWriter  # noqa: E402


@pytest.fixture
def point_and_bounds(write_mcmc_config, make_profile):
    cfg = load_config(write_mcmc_config())
    point = build_inversion_point(
        lon=0.0,
        lat=0.0,
        topo=1000.0,
        sediment=3.0,
        moho=40.0,
        vs_profile=make_profile(),
        cfg=cfg,
    )
    bounds = compute_point_bounds(point, PriorSettings.from_config(cfg))
    return point, bounds


@pytest.mark.parametrize("from_disk", [False, True])
def test_point_figures(tmp_path, write_mcmc_config, point_and_bounds, from_disk):
    cfg = load_config(write_mcmc_config())
    point, bounds = point_and_bounds
    curve = DispersionCurve(
        np.array([10.0, 5.0, 9.0, 6.0, 8.0, 7.0]),
        np.array([3.5, 3.0, 3.4, 3.1, 3.3, 3.2]),
        np.array([0.04, 0.03, np.nan, 0.03, 0.04, 0.03]),
    )
    try:
        if from_disk:
            rows = valid_dispersion_rows(curve, cfg)
            assert rows is not None
            FortranInputWriter(tmp_path / "grids", cfg).write_point(point, rows, bounds)
            output = plot_point_dir(tmp_path / "grids" / point.folder_name)
        else:
            output = tmp_path / "figure.png"
            _, axes = plot_point(
                point, curve, bounds, default_sigma=0.05, output_file=output
            )
            assert len(axes) == 2
            assert {ax.get_xlabel() for ax in axes} == {"Vs (km/s)", "Period (s)"}
        assert output.is_file() and output.stat().st_size > 0
    finally:
        matplotlib.pyplot.close("all")
