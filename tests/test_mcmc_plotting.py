import numpy as np
import pytest

matplotlib = pytest.importorskip("matplotlib")
matplotlib.use("Agg")

from seispy.mcmc.config import load_config  # noqa: E402
from seispy.mcmc.dispersion import DispersionCurve  # noqa: E402
from seispy.mcmc.inversion import build_inversion_point  # noqa: E402
from seispy.mcmc.plotting import (  # noqa: E402
    plot_dispersion,
    plot_model,
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


def test_plot_dispersion_sorts_periods_and_fills_sigma():
    curve = DispersionCurve(
        periods=np.array([20.0, 5.0, 10.0]),
        velocities=np.array([3.5, 3.0, 3.2]),
        sigmas=np.array([0.04, 0.03, np.nan]),
    )
    axes = plot_dispersion(curve, default_sigma=0.05)
    assert axes.get_xlabel() == "Period (s)"
    assert axes.get_ylabel() == "Phase velocity (km/s)"
    matplotlib.pyplot.close("all")


def test_plot_model_uses_the_final_bounds(point_and_bounds):
    point, bounds = point_and_bounds
    axes = plot_model(point, bounds)
    assert axes.get_xlabel() == "Vs (km/s)"
    assert axes.get_ylabel() == "Depth (km)"
    assert axes.get_title() == point.folder_name
    matplotlib.pyplot.close("all")


def test_plot_point_dir_round_trips_from_disk(
    tmp_path, write_mcmc_config, make_profile
):
    cfg = load_config(write_mcmc_config())
    point = build_inversion_point(0.0, 0.0, 0.0, 0.0, 40.0, make_profile(), cfg)
    bounds = compute_point_bounds(point, PriorSettings.from_config(cfg))
    curve = DispersionCurve(np.arange(5.0, 10.0), np.full(5, 3.5), np.full(5, 0.03))
    rows = valid_dispersion_rows(curve, cfg)
    assert rows is not None
    FortranInputWriter(tmp_path / "grids", cfg).write_point(point, rows, bounds)

    output = plot_point_dir(tmp_path / "grids" / point.folder_name)
    assert output.is_file() and output.stat().st_size > 0
    matplotlib.pyplot.close("all")


def test_plot_point_writes_a_two_panel_figure(tmp_path, point_and_bounds):
    point, bounds = point_and_bounds
    curve = DispersionCurve(
        periods=np.array([5.0, 10.0, 20.0]),
        velocities=np.array([3.0, 3.2, 3.5]),
        sigmas=np.array([0.03, 0.03, 0.04]),
    )
    output = tmp_path / "figure.png"
    _, axes = plot_point(point, curve, bounds, default_sigma=0.05, output_file=output)

    assert len(axes) == 2
    assert output.exists() and output.stat().st_size > 0
    matplotlib.pyplot.close("all")
