import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")

from seispy.mcmc.gridding import (  # noqa: E402
    TargetGrid,
    align_to_grid,
    long_to_grid,
    regular_grid_from_xyz,
    target_shape,
)


def test_target_grid():
    target = TargetGrid.from_region([0.0, 1.0, 0.0, 2.0], 0.5)
    np.testing.assert_allclose(target.x, [0.0, 0.5, 1.0])
    np.testing.assert_allclose(target.y, [0.0, 0.5, 1.0, 1.5, 2.0])
    assert target.shape == (5, 3) and target.size == 15
    lon, lat = target.flat_lonlat()
    np.testing.assert_allclose(lon, np.tile(target.x, 5))
    np.testing.assert_allclose(lat, np.repeat(target.y, 3))
    for region, spacing, shape in [
        ([0.0, 1.0, 0.0, 1.0], 0.25, (5, 5)),
        ([0.0, 2.0, 0.0, 1.0], 1.0, (2, 3)),
    ]:
        assert target_shape(region, spacing) == shape
    for region, spacing, error in [
        ([0.0, 1.0, 0.0, 1.0], 0.3, "divisible"),
        ([0.0, 0.0, 0.0, 1.0], 0.5, "positive extents"),
    ]:
        with pytest.raises(ValueError, match=error):
            target_shape(region, spacing)


def _bilinear_source():
    x = np.arange(0.0, 5.0)
    yy, xx = np.meshgrid(x, x, indexing="ij")
    return xr.DataArray(2.0 + xx + 3.0 * yy, dims=("y", "x"), coords={"y": x, "x": x})


@pytest.mark.parametrize(
    "scenario",
    [
        "align_to_grid_selects_an_aligned_grid_without_resampling",
        "align_to_grid_interpolates_a_coarser_source_exactly",
        "align_to_grid_averages_a_finer_source_over_the_target_cell",
        "align_to_grid_preserves_extra_dimensions_and_enforces_coverage",
    ],
)
def test_grid_alignment(scenario):
    if scenario == "align_to_grid_selects_an_aligned_grid_without_resampling":
        source = _bilinear_source()
        equal = align_to_grid(source, TargetGrid.from_region([1.0, 3.0, 1.0, 3.0], 1.0))
        assert equal.attrs["source_align_x"] == "identity"
        assert equal.attrs["source_align_y"] == "identity"
        np.testing.assert_allclose(equal.values, source.values[1:4, 1:4])
    elif scenario == "align_to_grid_interpolates_a_coarser_source_exactly":
        source = _bilinear_source()
        fine = align_to_grid(source, TargetGrid.from_region([0.0, 4.0, 0.0, 4.0], 0.5))
        assert fine.attrs["source_align_x"] == "interp"
        gx, gy = np.meshgrid(np.arange(0.0, 4.01, 0.5), np.arange(0.0, 4.01, 0.5))
        np.testing.assert_allclose(fine.values, 2.0 + gx + 3.0 * gy)
    elif scenario == "align_to_grid_averages_a_finer_source_over_the_target_cell":
        line = xr.DataArray(
            np.tile(np.array([0.0, 1.0, 2.0]), (3, 1)),
            dims=("y", "x"),
            coords={"y": [0.0, 1.0, 2.0], "x": [0.0, 1.0, 2.0]},
        )
        averaged = align_to_grid(
            line, TargetGrid.from_region([0.0, 2.0, 0.0, 2.0], 2.0)
        )
        assert averaged.attrs["source_align_x"] == "aggregate"
        np.testing.assert_allclose(averaged.values[0], [1.0 / 3.0, 5.0 / 3.0])

        constant = xr.DataArray(
            np.full((5, 5), 7.0),
            dims=("y", "x"),
            coords={"y": np.arange(5.0), "x": np.arange(5.0)},
        )
        coarse = align_to_grid(
            constant, TargetGrid.from_region([0.0, 4.0, 0.0, 4.0], 2.0)
        )
        assert coarse.shape == (3, 3)
        np.testing.assert_allclose(coarse.values, 7.0)
    elif scenario == "align_to_grid_preserves_extra_dimensions_and_enforces_coverage":
        x = np.arange(0.0, 5.0)
        cube = xr.DataArray(
            np.arange(2.0)[:, None, None] + np.zeros((2, 5, 5)),
            dims=("period", "y", "x"),
            coords={"period": [10.0, 20.0], "y": x, "x": x},
        )
        aligned = align_to_grid(cube, TargetGrid.from_region([1.0, 3.0, 1.0, 3.0], 1.0))
        assert aligned.dims == ("period", "y", "x")
        np.testing.assert_allclose(aligned.period, [10.0, 20.0])
        np.testing.assert_allclose(aligned.values[1], 1.0)

        with pytest.raises(ValueError, match="does not cover"):
            align_to_grid(cube, TargetGrid.from_region([10.0, 20.0, 0.0, 4.0], 1.0))

        partial = align_to_grid(
            cube, TargetGrid.from_region([3.0, 9.0, 0.0, 4.0], 1.0), on_missing="nan"
        )
        assert np.isnan(partial.sel(x=5.0).values).all()
        np.testing.assert_allclose(partial.sel(x=3.0).values[:, 0], [0.0, 1.0])


@pytest.mark.parametrize(
    "kind", ["valid", "missing", "duplicate", "nonuniform", "nan", "long"]
)
def test_spatial_table_geometry(kind):
    if kind == "long":
        frame = pd.DataFrame(
            dict(
                lon=[0.0, 0.5, 1.0, 1.0],
                lat=[0.0] * 4,
                period=[10.0] * 4,
                phv=[3.0, 3.1, 3.2, 3.3],
            )
        )
        cube = long_to_grid(frame, "phv", "period", extra_name="period")
        np.testing.assert_allclose(cube.x, [0.0, 0.5, 1.0])
        np.testing.assert_allclose(cube.values[0, 0, :], [3.0, 3.1, 3.25])
        frame.loc[3, "lon"] = 1.08
        with pytest.raises(ValueError, match="uniform spacing"):
            long_to_grid(frame, "phv", "period", extra_name="period")
        return
    data = np.array(
        [[x, y, x + 10 * y] for y in [2.0, 0.0, 1.0] for x in [2.0, 0.0, 1.0]]
    )
    if kind == "missing":
        data = data[:-1]
    elif kind == "duplicate":
        data[-1] = data[0]
    elif kind == "nonuniform":
        data[data[:, 0] == 1.0, 0] = 0.8
    elif kind == "nan":
        data[4, 2] = np.nan
    if kind != "valid":
        with pytest.raises(ValueError):
            regular_grid_from_xyz(data)
    else:
        grid = regular_grid_from_xyz(data)
        np.testing.assert_allclose(grid.x, [0.0, 1.0, 2.0])
        np.testing.assert_allclose(grid.y, [0.0, 1.0, 2.0])
        np.testing.assert_allclose(
            grid.values, [[0.0, 1.0, 2.0], [10.0, 11.0, 12.0], [20.0, 21.0, 22.0]]
        )
