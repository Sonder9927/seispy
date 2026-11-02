"""Readers and alignment for the MCMC input products.

One file covers the whole input layer: ``inputs`` (alias resolution),
``spatial`` (topography/sediment/Moho fields), ``dispersion`` (phase products)
and ``velocity`` (reference Vs models). All of them read a table or cube and
align it to the shared ``TargetGrid``.
"""

import sys

import numpy as np
import pandas as pd
import pytest

xr = pytest.importorskip("xarray")

from seispy.mcmc.config import load_config  # noqa: E402
from seispy.mcmc.dispersion import (  # noqa: E402
    DispersionCurve,
    build_dispersion_grid,
    read_dispersion_data,
)
from seispy.mcmc.gridding import TargetGrid  # noqa: E402
from seispy.mcmc.spatial import (  # noqa: E402
    ScalarFieldSpec,
    build_target_fields,
    interpolate_regular_grid,
    normalize_scalar_values,
    read_spatial_scalar,
)
from seispy.mcmc.velocity import (  # noqa: E402
    VsModelLibrary,
    VsProfile,
    velocity_at_depths,
)


def _target(region=(0.0, 1.0, 0.0, 1.0), spacing=0.5):
    return TargetGrid.from_region(list(region), spacing)


# =========================
# ALIASES AND TABLE READERS
# =========================


# =========================
# SPATIAL SCALAR FIELDS
# =========================


@pytest.mark.parametrize("format", ["csv", "parquet", "nc", "xyz"])
def test_scalar_sources(tmp_path, format):
    path = tmp_path / f"moho.{format}"
    values = np.array([40.0, 42.0])
    frame = pd.DataFrame(dict(longitude=[0.0, 1.0], latitude=[0.0, 0.0], depth=-values))
    if format == "csv":
        frame.to_csv(path, index=False)
    elif format == "parquet":
        frame.to_parquet(path, index=False)
    elif format == "nc":
        xr.Dataset(
            {"depth": (("latitude", "longitude"), [-values])},
            coords={"longitude": [0.0, 1.0], "latitude": [0.0]},
        ).to_netcdf(path)
    else:
        np.savetxt(path, frame.to_numpy())
    result = read_spatial_scalar(
        path,
        ScalarFieldSpec(
            "moho",
            convention="positive_depth",
            value_columns=("depth",),
            allow_zero=False,
        ),
    )
    np.testing.assert_allclose(result[:, 2], values)
    for data, zero, error in [
        ([1.0, -1.0], True, "mixed positive and negative"),
        ([1.0, 0.0], False, "zero values"),
    ]:
        with pytest.raises(ValueError, match=error):
            normalize_scalar_values(
                np.array(data),
                convention="positive_depth",
                name="field",
                allow_zero=zero,
            )


@pytest.mark.parametrize(
    "region,spacing",
    [
        ((0.0, 2.0, 0.0, 2.0), 1.0),
        ((0.5, 1.5, 0.5, 1.5), 0.5),
        ((-0.5, 2.0, 0.0, 2.0), 0.5),
    ],
)
def test_regular_interpolation(monkeypatch, region, spacing):
    monkeypatch.setitem(sys.modules, "pygmt", None)
    source = np.array(
        [
            [x, y, 2 + x + 3 * y + x * y]
            for y in [2.0, 1.0, 0.0]
            for x in [0.0, 1.0, 2.0]
        ]
    )
    target = _target(region, spacing)
    if region[0] < 0:
        with pytest.raises(ValueError, match="does not cover"):
            interpolate_regular_grid(source, target)
    else:
        result = interpolate_regular_grid(source[::-1], target)
        x, y = np.meshgrid(target.x, target.y)
        np.testing.assert_allclose(result.values, 2 + x + 3 * y + x * y)
        np.testing.assert_array_equal(result.x, target.x)
        np.testing.assert_array_equal(result.y, target.y)


@pytest.mark.parametrize("descending", [False, True])
def test_netcdf_crop_keeps_boundary_neighbors(tmp_path, descending):
    x = np.arange(-0.5, 3.0, 1.0)
    y = x[::-1] if descending else x
    z = y[:, None] + 2 * x[None, :]
    path = tmp_path / "grid.nc"
    xr.Dataset({"z": (("lat", "lon"), z)}, coords={"lat": y, "lon": x}).to_netcdf(path)

    xyz = read_spatial_scalar(
        path,
        ScalarFieldSpec("topography", value_columns=("z",)),
        region=[0.0, 2.0, 0.0, 2.0],
    )
    assert xyz[:, 0].min() == -0.5
    assert xyz[:, 0].max() == 2.5

    result = interpolate_regular_grid(xyz, _target((0.0, 2.0, 0.0, 2.0), 0.5))
    tx, ty = np.meshgrid(np.arange(0.0, 2.1, 0.5), np.arange(0.0, 2.1, 0.5))
    np.testing.assert_allclose(result.values, ty + 2 * tx)


def test_build_target_fields_on_the_target_grid(
    tmp_path, monkeypatch, write_mcmc_config
):
    monkeypatch.setitem(sys.modules, "pygmt", None)
    x = np.array([-0.5, 0.5, 1.5])
    topo = tmp_path / "topo.nc"
    xr.Dataset(
        {"z": (("lat", "lon"), np.full((3, 3), -1500.0))},
        coords={"lat": x, "lon": x},
    ).to_netcdf(topo)
    sediment = tmp_path / "sed.xyz"
    moho = tmp_path / "moho.csv"
    rows = np.array([[a, b, 2.0 + a + b] for b in x for a in x])
    np.savetxt(sediment, rows[::-1])
    pd.DataFrame(
        {"lon": rows[:, 0], "lat": rows[:, 1], "moho": np.full(9, 40.0)}
    ).to_csv(moho, index=False)

    config_path = write_mcmc_config(
        paths={
            "topography_file": str(topo),
            "sediment_file": str(sediment),
            "moho_file": str(moho),
        }
    )
    cfg = load_config(config_path)
    target = _target((0.0, 1.0, 0.0, 1.0), 1.0)
    fields = build_target_fields(cfg, target)

    assert fields.shape == target.shape
    np.testing.assert_allclose(fields.topo, -1500.0)
    np.testing.assert_allclose(fields.sediment, [[2.0, 3.0], [3.0, 4.0]])
    np.testing.assert_allclose(fields.moho, 40.0)


# =========================
# PHASE DISPERSION
# =========================


@pytest.mark.parametrize("format", ["csv", "parquet", "nc", "missing"])
def test_dispersion_sources(tmp_path, write_mcmc_config, format):
    path = tmp_path / ("phase.csv" if format == "missing" else f"phase.{format}")
    periods = np.array([10.0, 20.0])
    axis = np.arange(0.0, 1.01, 0.25)
    rows = [(x, y, t, 3.0 + 0.1 * x, 10.0) for t in periods for y in axis for x in axis]
    frame = pd.DataFrame(rows, columns=["x", "y", "T", "vel", "uncertainty"])
    if format == "missing":
        frame.drop(columns="vel").to_csv(path, index=False)
        with pytest.raises(ValueError, match="phase velocity"):
            read_dispersion_data(path)
        return
    if format == "nc":
        cube = frame.vel.to_numpy().reshape(2, 5, 5).transpose(1, 2, 0)
        xr.Dataset(
            {
                "phase_velocity": (("y", "x", "period"), cube),
                "error": (("y", "x", "period"), np.full(cube.shape, 10.0)),
            },
            coords={"x": axis, "y": axis, "period": periods},
        ).to_netcdf(path)
    elif format == "parquet":
        frame.to_parquet(path, index=False)
    else:
        frame.to_csv(path, index=False)
    data = read_dispersion_data(path)
    assert list(data.columns) == ["lon", "lat", "period", "phv", "std"]
    np.testing.assert_allclose(data.phv, 3.0 + 0.1 * data.lon)
    np.testing.assert_allclose(data["std"], 10.0)
    cfg = load_config(
        write_mcmc_config(grid_spacing=0.5, paths={"phase_dispersion_file": str(path)})
    )
    grid = build_dispersion_grid(
        cfg, TargetGrid.from_region(cfg.region, cfg.grid_spacing)
    )
    np.testing.assert_allclose(grid.periods, periods)
    assert grid.velocities.shape == (2, 3, 3)
    assert np.isfinite(grid.velocities).all()
    assert grid.velocities.min() >= 3.0 and grid.velocities.max() <= 3.1
    np.testing.assert_allclose(grid.sigmas, 10.0 * cfg.input_units.phase_std_to_km_s)
    # Exercise invalid rows and uncertainty fallback on the same public curve.
    curve = DispersionCurve(
        np.array([10.0, np.nan, 30.0, 40.0]),
        np.array([3.0, 3.1, np.nan, 3.4]),
        np.array([np.nan, 0.03, 0.03, -1.0]),
    )
    assert curve.valid_rows(0.05) == [(10.0, 3.0, 0.05), (40.0, 3.4, 0.05)]


# =========================
# REFERENCE VS MODELS
# =========================


@pytest.mark.parametrize(
    "scenario",
    [
        "vs_profile_rejects_invalid_data",
        "velocity_at_depths_allows_limited_shallow_extrapolation",
        "vs_library_accepts_csv_aliases_and_scales_velocity",
        "vs_library_accepts_netcdf_aliases",
        "vs_library_rejects_duplicate_rows_and_negative_depth",
        "vs_library_aligns_to_the_inversion_grid",
    ],
)
def test_velocity_sources(tmp_path, scenario):
    if scenario == "vs_profile_rejects_invalid_data":
        with pytest.raises(ValueError, match="strictly increasing"):
            VsProfile(0.0, 0.0, np.array([0.0, 0.0]), np.array([3.0, 3.2]))
        with pytest.raises(ValueError, match="positive downward"):
            VsProfile(0.0, 0.0, np.array([-1.0, 1.0]), np.array([3.0, 3.2]))
        with pytest.raises(ValueError, match="at least two depth samples"):
            VsProfile(0.0, 0.0, np.array([0.0]), np.array([3.0]))
    elif scenario == "velocity_at_depths_allows_limited_shallow_extrapolation":
        profile = VsProfile(
            lon=0.0,
            lat=0.0,
            depth=np.array([3.0, 5.0, 40.0]),
            vs=np.array([3.0, 3.2, 4.2]),
        )

        values = velocity_at_depths(profile, np.array([0.0, 3.0, 5.0]))
        np.testing.assert_allclose(values, [2.7, 3.0, 3.2])

        with pytest.raises(ValueError, match="shallow Vs extrapolation"):
            velocity_at_depths(
                profile, np.array([0.0]), max_shallow_extrapolation_km=2.0
            )

        with pytest.raises(ValueError, match="shallow gap"):
            velocity_at_depths(
                profile, np.array([0.0]), allow_shallow_extrapolation=False
            )

        with pytest.raises(ValueError, match="exceed reference profile coverage"):
            velocity_at_depths(profile, np.array([41.0]))
    elif scenario == "vs_library_accepts_csv_aliases_and_scales_velocity":
        path = tmp_path / "reference.csv"
        pd.DataFrame(
            {
                "longitude": [0.0, 0.0],
                "latitude": [0.0, 0.0],
                "depth": [0.0, 300.0],
                "vsv": [3000.0, 4700.0],
            }
        ).to_csv(path, index=False)

        library = VsModelLibrary.from_file(
            path, target=_target(spacing=1.0), vs_scale=0.001
        )
        profile = library.profile_at(0.0, 0.0)
        np.testing.assert_allclose(profile.vs, [3.0, 4.7])
    elif scenario == "vs_library_accepts_netcdf_aliases":
        path = tmp_path / "reference.nc"
        depths = np.array([0.0, 5.0, 40.0, 300.0])
        xr.Dataset(
            {"shear_velocity": (("lat", "lon", "depth"), [[[3.0, 3.2, 4.2, 4.7]]])},
            coords={"lon": [0.0], "lat": [0.0], "depth": depths},
        ).to_netcdf(path)

        library = VsModelLibrary.from_file(path, target=_target(spacing=1.0))
        profile = library.profile_at(0.0, 0.0)
        np.testing.assert_allclose(profile.depth, depths)
        np.testing.assert_allclose(profile.vs, [3.0, 3.2, 4.2, 4.7])
    elif scenario == "vs_library_rejects_duplicate_rows_and_negative_depth":
        path = tmp_path / "reference.csv"
        pd.DataFrame(
            {
                "lon": [0.0, 0.0, 0.0],
                "lat": [0.0, 0.0, 0.0],
                "z": [0.0, 0.0, 10.0],
                "vs": [3.0, 3.1, 4.0],
            }
        ).to_csv(path, index=False)
        target = _target(spacing=1.0)
        with pytest.raises(ValueError, match="duplicate"):
            VsModelLibrary.from_file(path, target=target)

        path = tmp_path / "reference_negative.csv"
        pd.DataFrame(
            {"lon": [0.0, 0.0], "lat": [0.0, 0.0], "z": [-1.0, 10.0], "vs": [3.0, 4.0]}
        ).to_csv(path, index=False)
        with pytest.raises(ValueError, match="positive depth"):
            VsModelLibrary.from_file(path, target=target)
    elif scenario == "vs_library_aligns_to_the_inversion_grid":
        lon = np.arange(0.0, 1.01, 0.25)
        lat = np.arange(0.0, 1.01, 0.25)
        rows = [
            (lo, la, z, 3.0 + 0.1 * lo + 0.02 * z)
            for la in lat
            for lo in lon
            for z in (0.0, 10.0, 20.0)
        ]
        path = tmp_path / "reference.csv"
        pd.DataFrame(rows, columns=["lon", "lat", "z", "vs"]).to_csv(path, index=False)

        identity = VsModelLibrary.from_file(path, target=_target(spacing=0.25))
        np.testing.assert_allclose(identity.profile_at(0.0, 0.0).vs, [3.0, 3.2, 3.4])

        aligned = VsModelLibrary.from_file(path, target=_target(spacing=0.5))
        assert len(aligned.profiles) == 9
        profile = aligned.profile_at(0.5, 0.5)
        np.testing.assert_allclose(profile.depth, [0.0, 10.0, 20.0])
        np.testing.assert_allclose(profile.vs, [3.05, 3.25, 3.45])
