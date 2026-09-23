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
    DispersionGrid,
    build_dispersion_grid,
    read_dispersion_data,
)
from seispy.mcmc.gridding import TargetGrid  # noqa: E402
from seispy.mcmc.inputs import (  # noqa: E402
    coordinate_key,
    coordinate_pair_key,
    netcdf_variable_frame,
    pick_name,
    read_table,
)
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


def test_pick_name_returns_first_candidate_available():
    # Candidate order wins: the caller states its alias preference.
    assert pick_name(["foo", "lon", "x"], ("x", "lon")) == "x"
    assert pick_name(["foo", "lon"], ("x", "lon")) == "lon"
    assert pick_name(["foo"], ("x", "lon")) is None


def test_coordinate_keys_round_to_the_shared_precision():
    assert coordinate_key(1.0000004) == 1.0
    assert coordinate_pair_key(1.0000004, 2.0000004) == (1.0, 2.0)


def test_read_table_accepts_csv(tmp_path):
    csv = tmp_path / "table.csv"
    pd.DataFrame({"a": [1, 2]}).to_csv(csv, index=False)
    pd.testing.assert_frame_equal(read_table(csv), pd.read_csv(csv))

    with pytest.raises(ValueError, match="Expected CSV or Parquet"):
        read_table(tmp_path / "table.txt")


def test_read_table_accepts_parquet(tmp_path, requires_pyarrow):
    parquet = tmp_path / "table.parquet"
    pd.DataFrame({"a": [1, 2]}).to_parquet(parquet, index=False)
    pd.testing.assert_frame_equal(read_table(parquet), pd.DataFrame({"a": [1, 2]}))


def test_netcdf_variable_frame_exposes_coordinates(tmp_path):
    path = tmp_path / "grid.nc"
    xr.Dataset(
        {"shear_velocity": (("lat", "lon"), [[3.0, 3.1]])},
        coords={"lon": [10.0, 11.0], "lat": [50.0]},
    ).to_netcdf(path)

    frame = netcdf_variable_frame(path, ("vs", "vsv", "shear_velocity"), name="model")
    assert set(frame.columns) == {"lon", "lat", "shear_velocity"}
    np.testing.assert_allclose(frame["shear_velocity"], [3.0, 3.1])

    with pytest.raises(ValueError, match="cannot determine NetCDF value variable"):
        netcdf_variable_frame(path, ("unknown",), name="model")


# =========================
# SPATIAL SCALAR FIELDS
# =========================


@pytest.mark.parametrize(
    "convention,values,expected",
    [
        ("raw", [-1.0, 0.0, 1.0], [-1.0, 0.0, 1.0]),
        ("positive_depth", [1.0, 2.0], [1.0, 2.0]),
        ("positive_thickness", [-1.0, -2.0], [1.0, 2.0]),
        ("positive_thickness", [0.0, 0.0], [0.0, 0.0]),
    ],
)
def test_normalize_scalar_values(convention, values, expected):
    result = normalize_scalar_values(
        np.asarray(values, dtype=float),
        convention=convention,
        name="field",
        allow_zero=True,
    )
    np.testing.assert_allclose(result, expected)


def test_normalize_scalar_values_rejects_mixed_signs_and_forbidden_zero():
    with pytest.raises(ValueError, match="mixed positive and negative"):
        normalize_scalar_values(
            np.array([1.0, -1.0]),
            convention="positive_depth",
            name="field",
            allow_zero=True,
        )
    with pytest.raises(ValueError, match="zero values"):
        normalize_scalar_values(
            np.array([1.0, 0.0]),
            convention="positive_depth",
            name="field",
            allow_zero=False,
        )


def test_read_spatial_scalar_accepts_csv_and_netcdf(tmp_path):
    topography = tmp_path / "topography.csv"
    pd.DataFrame(
        {
            "longitude": [0.0, 1.0],
            "latitude": [0.0, 1.0],
            "elevation": [100.0, 200.0],
        }
    ).to_csv(topography, index=False)

    sediment = tmp_path / "sediment.nc"
    xr.Dataset(
        {"thickness": (("latitude", "longitude"), [[5.0, 6.0], [7.0, 8.0]])},
        coords={"longitude": [0.0, 1.0], "latitude": [0.0, 1.0]},
    ).to_netcdf(sediment)

    moho = tmp_path / "moho.xyz"
    np.savetxt(moho, np.array([[0.0, 0.0, -40.0], [1.0, 1.0, -42.0]]))

    topo = read_spatial_scalar(
        topography, ScalarFieldSpec(name="topography", value_columns=("elevation",))
    )
    sed = read_spatial_scalar(
        sediment,
        ScalarFieldSpec(
            name="sediment",
            convention="positive_thickness",
            value_columns=("thickness",),
        ),
    )
    moho_values = read_spatial_scalar(
        moho,
        ScalarFieldSpec(
            name="moho",
            convention="positive_depth",
            value_columns=(),
            allow_zero=False,
        ),
    )

    np.testing.assert_allclose(topo[:, 2], [100.0, 200.0])
    np.testing.assert_allclose(sed[:, 2], [5.0, 6.0, 7.0, 8.0])
    np.testing.assert_allclose(moho_values[:, 2], [40.0, 42.0])


def test_read_spatial_scalar_accepts_parquet(tmp_path, requires_pyarrow):
    path = tmp_path / "topography.parquet"
    pd.DataFrame({"x": [0.0, 1.0], "y": [0.0, 1.0], "topo": [1.0, 2.0]}).to_parquet(
        path, index=False
    )
    result = read_spatial_scalar(
        path, ScalarFieldSpec("topography", value_columns=("topo",))
    )
    np.testing.assert_allclose(result[:, 2], [1.0, 2.0])


def test_interpolate_regular_grid_is_exact_for_a_bilinear_field(monkeypatch):
    monkeypatch.setitem(sys.modules, "pygmt", None)
    source = np.array(
        [
            [x, y, 2 + x + 3 * y + x * y]
            for y in [2.0, 1.0, 0.0]
            for x in [0.0, 1.0, 2.0]
        ]
    )
    result = interpolate_regular_grid(source[::-1], _target((0.5, 1.5, 0.5, 1.5), 0.5))
    x, y = np.meshgrid([0.5, 1.0, 1.5], [0.5, 1.0, 1.5])
    np.testing.assert_allclose(result.values, 2 + x + 3 * y + x * y)
    np.testing.assert_array_equal(result.x, x[0])
    np.testing.assert_array_equal(result.y, y[:, 0])


def test_interpolate_regular_grid_copies_an_exact_target_without_gmt(monkeypatch):
    monkeypatch.setitem(sys.modules, "pygmt", None)
    source = np.array(
        [[x, y, x + 10.0 * y] for y in [0.0, 0.5, 1.0] for x in [0.0, 0.5, 1.0]]
    )
    result = interpolate_regular_grid(source, _target(spacing=0.5))
    np.testing.assert_allclose(result.coords["x"], [0.0, 0.5, 1.0])
    np.testing.assert_allclose(result.coords["y"], [0.0, 0.5, 1.0])
    np.testing.assert_allclose(
        result.values, [[0.0, 0.5, 1.0], [5.0, 5.5, 6.0], [10.0, 10.5, 11.0]]
    )


def test_interpolate_regular_grid_rejects_targets_outside_the_source():
    source = np.array([[x, y, x + y] for y in [0.0, 1.0, 2.0] for x in [0.0, 1.0, 2.0]])
    with pytest.raises(ValueError, match="does not cover"):
        interpolate_regular_grid(source, _target((-0.5, 2.0, 0.0, 2.0), 0.5))


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


def test_read_dispersion_data_accepts_netcdf_aliases(tmp_path):
    phase = tmp_path / "phase.nc"
    xr.Dataset(
        {
            "phase_velocity": (("y", "x", "period"), [[[3.1, 3.3]]]),
            "error": (("y", "x", "period"), [[[20.0, 25.0]]]),
        },
        coords={"x": [0.0], "y": [0.0], "period": [10.0, 20.0]},
    ).to_netcdf(phase)

    frame = read_dispersion_data(phase)
    assert list(frame.columns) == ["lon", "lat", "period", "phv", "std"]
    np.testing.assert_allclose(frame["phv"], [3.1, 3.3])
    np.testing.assert_allclose(frame["std"], [20.0, 25.0])


def test_read_dispersion_data_accepts_parquet_aliases(tmp_path, requires_pyarrow):
    phase = tmp_path / "phase.parquet"
    pd.DataFrame(
        {
            "x": [0.0, 0.0],
            "y": [0.0, 0.0],
            "T": [10.0, 20.0],
            "vel": [3.1, 3.3],
            "uncertainty": [20.0, 25.0],
        }
    ).to_parquet(phase, index=False)

    frame = read_dispersion_data(phase)
    np.testing.assert_allclose(frame["phv"], [3.1, 3.3])
    np.testing.assert_allclose(frame["std"], [20.0, 25.0])


def test_read_dispersion_data_rejects_missing_columns(tmp_path):
    phase = tmp_path / "phase.csv"
    pd.DataFrame({"lon": [0.0], "lat": [0.0], "period": [10.0]}).to_csv(
        phase, index=False
    )
    with pytest.raises(ValueError, match="phase velocity"):
        read_dispersion_data(phase)


def test_dispersion_curve_valid_rows_filters_and_fills_sigma():
    curve = DispersionCurve(
        periods=np.array([10.0, np.nan, 30.0, 40.0]),
        velocities=np.array([3.0, 3.1, np.nan, 3.4]),
        sigmas=np.array([np.nan, 0.03, 0.03, -1.0]),
    )
    assert curve.valid_rows(0.05) == [(10.0, 3.0, 0.05), (40.0, 3.4, 0.05)]
    with pytest.raises(ValueError, match="default_sigma"):
        curve.valid_rows(0.0)


def test_dispersion_grid_curve_at_uses_row_major_order():
    periods = np.array([10.0, 20.0])
    velocities = np.arange(2 * 2 * 2, dtype=float).reshape(2, 2, 2)
    sigmas = np.full((2, 2, 2), 0.1)
    grid = DispersionGrid(periods=periods, velocities=velocities, sigmas=sigmas)

    np.testing.assert_allclose(grid.curve_at(0).velocities, [0.0, 4.0])
    np.testing.assert_allclose(grid.curve_at(3).velocities, [3.0, 7.0])
    with pytest.raises(IndexError):
        grid.curve_at(4)


def test_build_dispersion_grid_aligns_a_regular_source(tmp_path, write_mcmc_config):
    lon = np.arange(0.0, 1.01, 0.25)
    lat = np.arange(0.0, 1.01, 0.25)
    rows = [
        (period, lo, la, 3.0 + 0.1 * lo, 10.0)
        for period in (10.0, 20.0)
        for la in lat
        for lo in lon
    ]
    phase = tmp_path / "phase.csv"
    pd.DataFrame(rows, columns=["period", "lon", "lat", "phv", "std"]).to_csv(
        phase, index=False
    )

    config_path = write_mcmc_config(
        grid_spacing=0.5, paths={"phase_dispersion_file": str(phase)}
    )
    cfg = load_config(config_path)
    target = TargetGrid.from_region(cfg.region, cfg.grid_spacing)

    grid = build_dispersion_grid(cfg, target)

    np.testing.assert_allclose(grid.periods, [10.0, 20.0])
    assert grid.velocities.shape == (2, 3, 3)
    assert np.isfinite(grid.velocities).all()
    assert grid.velocities.min() >= 3.0 and grid.velocities.max() <= 3.1
    np.testing.assert_allclose(grid.sigmas, 10.0 * cfg.input_units.phase_std_to_km_s)


# =========================
# REFERENCE VS MODELS
# =========================


def test_vs_profile_rejects_invalid_data():
    with pytest.raises(ValueError, match="strictly increasing"):
        VsProfile(0.0, 0.0, np.array([0.0, 0.0]), np.array([3.0, 3.2]))
    with pytest.raises(ValueError, match="positive downward"):
        VsProfile(0.0, 0.0, np.array([-1.0, 1.0]), np.array([3.0, 3.2]))
    with pytest.raises(ValueError, match="at least two depth samples"):
        VsProfile(0.0, 0.0, np.array([0.0]), np.array([3.0]))


def test_velocity_at_depths_allows_limited_shallow_extrapolation():
    profile = VsProfile(
        lon=0.0, lat=0.0, depth=np.array([3.0, 5.0, 40.0]), vs=np.array([3.0, 3.2, 4.2])
    )

    values = velocity_at_depths(profile, np.array([0.0, 3.0, 5.0]))
    np.testing.assert_allclose(values, [2.7, 3.0, 3.2])

    with pytest.raises(ValueError, match="shallow Vs extrapolation"):
        velocity_at_depths(profile, np.array([0.0]), max_shallow_extrapolation_km=2.0)

    with pytest.raises(ValueError, match="shallow gap"):
        velocity_at_depths(profile, np.array([0.0]), allow_shallow_extrapolation=False)

    with pytest.raises(ValueError, match="exceed reference profile coverage"):
        velocity_at_depths(profile, np.array([41.0]))


def test_vs_library_accepts_csv_aliases_and_scales_velocity(tmp_path):
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


def test_vs_library_accepts_netcdf_aliases(tmp_path):
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


def test_vs_library_rejects_duplicate_rows_and_negative_depth(tmp_path):
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


def test_vs_library_aligns_to_the_inversion_grid(tmp_path):
    lon = np.arange(0.0, 1.01, 0.25)
    lat = np.arange(0.0, 1.01, 0.25)
    rows = [
        (lo, la, z, 3.0 + 0.2 * lo + 0.1 * z)
        for la in lat
        for lo in lon
        for z in (0.0, 10.0, 20.0)
    ]
    path = tmp_path / "reference.csv"
    pd.DataFrame(rows, columns=["lon", "lat", "z", "vs"]).to_csv(path, index=False)

    identity = VsModelLibrary.from_file(path, target=_target(spacing=0.25))
    np.testing.assert_allclose(identity.profile_at(0.0, 0.0).vs, [3.0, 4.0, 5.0])

    aligned = VsModelLibrary.from_file(path, target=_target(spacing=0.5))
    assert len(aligned.profiles) == 9
    profile = aligned.profile_at(0.5, 0.5)
    np.testing.assert_allclose(profile.depth, [0.0, 10.0, 20.0])
    np.testing.assert_allclose(profile.vs, [3.1, 4.1, 5.1])


def test_vs_library_profile_at_reports_grid_coordinate(tmp_path):
    path = tmp_path / "reference.csv"
    pd.DataFrame(
        {"lon": [0.0, 0.0], "lat": [0.0, 0.0], "z": [0.0, 10.0], "vs": [3.0, 4.0]}
    ).to_csv(path, index=False)
    library = VsModelLibrary.from_file(path, target=_target(spacing=1.0))
    with pytest.raises(KeyError, match="no profile at inversion-grid coordinate"):
        library.profile_at(0.5, 0.5)
