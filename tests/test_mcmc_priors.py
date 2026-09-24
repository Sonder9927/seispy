import json

import numpy as np
import pandas as pd
import pytest

from seispy.mcmc.bspline import (
    basis_geometry,
    basis_matrix,
    fortran_knot_vector,
    greville_depths,
    node_matrix,
)
from seispy.mcmc.config import SearchRadius, VsConstraints, load_config
from seispy.mcmc.dispersion import DispersionCurve, valid_dispersion_rows
from seispy.mcmc.inversion import build_inversion_point
from seispy.mcmc.priors import (
    LayerSpec,
    PriorBound,
    PriorSettings,
    _model_space_window,
    _projection_centers,
    _repair_moho_jump,
    _sediment_bounds,
    compute_point_bounds,
    greville_reference_centers,
    minimum_vs,
    reconstruct_initial_model,
)
from seispy.mcmc.plotting import plot_point_dir
from seispy.mcmc.serialization import FortranInputWriter
from seispy.mcmc.velocity import VsProfile


def _settings(vc: VsConstraints | None = None, **overrides) -> PriorSettings:
    values = {
        "search_radius": SearchRadius(
            sediment=0.2, moho=1.0, crust_vs=0.1, mantle_vs=0.1
        ),
        "vs_constraints": vc or VsConstraints(),
        "sediment_intervals": ((0.3, 1.0),),
        "n_coeff_crust": 4,
        "n_coeff_mantle": 5,
        "factor": 2.0,
    }
    values.update(overrides)
    return PriorSettings(**values)


def _write(cfg, point, phase, base, *, plot=False):
    bounds = compute_point_bounds(point, PriorSettings.from_config(cfg))
    rows = valid_dispersion_rows(phase, cfg)
    if rows is None:
        return False, bounds
    FortranInputWriter(base, cfg).write_point(point, rows, bounds)
    if plot:
        plot_point_dir(base / point.folder_name)
    return True, bounds


# =========================
# FORTRAN SPLINE GEOMETRY
# =========================


def test_fortran_knot_vector_shape_and_interior_knot():
    knots = fortran_knot_vector(4, 5.0, 40.0, 2.0)
    assert knots.shape == (7,)
    assert knots[3] == pytest.approx(5.0 + 35.0 / 3.0)
    assert knots[0] == 5.0
    assert knots[-1] == 40.0


def test_greville_depths_match_the_documented_fortran_example():
    crust = greville_depths(4, 5.0, 40.0, 2.0)
    mantle = greville_depths(5, 40.0, 300.0, 2.0)
    np.testing.assert_allclose(
        crust, [5.000525, 10.833683, 28.332983, 39.999475], atol=1e-6
    )
    np.testing.assert_allclose(
        mantle,
        [40.005200, 68.893222, 155.555556, 242.217889, 299.994800],
        atol=1e-6,
    )


def test_greville_depths_reject_invalid_intervals():
    with pytest.raises(ValueError, match="n_basis must be >= 3"):
        greville_depths(2, 0.0, 10.0, 2.0)
    with pytest.raises(ValueError, match="Invalid spline interval"):
        greville_depths(4, 10.0, 10.0, 2.0)
    with pytest.raises(ValueError, match="factor must be finite"):
        greville_depths(4, 0.0, 10.0, 0.0)


@pytest.mark.parametrize(
    "n_basis,z_top,z_bottom",
    [(4, 3.0, 33.01), (5, 33.01, 300.0), (6, 0.0, 100.0)],
)
def test_basis_matrix_is_a_partition_of_unity(n_basis, z_top, z_bottom):
    depths = np.linspace(z_top, z_bottom, 501)[1:-1]
    values = basis_matrix(n_basis, z_top, z_bottom, 2.0, depths)

    assert values.shape == (depths.size, n_basis)
    np.testing.assert_allclose(values.sum(axis=1), 1.0, atol=1e-12)
    assert (values >= -1e-15).all()


def test_basis_matrix_rejects_non_1d_depths():
    with pytest.raises(ValueError, match="depths must be a 1-D array"):
        basis_matrix(4, 0.0, 10.0, 2.0, np.zeros((2, 2)))


def test_basis_geometry_locates_each_coefficient_inside_the_layer():
    n_basis, z_top, z_bottom = 4, 3.0, 33.01
    mass, centroid = basis_geometry(n_basis, z_top, z_bottom, 2.0)
    greville = greville_depths(n_basis, z_top, z_bottom, 2.0)

    np.testing.assert_allclose(mass.sum(), 1.0, atol=1e-12)
    assert mass.shape == centroid.shape == (n_basis,)
    assert (centroid > z_top).all() and (centroid < z_bottom).all()

    # The endpoint basis functions carry their weight well inside the layer,
    # unlike the Greville abscissae which the knot rule pins on the boundaries.
    assert centroid[0] > greville[0] + 1.0
    assert centroid[-1] < greville[-1] - 1.0
    # Interior coefficients carry more mass than the two endpoint ones.
    assert mass[0] < mass[1] and mass[-1] < mass[-2]


def test_basis_geometry_rejects_invalid_sample_counts():
    with pytest.raises(ValueError, match="samples must be >= 2"):
        basis_geometry(4, 0.0, 10.0, 2.0, samples=1)


# =========================
# WRITER INTEGRATION
# =========================


def test_writer_records_shallow_extrapolated_prior_centers(
    tmp_path, write_mcmc_config, make_profile
):
    cfg = load_config(write_mcmc_config())
    point = build_inversion_point(
        lon=0.0,
        lat=0.0,
        topo=0.0,
        sediment=0.0,
        moho=40.0,
        vs_profile=make_profile(depths=(3.0, 5.0, 40.0, 300.0)),
        cfg=cfg,
    )
    phase = DispersionCurve(
        periods=np.arange(10.0, 60.0, 10.0),
        velocities=np.full(5, 3.5),
        sigmas=np.full(5, 0.03),
    )

    written, point_bounds = _write(cfg, point, phase, tmp_path / "grids")
    assert written
    bounds = pd.read_csv(tmp_path / "grids" / "0.00_0.00" / "prior_bounds.csv")
    crust = bounds[bounds["section"] == "crust"]
    mantle = bounds[bounds["section"] == "mantle"]
    # The first crustal Greville label sits above the reference coverage.
    assert crust.iloc[0]["shallow_extrapolated"] == 1
    assert crust.iloc[1:]["shallow_extrapolated"].eq(0).all()
    assert mantle["shallow_extrapolated"].eq(0).all()
    # The admissible Vs domain is enforced on the reconstructed model, not on
    # each coefficient, so a coefficient upper bound may pass 4.9.
    model_depth, model_vs = reconstruct_initial_model(point, point_bounds)
    assert model_vs.max() <= 4.9 + 1e-9
    assert bounds["lower_km_s"].ge(0.5).all()
    assert mantle.iloc[-1]["lower_km_s"] >= 4.0
    assert (
        mantle.iloc[0]["initial_midpoint_vs_km_s"]
        > crust.iloc[-1]["initial_midpoint_vs_km_s"]
    )


@pytest.mark.parametrize(
    "intervals",
    [
        # The reference executable uses two sediment parameters.
        [[0.2, 2.5], [0.5, 3.0]],
        [[0.3, 1.0], [0.4, 1.2], [0.5, 1.4]],
    ],
)
def test_writer_writes_one_sediment_parameter_per_interval(
    tmp_path, write_mcmc_config, make_profile, intervals
):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["sediment_vs"] = intervals
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    cfg = load_config(config_path)
    point = build_inversion_point(
        lon=0.0,
        lat=0.0,
        topo=0.0,
        sediment=5.0,
        moho=40.0,
        vs_profile=make_profile(depths=(3.0, 5.0, 40.0, 300.0)),
        cfg=cfg,
    )
    phase = DispersionCurve(
        periods=np.arange(5.0, 10.0),
        velocities=np.full(5, 3.5),
        sigmas=np.full(5, 0.03),
    )

    written, _ = _write(cfg, point, phase, tmp_path / "grids")
    assert written
    sediment_lines = [
        line.split()
        for line in (tmp_path / "grids" / "0.00_0.00" / "para.inp")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.startswith("10 ")
    ]
    assert len(sediment_lines) == len(intervals)
    assert [int(line[1]) for line in sediment_lines] == list(
        range(1, len(intervals) + 1)
    )
    centres = [(float(line[2]) + float(line[3])) / 2.0 for line in sediment_lines]
    assert centres == sorted(centres) and len(set(centres)) == len(centres)


@pytest.mark.parametrize(
    "crust_vs,mantle_vs,expected",
    [
        (None, None, {"crust": [0.30] * 4, "mantle": [0.20] * 5}),
        (0.1, 0.1, {"crust": [0.1] * 4, "mantle": [0.1] * 5}),
        (
            [0.05, 0.10, 0.15, 0.20],
            0.1,
            {"crust": [0.05, 0.10, 0.15, 0.20], "mantle": [0.1] * 5},
        ),
    ],
)
def test_configured_half_widths_reach_para_with_projection_centers(
    tmp_path, write_mcmc_config, make_profile, crust_vs, mantle_vs, expected
):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    radius = {"sediment": 0.2, "moho": 1.0}
    if crust_vs is not None:
        radius["crust_vs"] = crust_vs
    if mantle_vs is not None:
        radius["mantle_vs"] = mantle_vs
    raw["search_radius"] = radius
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    cfg = load_config(config_path)
    settings = PriorSettings.from_config(cfg)

    point = build_inversion_point(0.0, 0.0, 0.0, 0.0, 40.0, make_profile(), cfg)
    phase = DispersionCurve(
        np.arange(10.0, 60.0, 10.0), np.full(5, 3.5), np.full(5, 0.03)
    )
    assert _write(cfg, point, phase, tmp_path / "grids")[0]
    audit = pd.read_csv(tmp_path / "grids" / point.folder_name / "prior_bounds.csv")

    for section, z_top, z_bottom in (
        ("crust", point.crustal_spline_top, point.moho_depth),
        ("mantle", point.moho_depth, point.max_depth),
    ):
        rows = audit[audit["section"] == section]
        np.testing.assert_allclose(rows["search_radius_km_s"], expected[section])
        independent, _ = _projection_centers(
            point, settings, settings.layer(section, z_top, z_bottom)
        )
        # The audit CSV stores six decimals, so compare at that precision.
        np.testing.assert_allclose(rows["center_vs_km_s"], independent, atol=1e-6)

    para = {}
    for line in (
        (tmp_path / "grids" / point.folder_name / "para.inp")
        .read_text(encoding="utf-8")
        .splitlines()
    ):
        parts = line.split()
        if len(parts) == 4 and parts[0] in {"1", "2"}:
            para[(parts[0], int(parts[1]))] = (float(parts[2]), float(parts[3]))
    for section, family in (("crust", "1"), ("mantle", "2")):
        for _, row in audit[audit["section"] == section].iterrows():
            assert para[(family, int(row["coefficient"]))] == (
                pytest.approx(row["lower_km_s"]),
                pytest.approx(row["upper_km_s"]),
            )


def test_grid_layer_thresholds_and_sediment_priority_reach_para(
    tmp_path, write_mcmc_config
):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["sediment_vs"] = [[0.3, 1.0], [1.0, 1.8], [1.8, 2.5]]
    raw["sm_on"] = 0
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    cfg = load_config(config_path)
    phase = DispersionCurve(
        np.arange(10.0, 60.0, 10.0), np.full(5, 3.5), np.full(5, 0.03)
    )

    for water, sediment, expected in [
        (0.0, 0.0, (0, 0, 0.0)),
        (1.0, 2.0, (0, 0, 0.0)),
        (1.001, 2.0, (1, 0, 1.001)),
        (1.0, 2.001, (0, 1, 2.001)),
        (3.0, 5.0, (0, 1, 5.0)),
    ]:
        profile = VsProfile(
            0.0, 0.0, np.array([0.0, 40.0, 300.0]), np.array([3.0, 4.1, 4.7])
        )
        point = build_inversion_point(
            0.0, 0.0, -water * 1000, sediment, 40.0, profile, cfg
        )
        water_on, sediment_on, spline_top = expected
        assert (point.water_on, point.sediment_on, point.crustal_spline_top) == expected
        assert minimum_vs(point, cfg.vs_constraints) == (
            0.0 if water_on or sediment_on else 0.5
        )

        base = tmp_path / f"grids_{water}_{sediment}"
        assert _write(cfg, point, phase, base)[0]
        lines = (base / point.folder_name / "para.inp").read_text().splitlines()
        assert lines[:3] == ["0", "0", str(water_on)]
        index = 3
        if water_on:
            assert float(lines[index]) == pytest.approx(water)
            index += 1
        assert lines[index] == str(sediment_on)
        assert lines[index + 5] == (
            cfg.reference_water_model if water_on else cfg.reference_model
        )
        assert sum(line.startswith("10 ") for line in lines) == (
            3 if sediment_on else 0
        )
        assert sum(line.startswith("0 0 ") for line in lines) == sediment_on
        if sediment_on:
            assert lines[index + 6] == f"0 0 {sediment - 0.2:.2f} {sediment + 0.2:.2f}"


def test_skipped_point_creates_no_folder_or_plot(
    tmp_path, write_mcmc_config, make_profile
):
    cfg = load_config(write_mcmc_config())
    point = build_inversion_point(0.0, 0.0, 1000.0, 0.0, 40.0, make_profile(), cfg)
    phase = DispersionCurve(
        periods=np.array([5.0, 10.0]),
        velocities=np.array([3.0, 3.2]),
        sigmas=np.array([0.03, 0.03]),
    )
    base = tmp_path / "grids"
    assert _write(cfg, point, phase, base, plot=True)[0] is False
    assert not (base / point.folder_name).exists()


# =========================
# VS BOUND ALGORITHM (unit)
# =========================


def test_moho_midpoint_repair_keeps_search_space():
    def bound(section, limits):
        center = sum(limits) / 2
        return PriorBound(
            section=section,
            coefficient=1,
            greville_depth=40.0,
            basis_centroid=40.0,
            reference_vs_at_centroid=center,
            projection_vs=center,
            center_vs=center,
            search_radius=0.3,
            lower=limits[0],
            upper=limits[1],
        )

    for crust_range, mantle_range, minimum in [
        ((3.6, 4.2), (3.8, 4.2), 0.5),
        ((3.7, 4.3), (3.8, 4.2), 0.5),
        ((4.3, 4.9), (3.6, 4.0), 0.5),
        ((4.5, 4.9), (4.5, 4.9), 0.5),
        ((0.5, 4.9), (0.5, 4.9), 0.5),
        ((0.0, 0.4), (0.0, 0.4), 0.0),
    ]:
        crust = (bound("crust", crust_range),)
        mantle = (bound("mantle", mantle_range),)
        new_crust, new_mantle = _repair_moho_jump(minimum, _settings(), crust, mantle)
        c, m = new_crust[-1], new_mantle[0]
        # Test what the executable actually reads, including real(4) endpoints.
        cm = np.float32((float(f"{c.lower:.3f}") + float(f"{c.upper:.3f}")) / 2)
        mm = np.float32((float(f"{m.lower:.3f}") + float(f"{m.upper:.3f}")) / 2)
        assert mm > cm
        assert float(mm - cm) >= 0.00099
        assert c.lower <= m.upper and m.lower <= c.upper
        for output, original in [(c, crust_range), (m, mantle_range)]:
            assert minimum <= output.lower < output.upper <= 4.9
            assert output.upper - output.lower <= original[1] - original[0] + 1e-12
            assert output.reference_vs_at_centroid == sum(original) / 2
        if sum(mantle_range) - sum(crust_range) >= 0.002:
            assert (c.lower, c.upper) == crust_range
            assert (m.lower, m.upper) == mantle_range


def _mantle_layer(half_width: float = 0.2) -> LayerSpec:
    return LayerSpec(
        section="mantle",
        z_top=40.0,
        z_bottom=300.0,
        factor=2.0,
        n_coeff=5,
        n_nodes=20,
        half_widths=np.full(5, half_width),
    )


def test_model_space_window_keeps_interior_coefficients_past_the_limit():
    raw = np.array([4.342, 4.928, 3.545, 4.832, 4.565])
    layer = _mantle_layer()
    matrix = node_matrix(5, 40.0, 300.0, 2.0, 20)

    clipped, lower, upper, _, upper_scale = _model_space_window(
        layer, raw, minimum=0.5, deepest_min=4.0, model_max=4.9
    )
    # An out-of-domain projection is clipped into the admissible domain ...
    assert clipped[1] == pytest.approx(4.9)
    # ... while an interior window may still pass the model limit ...
    assert upper_scale > 0.0
    assert (upper > 4.9).any()
    # ... as long as the whole reconstructed box stays inside it.
    assert float((matrix @ upper).max()) <= 4.9 + 1e-12
    assert float((matrix @ lower).min()) >= 0.5 - 1e-12


def test_model_space_window_scales_an_infeasible_box():
    centers = np.array([4.739, 4.817, 2.957, 5.448, 4.587])
    layer = _mantle_layer()
    matrix = node_matrix(5, 40.0, 300.0, 2.0, 20)

    _, lower, upper, _, upper_scale = _model_space_window(
        layer, centers, minimum=0.5, deepest_min=4.0, model_max=4.9
    )
    assert upper_scale < 1.0
    assert float((matrix @ upper).max()) <= 4.9 + 1e-12
    assert float((matrix @ lower).min()) >= 0.5 - 1e-12


def test_sediment_intervals_may_overlap_when_centres_increase(write_mcmc_config):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["sediment_vs"] = [[0.0, 2.5], [0.5, 3.0]]
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    cfg = load_config(config_path)
    settings = PriorSettings.from_config(cfg)
    np.testing.assert_allclose(_sediment_bounds(settings), [[0.0, 2.5], [0.5, 3.0]])


def test_sediment_bounds_reject_vanishing_ordering_at_output_precision():
    # Raw centres differ, but both collapse to the same 3-decimal tick.
    settings = _settings(sediment_intervals=((1.0004, 1.2004), (1.0006, 1.2006)))
    with pytest.raises(ValueError, match="strictly increasing at"):
        _sediment_bounds(settings)


def test_compute_point_bounds_is_repeatable(tmp_path, write_mcmc_config, make_profile):
    cfg = load_config(write_mcmc_config())
    point = build_inversion_point(0.0, 0.0, -2000.0, 3.0, 40.0, make_profile(), cfg)
    settings = PriorSettings.from_config(cfg)
    first = compute_point_bounds(point, settings)
    second = compute_point_bounds(point, settings)
    assert first == second
    # Inputs are immutable: repairing the Moho jump must not mutate callers.
    assert isinstance(first.crust, tuple) and isinstance(first.mantle, tuple)


# =========================
# MOHO CONTRAST PRIOR
# =========================


def _interface_config(write_mcmc_config, jump: float):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw.setdefault("vs_constraints", {})["moho_vs_jump"] = jump
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    return load_config(config_path)


def _interface_point(cfg, make_profile):
    return build_inversion_point(0.0, 0.0, 0.0, 0.0, 40.0, make_profile(), cfg)


def test_projection_centers_do_not_invent_a_moho_jump(write_mcmc_config, make_profile):
    cfg = _interface_config(write_mcmc_config, 0.0)
    bounds = compute_point_bounds(
        _interface_point(cfg, make_profile), PriorSettings.from_config(cfg)
    )
    # make_profile is continuous across the Moho (4.2 km/s on both sides), so
    # the projection centres differ only by the strict-margin repair.
    assert abs(bounds.crust[-1].center_vs - bounds.mantle[0].center_vs) < 0.01
    assert bounds.moho_contrast < 0.01


def test_moho_vs_jump_is_deprecated_and_ignored(write_mcmc_config, make_profile):
    zero = _interface_config(write_mcmc_config, 0.0)
    with pytest.warns(DeprecationWarning, match="moho_vs_jump"):
        jumped = _interface_config(write_mcmc_config, 0.3)
    b_zero = compute_point_bounds(
        _interface_point(zero, make_profile), PriorSettings.from_config(zero)
    )
    b_jump = compute_point_bounds(
        _interface_point(jumped, make_profile), PriorSettings.from_config(jumped)
    )
    assert b_zero.crust == b_jump.crust
    assert b_zero.mantle == b_jump.mantle


def test_greville_reference_centers_is_deprecated(write_mcmc_config, make_profile):
    cfg = load_config(write_mcmc_config())
    settings = PriorSettings.from_config(cfg)
    point = _interface_point(cfg, make_profile)
    with pytest.warns(DeprecationWarning, match="greville_reference_centers"):
        depths, centers = greville_reference_centers(
            point,
            settings,
            z_top=point.crustal_spline_top,
            z_bottom=point.moho_depth,
            section="crust",
        )
    assert depths.shape == centers.shape == (4,)


def test_projection_centers_represent_a_sharp_moho_reference(write_mcmc_config):
    cfg = load_config(write_mcmc_config())
    depths = np.array([0, 5, 10, 20, 34.9, 35.0, 35.1, 60, 80, 120, 200, 300])
    vs = np.array([3.0, 3.3, 3.5, 3.6, 3.80, 3.80, 4.47, 4.49, 4.5, 4.5, 4.5, 4.6])
    profile = VsProfile(0.0, 0.0, depths, vs)
    point = build_inversion_point(0.0, 0.0, 0.0, 0.0, 35.0, profile, cfg)
    bounds = compute_point_bounds(point, PriorSettings.from_config(cfg))

    # The mantle-side reference (~4.47) must be inside the first mantle window.
    # The old Greville centre was ~3.84, drawing a window near [3.64, 4.04] that
    # excluded it.
    assert bounds.mantle[0].lower <= 4.47 <= bounds.mantle[0].upper
    assert bounds.crust[-1].center_vs < bounds.mantle[0].center_vs
    assert bounds.moho_contrast > 0.5

    model_depth, model_vs = reconstruct_initial_model(point, bounds)
    assert float(np.interp(40.0, model_depth, model_vs)) > 4.0


def test_compute_point_bounds_enforces_the_model_upper_limit(write_mcmc_config):
    cfg = load_config(write_mcmc_config())
    depths = np.array([0, 5, 20, 40, 41, 60, 80, 110, 150, 200, 300])
    vs = np.array([3.0, 3.3, 3.6, 3.8, 4.7, 4.75, 4.6, 4.1, 3.9, 4.6, 4.8])
    profile = VsProfile(0.0, 0.0, depths, vs)
    point = build_inversion_point(0.0, 0.0, 0.0, 0.0, 40.0, profile, cfg)
    settings = PriorSettings.from_config(cfg)
    bounds = compute_point_bounds(point, settings)

    matrix = node_matrix(
        len(bounds.mantle),
        point.moho_depth,
        point.max_depth,
        settings.factor,
        settings.npts_mantle,
    )
    upper = np.array([b.upper for b in bounds.mantle])
    lower = np.array([b.lower for b in bounds.mantle])
    assert float((matrix @ upper).max()) <= 4.9 + 1e-9
    assert float((matrix @ lower).min()) >= 0.5 - 1e-9
    # The low-velocity zone pushes an interior projection past the model limit;
    # that centre is clipped into the domain, while an interior window may still
    # pass 4.9 as long as the reconstructed box stays inside it.
    assert (upper > 4.9).any()
    assert max(b.center_vs for b in bounds.mantle) <= 4.9 + 1e-9


def test_prior_bounds_csv_records_basis_diagnostics(
    tmp_path, write_mcmc_config, make_profile
):
    cfg = _interface_config(write_mcmc_config, 0.0)
    point = _interface_point(cfg, make_profile)
    phase = DispersionCurve(
        np.arange(10.0, 60.0, 10.0), np.full(5, 3.5), np.full(5, 0.03)
    )
    written, bounds = _write(cfg, point, phase, tmp_path / "grids")
    assert written

    audit = pd.read_csv(tmp_path / "grids" / point.folder_name / "prior_bounds.csv")
    assert {
        "greville_depth_km",
        "basis_centroid_km",
        "projection_vs_km_s",
        "center_vs_km_s",
        "reference_vs_at_centroid_km_s",
        "initial_midpoint_vs_km_s",
        "layer_projection_max_error_km_s",
        "window_scale",
        "moho_contrast_km_s",
    } <= set(audit.columns)

    # Only the two interface coefficients carry the realized Moho contrast.
    boundary = audit[audit["moho_contrast_km_s"].notna()]
    assert set(zip(boundary["section"], boundary["coefficient"], strict=True)) == {
        ("crust", 4),
        ("mantle", 1),
    }
    np.testing.assert_allclose(
        boundary["moho_contrast_km_s"], bounds.moho_contrast, atol=1e-9
    )

    # Basis mass fractions are normalized per layer.
    for section in ("crust", "mantle"):
        mass = audit.loc[audit["section"] == section, "basis_mass_fraction"]
        np.testing.assert_allclose(mass.sum(), 1.0, atol=1e-9)
    # Endpoint coefficients carry less of the layer than the interior ones.
    crust = audit[audit["section"] == "crust"]
    assert crust.iloc[0]["basis_mass_fraction"] < crust.iloc[1]["basis_mass_fraction"]
    assert crust.iloc[-1]["basis_mass_fraction"] < crust.iloc[-2]["basis_mass_fraction"]
    # The projection is accurate on this smooth reference.
    assert audit["layer_projection_max_error_km_s"].max() < 0.05
