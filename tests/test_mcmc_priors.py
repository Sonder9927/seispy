import json

import numpy as np
import pandas as pd
import pytest

from seispy.mcmc.bspline import (
    basis_geometry,
    basis_matrix,
    fortran_knot_vector,
    greville_depths,
)
from seispy.mcmc.config import SearchRadius, VsConstraints, load_config
from seispy.mcmc.dispersion import DispersionCurve
from seispy.mcmc.inversion import build_inversion_point
from seispy.mcmc.priors import (
    PriorBound,
    PriorSettings,
    _apply_vs_limits,
    _constrain_deepest,
    _repair_moho_jump,
    _section_limits,
    _sediment_bounds,
    compute_point_bounds,
    minimum_vs,
)
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
    written = FortranInputWriter(base, cfg).write_point(point, phase, bounds, plot=plot)
    return written, bounds


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

    written, _ = _write(cfg, point, phase, tmp_path / "grids")
    assert written
    bounds = pd.read_csv(tmp_path / "grids" / "0.00_0.00" / "prior_bounds.csv")
    crust = bounds[bounds["section"] == "crust"]
    mantle = bounds[bounds["section"] == "mantle"]
    assert crust.iloc[0]["shallow_extrapolated"] == 1
    assert crust.iloc[1:]["shallow_extrapolated"].eq(0).all()
    assert mantle["shallow_extrapolated"].eq(0).all()
    assert bounds["upper_km_s"].le(4.9).all()
    assert bounds["lower_km_s"].ge(0.5).all()
    assert mantle.iloc[-1]["lower_km_s"] >= 4.0
    assert (
        mantle.iloc[0]["effective_center_vs_km_s"]
        > crust.iloc[-1]["effective_center_vs_km_s"]
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
    "half_widths,centers,expected_ranges",
    [
        (None, (3.9, 4.0), ((3.600, 4.200), (3.800, 4.200))),
        (None, (3.9, 4.95), ((3.600, 4.200), (4.750, 4.900))),
        (None, (4.1, 4.5), ((3.800, 4.300), (4.300, 4.700))),
        (None, (4.4, 4.5), ((4.100, 4.300), (4.300, 4.700))),
        (
            {"crust_vs": 0.1, "mantle_vs": 0.1},
            (3.9, 4.0),
            ((3.800, 4.000), (3.900, 4.100)),
        ),
        (None, (4.85, 4.85), ((3.700, 4.300), (4.650, 4.900))),
    ],
)
def test_configured_half_widths_reach_para_and_preserve_reference_centers(
    tmp_path, write_mcmc_config, half_widths, centers, expected_ranges
):
    config_path = write_mcmc_config()
    raw = json.loads(config_path.read_text(encoding="utf-8"))
    raw["search_radius"] = {"sediment": 0.2, "moho": 1.0, **(half_widths or {})}
    config_path.write_text(json.dumps(raw), encoding="utf-8")
    cfg = load_config(config_path)
    if half_widths is None:
        assert cfg.search_radius.crust_vs == 0.30
        assert cfg.search_radius.mantle_vs == 0.20

    depths = np.concatenate(
        (
            [0.0],
            greville_depths(4, 0.0, 40.0, 2.0),
            greville_depths(5, 40.0, 300.0, 2.0),
            [300.0],
        )
    )
    profile = VsProfile(
        lon=0.0,
        lat=0.0,
        depth=depths,
        vs=np.array([3.0, 3.0, 3.3, 3.6, *centers, 4.4, 4.5, 4.6, 4.7, 4.7]),
    )
    point = build_inversion_point(0.0, 0.0, 0.0, 0.0, 40.0, profile, cfg)
    phase = DispersionCurve(
        np.arange(10.0, 60.0, 10.0), np.full(5, 3.5), np.full(5, 0.03)
    )
    assert _write(cfg, point, phase, tmp_path / "grids")[0]

    output = tmp_path / "grids" / point.folder_name
    rows = {
        tuple(parts[:2]): tuple(map(float, parts[2:]))
        for line in (output / "para.inp").read_text(encoding="utf-8").splitlines()
        if len(parts := line.split()) == 4
    }
    audit = pd.read_csv(output / "prior_bounds.csv")
    for key, section, coefficient, reference, expected in zip(
        [("1", "4"), ("2", "1")],
        ["crust", "mantle"],
        [4, 1],
        centers,
        expected_ranges,
        strict=True,
    ):
        np.testing.assert_allclose(rows[key], expected)
        bound = audit[
            (audit["section"] == section) & (audit["coefficient"] == coefficient)
        ].iloc[0]
        assert bound["reference_vs_km_s"] == pytest.approx(reference)
        assert bound["effective_center_vs_km_s"] == pytest.approx(sum(expected) / 2)
        np.testing.assert_allclose(
            [bound["lower_km_s"], bound["upper_km_s"]], rows[key]
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
        return PriorBound(section, 1, 40.0, center, center, 0.3, *limits)

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
            assert output.reference_vs == sum(original) / 2
        if sum(mantle_range) - sum(crust_range) >= 0.002:
            assert (c.lower, c.upper) == crust_range
            assert (m.lower, m.upper) == mantle_range


@pytest.mark.parametrize(
    "center,radius,minimum,expected",
    [
        (4.8, 0.2, 0.5, (4.6, 4.9)),
        (4.9, 0.2, 0.5, (4.7, 4.9)),
        (4.95, 0.2, 0.5, (4.75, 4.9)),
        (5.1, 0.2, 0.5, (4.9, 4.9)),
        (5.2, 0.2, 0.5, (4.5, 4.9)),
        (0.6, 0.3, 0.5, (0.5, 0.9)),
        (0.2, 0.3, 0.5, (0.5, 0.5)),
        (0.4, 0.3, 0.5, (0.5, 0.7)),
        (0.1, 0.3, 0.5, (0.5, 1.1)),
        (0.2, 0.1, 0.0, (0.1, 0.3)),
        (4.1234, 0.2, 0.5, (3.924, 4.323)),
        (4.7, 0.2, 4.9, (4.9, 4.9)),
    ],
)
def test_vs_windows_respect_inclusive_limits_and_output_precision(
    center, radius, minimum, expected
):
    low, high = _apply_vs_limits(
        np.array([center]),
        np.array([radius]),
        "mantle",
        VsConstraints(),
        minimum=minimum,
        decimals=3,
    )
    np.testing.assert_allclose([low[0], high[0]], expected)


@pytest.mark.parametrize(
    "center,expected",
    [
        (4.1, (4.0, 4.3)),
        (3.7, (4.0, 4.4)),
        (3.9, (4.0, 4.1)),
        (3.8, (4.0, 4.0)),
        (4.95, (4.75, 4.9)),
        (4.0, (4.0, 4.2)),
        (4.9, (4.7, 4.9)),
    ],
)
def test_deepest_window_does_not_unnecessarily_raise_upper_bound(center, expected):
    settings = _settings()
    low, high = _apply_vs_limits(
        np.array([center]),
        np.array([0.2]),
        "mantle",
        settings.vs_constraints,
        minimum=0.5,
        decimals=3,
    )
    bound = PriorBound(
        "mantle", 5, 300.0, center, (low[0] + high[0]) / 2, 0.2, low[0], high[0]
    )
    adjusted = _constrain_deepest((bound,), settings)[0]
    np.testing.assert_allclose([adjusted.lower, adjusted.upper], expected)
    assert adjusted.reference_vs == center


@pytest.mark.parametrize(
    "cap,global_cap,expected",
    [
        (4.3, 4.9, (3.8, 4.3)),
        (4.0, 4.9, (3.8, 4.0)),
        (4.9, 4.9, (3.8, 4.4)),
        (4.3, 4.2, (3.8, 4.2)),
    ],
)
def test_crust_cap_is_configurable_and_respects_global_cap(cap, global_cap, expected):
    constraints = VsConstraints(crust_vs_max=cap, global_vs_max=global_cap)
    low, high = _apply_vs_limits(
        np.array([4.1]),
        np.array([0.3]),
        "crust",
        constraints,
        minimum=0.5,
        decimals=3,
    )
    np.testing.assert_allclose([low[0], high[0]], expected)
    for section in ("mantle", "sediment"):
        assert _section_limits(section, constraints)[1] == global_cap


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


def test_moho_vs_jump_separates_the_interface_coefficients(
    write_mcmc_config, make_profile
):
    cfg = _interface_config(write_mcmc_config, 0.3)
    bounds = compute_point_bounds(
        _interface_point(cfg, make_profile), PriorSettings.from_config(cfg)
    )

    # Reference=4.2 at the Moho, so the pair is centred on 4.05 / 4.35.
    assert bounds.crust[-1].reference_vs == pytest.approx(4.05, abs=1e-9)
    assert bounds.mantle[0].reference_vs == pytest.approx(4.35, abs=1e-9)
    assert bounds.moho_contrast == pytest.approx(0.3, abs=1e-6)
    # The two windows are now genuinely distinct rather than near-duplicates.
    assert bounds.mantle[0].lower > bounds.crust[-1].lower


def test_zero_moho_vs_jump_keeps_the_shared_interface_centre(
    write_mcmc_config, make_profile
):
    cfg = _interface_config(write_mcmc_config, 0.0)
    bounds = compute_point_bounds(
        _interface_point(cfg, make_profile), PriorSettings.from_config(cfg)
    )

    # Both coefficients still sample the continuous reference at the Moho, so
    # only the numerical strict-jump margin separates their midpoints.
    assert abs(bounds.mantle[0].reference_vs - bounds.crust[-1].reference_vs) < 0.01
    assert bounds.moho_contrast < 0.01


def test_prior_bounds_csv_records_basis_diagnostics(
    tmp_path, write_mcmc_config, make_profile
):
    cfg = _interface_config(write_mcmc_config, 0.3)
    point = _interface_point(cfg, make_profile)
    phase = DispersionCurve(
        np.arange(10.0, 60.0, 10.0), np.full(5, 3.5), np.full(5, 0.03)
    )
    written, _ = _write(cfg, point, phase, tmp_path / "grids")
    assert written

    audit = pd.read_csv(tmp_path / "grids" / point.folder_name / "prior_bounds.csv")
    assert {"basis_centroid_km", "basis_mass_fraction", "moho_contrast_km_s"} <= set(
        audit.columns
    )

    # Only the two interface coefficients carry the realized Moho contrast.
    boundary = audit[audit["moho_contrast_km_s"].notna()]
    assert set(zip(boundary["section"], boundary["coefficient"], strict=True)) == {
        ("crust", 4),
        ("mantle", 1),
    }
    np.testing.assert_allclose(boundary["moho_contrast_km_s"], 0.3, atol=1e-6)

    # Basis mass fractions are normalized per layer.
    for section in ("crust", "mantle"):
        mass = audit.loc[audit["section"] == section, "basis_mass_fraction"]
        np.testing.assert_allclose(mass.sum(), 1.0, atol=1e-9)
    # Endpoint coefficients carry less of the layer than the interior ones.
    crust = audit[audit["section"] == "crust"]
    assert crust.iloc[0]["basis_mass_fraction"] < crust.iloc[1]["basis_mass_fraction"]
    assert crust.iloc[-1]["basis_mass_fraction"] < crust.iloc[-2]["basis_mass_fraction"]
