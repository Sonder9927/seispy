from pathlib import Path
import json

import numpy as np
import pandas as pd
import pytest

from seispy.mcmc.collection import collect_results, read_grid_result
from seispy.mcmc.lab import LABSettings, identify_lab
from seispy.mcmc.posterior import ModelLayout, collect_posterior, effective_sample_size


def _write_grid(path, *, moho="40\n42\n"):
    path.mkdir()
    (path / "mean_prob.lst").write_text("0 3.0\n50 4.0\n100 3.8\n")
    (path / "Litmod_output.log").write_text("final result 0 0 0 0 0 1.25\n")
    (path / "moho.lst").write_text(moho)


def _chain_inputs(path):
    (path / "para.inp").write_text(
        "0\n0\n0\n0\n2\n300\n21\n41\nunused-reference\n"
        "0 1 30 50\n1 1 3 4\n1 2 3 4\n1 3 3 4\n"
        "2 1 4 5\n2 2 4 5\n2 3 4 5\n"
    )
    (path / "input_DRAM_T.dat").write_text("nsimu | burn_in | biasfac\n5 2 1.7d0\n")
    # The three repeats must retain three quarters of the posterior mass.
    a = [1.0, 40.0, 3.0, 3.5, 4.0, 4.6, 4.4, 4.5]
    b = [2.0, 44.0, 3.1, 3.6, 4.1, 4.7, 4.5, 4.6]
    with (path / "fort.8010").open("w") as stream:
        stream.write("Mean IAC: not calculated\nBest misfit in model 1\n")
        np.savetxt(stream, [a, a, a, b])


def test_collection_preserves_values_is_silent_and_isolates_failures(
    tmp_path, capsys, monkeypatch
):
    from seispy.mcmc import collection

    original = collection.collect_posterior

    def with_resolved_lab(*args, **kwargs):
        profile, summary, samples, covariance, chain = original(*args, **kwargs)
        summary.update(lab_estimate_km=102.4, lab_estimate_status="resolved")
        return profile, summary, samples, covariance, chain

    monkeypatch.setattr(collection, "collect_posterior", with_resolved_lab)
    p = tmp_path / "1.50_2.50"
    _write_grid(p, moho="40\n50\n")
    _chain_inputs(p)
    (tmp_path / "2.00_3.00").mkdir()
    before = read_grid_result(p)
    result = collect_results(tmp_path, tmp_path / "out")
    assert capsys.readouterr().out == ""
    assert (result.grid_count, result.success_count, result.failed_count) == (2, 1, 1)
    profiles = pd.read_parquet(result.output_paths["profiles"])
    pd.testing.assert_frame_equal(profiles[before.profile.columns], before.profile)
    np.testing.assert_allclose(profiles.z, [0, 50, 100])
    np.testing.assert_allclose(profiles.vs, [3.0, 4.0, 3.8])
    assert (
        json.loads(result.output_paths["metadata"].read_text())["depth_positive"]
        == "down"
    )
    row = pd.read_parquet(result.output_paths["interfaces"]).iloc[0]
    assert list(row[["x", "y", "misfit", "moho"]]) == [1.5, 2.5, 1.25, 45.0]
    assert row.moho_km_mean == 41.0  # 3*40 + 44, divided by 4
    assert row.lab == row.lab_estimate_km == 102.4
    assert row.retained_states == 4
    assert row.unique_states == 2
    assert set(result.diagnostics.status) == {"ok", "failed"}
    assert result.lab_valid_count == 1
    assert result.lab_nan_count == 0
    assert result.lab_status_counts == {"resolved": 1}
    again = collect_results(tmp_path, tmp_path / "out")
    assert again.grid_count == 2
    assert not (tmp_path / "out" / "vs.csv").exists()


@pytest.mark.parametrize(
    "problem",
    [
        "missing_chain",
        "negative_depth",
        "missing_directory",
        "empty_directory",
        "sample_write",
    ],
)
def test_collection_failures(tmp_path, monkeypatch, problem):
    if problem == "missing_directory":
        with pytest.raises(FileNotFoundError):
            collect_results(tmp_path / "absent", tmp_path / "out")
        return
    if problem == "empty_directory":
        result = collect_results(tmp_path, tmp_path / "out")
        assert result.grid_count == 0
        assert result.lab_valid_count == result.lab_nan_count == 0
        assert result.lab_status_counts == {}
        assert pd.read_parquet(result.output_paths["profiles"]).empty
        return
    grid = tmp_path / "0.00_0.00"
    _write_grid(grid)
    if problem == "negative_depth":
        (grid / "mean_prob.lst").write_text("0 3.0\n-50 4.0\n")
        with pytest.raises(ValueError, match="positive-downward"):
            read_grid_result(grid)
        return
    if problem == "sample_write":
        _chain_inputs(grid)
        write = pd.DataFrame.to_parquet

        def fail_samples(self, path, **kwargs):
            if Path(path).name == "samples.parquet":
                raise OSError("simulated write failure")
            return write(self, path, **kwargs)

        monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_samples)
    result = collect_results(tmp_path, tmp_path / "out")
    assert result.partial_count == 1
    assert result.lab_valid_count == 0
    assert result.lab_nan_count == 1
    assert result.lab_status_counts == {"posterior_unavailable": 1}
    assert result.diagnostics.status.tolist() == ["partial"]
    row = pd.read_parquet(result.output_paths["interfaces"]).iloc[0]
    assert row.posterior_status == "unavailable"
    assert np.isnan(row.lab)
    assert row.moho == 41.0
    np.testing.assert_allclose(
        pd.read_parquet(result.output_paths["profiles"]).vs, [3, 4, 3.8]
    )


@pytest.mark.parametrize("problem", [None, "biased", "short", "forward", "bad_token"])
def test_chain_records(tmp_path, problem):
    from seispy.mcmc.posterior import read_chain

    _chain_inputs(tmp_path)
    layout = ModelLayout.read(tmp_path / "para.inp")
    expected, burn = read_chain(tmp_path, layout)
    path = tmp_path / "fort.8010"
    lines = path.read_text().splitlines()
    tokens = " ".join(lines[2:]).split()
    path.write_text(
        "\n".join(lines[:2])
        + "\n"
        + "\n".join(
            " ".join(tokens[i : i + 5]).replace("e", "D")
            for i in range(0, len(tokens), 5)
        )
    )
    if problem is None:
        actual, actual_burn = read_chain(tmp_path, layout)
        np.testing.assert_array_equal(actual, expected)
        assert actual_burn == burn
        return
    if problem == "biased":
        config = tmp_path / "input_DRAM_T.dat"
        config.write_text(config.read_text().replace("1.7d0", "0.0"))
    elif problem == "short":
        path.write_text("\n".join(path.read_text().splitlines()[:-1]))
    elif problem == "forward":
        path.write_text("1 2 3\n")
    else:
        path.write_text(path.read_text() + "\nBAD_TOKEN\n")
    with pytest.raises(ValueError):
        read_chain(tmp_path, layout)


def test_chain_statistics_keep_rejected_state_multiplicities(tmp_path, monkeypatch):
    _chain_inputs(tmp_path)
    profile, summary, samples, covariance, chain = collect_posterior(
        tmp_path, np.array([0.0, 50.0, 100.0]), LABSettings()
    )
    assert samples.multiplicity.tolist() == [3, 1]
    assert summary["moho_km_mean"] == 41
    assert summary["moho_km_median"] == 40
    assert chain.iteration.tolist() == [2, 3, 4, 5]
    assert chain.sample_id.tolist() == [0, 0, 0, 1]
    moho_var = covariance.query(
        "parameter_i == '0:1' and parameter_j == '0:1'"
    ).covariance.item()
    assert moho_var == pytest.approx(np.var([40, 40, 40, 44], ddof=1))
    assert profile.vsv_mean.iloc[0] == pytest.approx(3.025)

    rng = np.random.default_rng(41)
    independent = rng.normal(size=4000)
    correlated = np.zeros(4000)
    for i in range(1, 4000):
        correlated[i] = 0.95 * correlated[i - 1] + independent[i]
    assert effective_sample_size(correlated) < effective_sample_size(independent) / 5
    assert np.isnan(effective_sample_size(np.ones(100)))

    from seispy.mcmc import posterior

    identify = posterior.identify_lab

    def controlled_detection(z, v, moho, settings, **kwargs):
        result = identify(z, v, moho, settings, **kwargs)
        result.update(
            lab_depth_km=100.0 if moho == 40 else np.nan,
            lab_status="detected" if moho == 40 else "not_detected",
        )
        return result

    monkeypatch.setattr(posterior, "identify_lab", controlled_detection)
    for fraction, status in [(0.7, "resolved"), (0.8, "low_support")]:
        _, row, *_ = collect_posterior(
            tmp_path,
            np.array([0.0, 50.0, 100.0]),
            LABSettings(min_detection_fraction=fraction),
        )
        assert row["lab_detected_fraction"] == 0.75
        assert row["lab_estimate_status"] == status


@pytest.mark.parametrize("mode", ["oracle", "water", "sediment", "joint", "sh"])
def test_reconstruction_layer_and_component_modes(tmp_path, mode):
    if mode == "oracle":
        p = Path(__file__).parent / "data" / "mcmc_fortran"
        layout = ModelLayout.read(p / "para.inp")
        parameters = np.loadtxt(p / "bsp00001.dat")[:, 2]
        expected = np.loadtxt(p / "mod00001.dat")
        depth, fields, moho = layout.reconstruct(parameters)
        np.testing.assert_allclose(depth, expected[:, 0], atol=2e-4, rtol=0)
        np.testing.assert_allclose(fields["vsv"], expected[:, 1], atol=2e-6, rtol=0)
        assert moho == parameters[0]

        return
    _chain_inputs(tmp_path)
    p = tmp_path / "para.inp"
    text = p.read_text()
    state = [40.0, 3.0, 3.5, 4.0, 4.6, 4.4, 4.5]
    if mode == "water":
        text = text.replace("0\n0\n0\n0\n", "0\n0\n1\n3\n0\n", 1)
    elif mode == "sediment":
        text = text.replace("0\n0\n0\n0\n", "0\n0\n0\n1\n", 1)
        text += "0 0 1 3\n10 1 1 2\n10 2 2 3\n"
        state += [2.0, 1.5, 2.5]
    elif mode == "joint":
        text += "3 1 0 0.1\n3 2 0 0.1\n3 3 0 0.1\n4 1 0 0.1\n4 2 0 0.1\n4 3 0 0.1\n"
        state += [0.03] * 6
    else:
        lines = text.splitlines()
        lines[10:] = [str(int(line[0]) + 2) + line[1:] for line in lines[10:]]
        text = "\n".join(lines)
    p.write_text(text)
    z, fields, moho = ModelLayout.read(p).reconstruct(state)
    assert np.all(np.diff(z) >= 0)
    assert z[-1] == 300
    if mode == "water":
        assert z[1] == 3
        assert fields["vsv"][0] == 0
    elif mode == "sediment":
        np.testing.assert_allclose(z[:3], [0, 2, 2 + (40 - 2) / 21])
        np.testing.assert_allclose(fields["vsv"][:2], [1.5, 2.5])
    elif mode == "joint":
        np.testing.assert_allclose(fields["vsv"], 0.99 * fields["v"])
        np.testing.assert_allclose(fields["vsh"], 1.02 * fields["v"])
        np.testing.assert_allclose(fields["xi"], (1.02 / 0.99) ** 2)
    else:
        assert "vsh" in fields and "vsv" not in fields


def _step(z, center, width=8):
    return 0.15 * (1 + np.tanh((z - center) / width))


@pytest.mark.parametrize(
    "kind,status,center,width_limited",
    [
        ("single", "detected", 130, False),
        ("wide", "detected", 90, True),
        ("boundary_and_interior", "detected", 125, False),
        ("increasing", "not_detected", None, False),
        ("flat", "not_detected", None, False),
        ("constant_descent", "not_detected", None, False),
        ("double", "ambiguous", None, False),
        ("edge", "boundary_limited", None, False),
    ],
)
def test_lab_bands(kind, status, center, width_limited):
    z = np.arange(30.0, 301.0)
    curves = {
        "single": 4.6 - _step(z, 130),
        "wide": 4.7 - _step(z, 90, width=60),
        "boundary_and_interior": 4.9 - _step(z, 125) - _step(z, 249),
        "increasing": 4 + 0.001 * z,
        "flat": np.full(z.shape, 4.5),
        "constant_descent": 4.8 - 0.001 * z,
        "double": 4.8 - _step(z, 110) - _step(z, 185),
        "edge": 4.6 - _step(z, 245),
    }
    result = identify_lab(z, curves[kind], 30.0, LABSettings())
    assert result["lab_status"] == status
    if center is None:
        assert np.isnan(result["lab_depth_km"])
        assert result["lab_rejection_reason"] != "none"
    else:
        assert result["lab_depth_km"] == pytest.approx(center, abs=3)
        assert result["lab_drop_km_s"] > 0.05
        assert result["lab_width_limited"] == width_limited


@pytest.mark.parametrize("resolution", [1.0, 7.0, 14.0])
def test_lab_resolution_and_noise(resolution):
    """Known weak/strong interfaces versus noisy profiles without an interface."""
    z = np.arange(30.0, 301.0)
    native = np.arange(30.0, 301.0, resolution)
    settings = LABSettings()
    for drop, width in [(0.03, 3), (0.03, 8), (0.03, 25), (0.1, 8), (0.3, 60)]:
        values = (
            4.6
            - 0.0003 * (native - 30)
            - drop / 2 * (1 + np.tanh((native - 130) / width))
        )
        result = identify_lab(
            z, np.interp(z, native, values), 30, settings, resolution_km=resolution
        )
        assert result["lab_status"] == "detected"
        assert abs(result["lab_depth_km"] - 130) <= max(3, resolution / 2)
        assert result["lab_smoothing_km"] >= resolution / 2
        if resolution == 14 and width == 3:
            assert not result["lab_width_resolved"]
    # A tiny smooth oscillation must not be promoted just because it has a peak.
    tiny = 4.6 - 0.0025 * (1 + np.tanh((native - 130) / 8))
    result = identify_lab(
        z, np.interp(z, native, tiny), 30, settings, resolution_km=resolution
    )
    assert result["lab_status"] == "not_detected"
    assert result["lab_rejection_reason"] == "weak_drop"
    assert np.isfinite(result["lab_candidate_depth_km"])
    rng = np.random.default_rng(71349)
    false_detections = 0
    for j in range(200):
        values = 4.6 - (0.0003 if j % 2 else 0) * (native - 30)
        values += rng.normal(0, 0.015, len(native))
        result = identify_lab(
            z, np.interp(z, native, values), 30, settings, resolution_km=resolution
        )
        false_detections += result["lab_status"] == "detected"
    assert false_detections <= 2  # <=1% on this fixed null ensemble; not a p value.


def test_single_lab_requires_support_and_no_separated_modes():
    from seispy.mcmc.lab import summarize_lab

    assert np.isnan(summarize_lab([100.0, np.nan], [49, 51])["lab_estimate_km"])
    assert (
        summarize_lab([100.0, 105.0, np.nan], [30, 30, 40])["lab_estimate_km"] == 102.5
    )
    r = summarize_lab([80.0, 150.0], [50, 50])
    assert r["lab_estimate_status"] == "multimodal"
    assert np.isnan(r["lab_estimate_km"])
    assert (
        summarize_lab([100.0, np.nan], [49, 51], min_detection_fraction=0.4)[
            "lab_estimate_km"
        ]
        == 100
    )
    assert (
        summarize_lab([100.0, np.nan], [60, 40], min_detection_fraction=0.7)[
            "lab_estimate_status"
        ]
        == "low_support"
    )
    for value in [0, 1.01, np.nan]:
        with pytest.raises(ValueError):
            LABSettings(min_detection_fraction=value)
        with pytest.raises(ValueError):
            summarize_lab([100.0], [1], min_detection_fraction=value)
