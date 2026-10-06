from pathlib import Path

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


def test_collection_preserves_values_is_silent_and_isolates_failures(tmp_path, capsys):
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
    row = pd.read_parquet(result.output_paths["interfaces"]).iloc[0]
    assert list(row[["x", "y", "misfit", "moho"]]) == [1.5, 2.5, 1.25, 45.0]
    assert row.moho_km_mean == 41.0  # 3*40 + 44, divided by 4
    assert row.retained_states == 4
    assert row.unique_states == 2
    assert set(result.diagnostics.status) == {"ok", "failed"}
    again = collect_results(tmp_path, tmp_path / "out")
    assert again.grid_count == 2
    assert not (tmp_path / "out" / "vs.csv").exists()


def test_missing_chain_keeps_original_data_but_marks_partial(tmp_path):
    _write_grid(tmp_path / "0.00_0.00")
    result = collect_results(tmp_path, tmp_path / "out")
    assert result.diagnostics.status.tolist() == ["partial"]
    row = pd.read_parquet(result.output_paths["interfaces"]).iloc[0]
    assert np.isnan(row.lab)
    assert row.posterior_status == "unavailable"


def test_chain_statistics_keep_rejected_state_multiplicities(tmp_path):
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


@pytest.mark.parametrize("problem", ["biased", "short", "forward"])
def test_invalid_chains_are_not_certified(tmp_path, problem):
    _chain_inputs(tmp_path)
    if problem == "biased":
        p = tmp_path / "input_DRAM_T.dat"
        p.write_text(p.read_text().replace("1.7d0", "0.0"))
    elif problem == "short":
        p = tmp_path / "fort.8010"
        p.write_text("\n".join(p.read_text().splitlines()[:-1]))
    else:
        (tmp_path / "fort.8010").write_text("1 2 3\n")
    with pytest.raises(ValueError):
        collect_posterior(tmp_path, np.arange(100.0), LABSettings())


def test_reconstruction_matches_external_fortran_oracle():
    p = Path(__file__).parent / "data" / "mcmc_fortran"
    layout = ModelLayout.read(p / "para.inp")
    parameters = np.loadtxt(p / "bsp00001.dat")[:, 2]
    expected = np.loadtxt(p / "mod00001.dat")
    depth, fields, moho = layout.reconstruct(parameters)
    np.testing.assert_allclose(depth, expected[:, 0], atol=2e-4, rtol=0)
    np.testing.assert_allclose(fields["vsv"], expected[:, 1], atol=2e-6, rtol=0)
    assert moho == parameters[0]


def _step(z, center, width=8):
    return 0.15 * (1 + np.tanh((z - center) / width))


def test_lab_detects_single_transition_and_uses_positive_depth():
    z = np.arange(30.0, 301.0)
    result = identify_lab(z, 4.6 - _step(z, 130), 30.0, LABSettings())
    assert result["lab_status"] == "detected"
    assert result["lab_depth_km"] == pytest.approx(130, abs=2)
    assert result["lab_drop_km_s"] > 0.05


@pytest.mark.parametrize(
    "kind,status",
    [
        ("increasing", "not_detected"),
        ("flat", "not_detected"),
        ("double", "ambiguous"),
        ("edge", "boundary_limited"),
    ],
)
def test_lab_does_not_force_a_unique_interface(kind, status):
    z = np.arange(30.0, 301.0)
    curves = {
        "increasing": 4 + 0.001 * z,
        "flat": np.full(z.shape, 4.5),
        "double": 4.8 - _step(z, 110) - _step(z, 185),
        "edge": 4.6 - _step(z, 245),
    }
    result = identify_lab(z, curves[kind], 30.0, LABSettings())
    assert result["lab_status"] == status
    assert np.isnan(result["lab_depth_km"])


def test_ess_distinguishes_correlated_and_independent_chains():
    rng = np.random.default_rng(41)
    independent = rng.normal(size=4000)
    correlated = np.zeros(4000)
    for i in range(1, 4000):
        correlated[i] = 0.95 * correlated[i - 1] + independent[i]
    assert effective_sample_size(correlated) < effective_sample_size(independent) / 5
    assert np.isnan(effective_sample_size(np.ones(100)))


def test_empty_and_missing_directories(tmp_path):
    with pytest.raises(FileNotFoundError):
        collect_results(tmp_path / "absent", tmp_path / "out")
    result = collect_results(tmp_path, tmp_path / "out")
    assert result.grid_count == 0
    assert pd.read_parquet(result.output_paths["profiles"]).empty


def test_failed_posterior_write_does_not_publish_lab(tmp_path, monkeypatch):
    p = tmp_path / "0.00_0.00"
    _write_grid(p)
    _chain_inputs(p)
    write = pd.DataFrame.to_parquet

    def fail_samples(self, path, **kwargs):
        if Path(path).name == "samples.parquet":
            raise OSError("simulated write failure")
        return write(self, path, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_samples)
    summary = collect_results(tmp_path, tmp_path / "out")
    row = pd.read_parquet(summary.output_paths["interfaces"]).iloc[0]
    assert row.posterior_status == "unavailable"
    assert np.isnan(row.lab)
    assert summary.partial_count == 1


@pytest.mark.parametrize("mode", ["water", "sediment", "joint", "sh"])
def test_reconstruction_layer_and_component_modes(tmp_path, mode):
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
