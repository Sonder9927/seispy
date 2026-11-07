"""Small collection contract: three input summaries, two output tables."""

import numpy as np
import pandas as pd
import pytest

from seispy.mcmc.collection import collect_results, read_grid_result
from seispy.mcmc.lab import LABSettings, estimate_lab


def _write_grid(path):
    path.mkdir()
    z = np.arange(301.0)
    vs = 4.6 - 0.15 * (1 + np.tanh((z - 110) / 8))
    np.savetxt(path / "mean_prob.lst", np.c_[z, vs])
    (path / "moho.lst").write_text("29\n31\n")
    (path / "Litmod_output.log").write_text("final result 0 0 0 0 0 1.25\n")
    return z, vs


def test_collection_products_and_failure_isolation(tmp_path, capsys, monkeypatch):
    grid = tmp_path / "1.50_2.50"
    z, vs = _write_grid(grid)
    # These files are intentionally invalid: the fast collector must ignore them.
    (grid / "fort.8010").write_text("not a chain")
    (grid / "para.inp").write_text("not model geometry")
    (grid / "probalCr.png").write_bytes(b"not an image")
    before = {p.name: p.read_bytes() for p in grid.iterdir()}
    (tmp_path / "2.00_3.00").mkdir()
    summary = collect_results(tmp_path, tmp_path / "out")
    assert capsys.readouterr().out == ""
    assert (summary.grid_count, summary.success_count, summary.failed_count) == (
        2,
        1,
        1,
    )
    assert summary.lab_nan_count == 0 and summary.lab_valid_count == 1
    assert list(summary.errors) == ["2.00_3.00"]
    profiles = pd.read_parquet(summary.output_paths["profiles"])
    interfaces = pd.read_parquet(summary.output_paths["interfaces"])
    assert list(profiles) == ["x", "y", "z", "vs"]
    assert list(interfaces) == ["x", "y", "misfit", "moho", "lab"]
    np.testing.assert_array_equal(profiles.z, z)
    np.testing.assert_array_equal(profiles.vs, read_grid_result(grid).profile.vs)
    assert interfaces.iloc[0].tolist() == [1.5, 2.5, 1.25, 30.0, 110.0]
    assert len(list((tmp_path / "out").iterdir())) == 2
    assert summary.probability_figures == 0
    assert "prob_figures" not in summary.output_paths
    assert before == {p.name: p.read_bytes() for p in grid.iterdir()}
    assert collect_results(tmp_path, tmp_path / "out").grid_count == 2
    _write_grid(tmp_path / "3.00_4.00")  # Missing figures are optional.
    copied = collect_results(tmp_path, tmp_path / "out", collect_prob_figures=True)
    figures = list(copied.output_paths["prob_figures"].iterdir())
    assert copied.probability_figures == len(figures) == 1
    assert figures[0].read_bytes() == before["probalCr.png"]
    again = collect_results(tmp_path, tmp_path / "out", collect_prob_figures=True)
    assert again.output_paths["prob_figures"] != copied.output_paths["prob_figures"]

    from seispy.mcmc import collection

    def fail_copy(*args, **kwargs):
        raise OSError("simulated figure copy failure")

    monkeypatch.setattr(collection.shutil, "copy2", fail_copy)
    failed_copy = collect_results(tmp_path, tmp_path / "out", collect_prob_figures=True)
    assert failed_copy.success_count == 2 and failed_copy.failed_count == 1
    assert failed_copy.probability_figures == 0
    assert (
        "simulated figure copy failure" in failed_copy.errors["1.50_2.50/probalCr.png"]
    )
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "problem", ["missing", "empty", "negative_depth", "write_failure"]
)
def test_collection_edge_cases(tmp_path, monkeypatch, problem):
    out = tmp_path / "out"
    if problem == "missing":
        with pytest.raises(FileNotFoundError):
            collect_results(tmp_path / "absent", out)
        assert not out.exists()
        return
    if problem == "empty":
        summary = collect_results(tmp_path, out)
        assert (
            summary.grid_count == summary.lab_nan_count == summary.lab_valid_count == 0
        )
        assert pd.read_parquet(summary.output_paths["profiles"]).empty
        return
    grid = tmp_path / "0.00_0.00"
    _write_grid(grid)
    if problem == "negative_depth":
        (grid / "mean_prob.lst").write_text("0 3\n-10 4\n")
        with pytest.raises(ValueError, match="positive-downward"):
            read_grid_result(grid)
        summary = collect_results(tmp_path, out)
        assert summary.failed_count == 1 and summary.success_count == 0
        assert summary.lab_nan_count == 0  # Failed grids have no interface row.
        return
    summary = collect_results(tmp_path, out)
    before = {name: p.read_bytes() for name, p in summary.output_paths.items()}
    write = pd.DataFrame.to_parquet

    def fail_second(self, path, **kwargs):
        if path.name == "interfaces.parquet":
            raise OSError("simulated output failure")
        return write(self, path, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "to_parquet", fail_second)
    with pytest.raises(OSError, match="simulated output failure"):
        collect_results(tmp_path, out)
    assert before == {name: p.read_bytes() for name, p in summary.output_paths.items()}
    assert not list(out.glob(".collect-*"))


@pytest.mark.parametrize(
    "kind,expected",
    [
        ("single", 110.0),
        ("weak", 110.0),
        ("below_threshold", None),
        ("flat", None),
        ("increasing", None),
        ("ramp", None),
        ("double", None),
        ("outside", None),
        ("endpoint", None),
        ("short", None),
    ],
)
def test_mean_profile_lab(kind, expected, tmp_path):
    z = np.arange(301.0)

    def step(center, drop):
        return drop / 2 * (1 + np.tanh((z - center) / 8))

    values = {
        "single": 4.6 - step(110, 0.3),
        "weak": 4.6 - step(110, 0.018),
        "below_threshold": 4.6 - step(110, 0.01),
        "flat": np.full(z.shape, 4.6),
        "increasing": 4.6 + 0.001 * z,
        "ramp": 4.6 - 0.001 * z,
        "double": 4.8 - step(110, 0.3) - step(180, 0.3),
        "outside": 4.6 - step(270, 0.3),
        "endpoint": 4.6 - step(298, 0.3),
        "short": 4.6 - step(110, 0.3),
    }
    moho = 299 if kind == "short" else 30
    lab = estimate_lab(z, values[kind], moho, LABSettings())
    if expected is None:
        assert np.isnan(lab)
    else:
        assert lab == pytest.approx(expected, abs=1)
    grid = tmp_path / "0.00_0.00"
    _write_grid(grid)
    np.savetxt(grid / "mean_prob.lst", np.c_[z, values[kind]])
    (grid / "moho.lst").write_text(str(moho))
    summary = collect_results(tmp_path, tmp_path / "out")
    assert summary.success_count == 1
    assert summary.lab_nan_count == int(expected is None)
    assert summary.lab_valid_count == int(expected is not None)
