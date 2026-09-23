from pathlib import Path

import numpy as np
import pytest

pytest.importorskip("pandas")


def _write_grid(path: Path, *, profile="0 3.0\n50 4.0\n100 3.8\n", moho="40\n42\n"):
    path.mkdir()
    (path / "mean_prob.lst").write_text(profile, encoding="utf-8")
    (path / "Litmod_output.log").write_text(
        "final result 0 0 0 0 0 1.25\n", encoding="utf-8"
    )
    (path / "moho.lst").write_text(moho, encoding="utf-8")


def test_collect_results_returns_summary_and_skips_failed_point(tmp_path: Path):
    from seispy.mcmc.collection import collect_results

    _write_grid(tmp_path / "0.00_1.00")
    (tmp_path / "0.00_1.00" / "probalCr.png").write_bytes(b"not a real image")

    (tmp_path / "1.00_1.00").mkdir()

    summary = collect_results(tmp_path, tmp_path / "summary")

    assert summary.grid_count == 2
    assert summary.success_count == 1
    assert summary.failed_count == 1
    assert summary.velocity_rows == 3
    assert summary.probability_figures == 1
    assert (
        summary.diagnostics.loc[
            summary.diagnostics["grid"] == "1.00_1.00", "status"
        ].item()
        == "failed"
    )
    assert (tmp_path / "summary" / "vs.csv").is_file()
    assert (tmp_path / "summary" / "misfit_moho_lab.csv").is_file()
    assert (
        tmp_path / "summary" / "mcmc_prob_figs" / "prob_fig_0.00_1.00.png"
    ).is_file()


def test_collect_results_only_saves_diagnostics_when_requested(tmp_path: Path):
    from seispy.mcmc.collection import collect_results

    _write_grid(tmp_path / "0.00_1.00")

    diagnostics = tmp_path / "diagnostics.csv"
    collect_results(tmp_path, tmp_path / "summary", diagnostics_path=diagnostics)
    assert diagnostics.is_file()


def test_collect_results_reports_missing_grid_directory(tmp_path: Path):
    from seispy.mcmc.collection import collect_results

    with pytest.raises(FileNotFoundError, match="grids directory not found"):
        collect_results(tmp_path / "absent", tmp_path / "summary")


def test_read_grid_result_parses_profile_misfit_and_moho(tmp_path: Path):
    from seispy.mcmc.collection import read_grid_result

    _write_grid(tmp_path / "1.50_2.50", profile="0 3.0\n50 4.0\n100 3.8\n")
    result = read_grid_result(tmp_path / "1.50_2.50")

    assert (result.lon, result.lat) == (1.5, 2.5)
    assert result.misfit == 1.25
    assert result.moho == 41.0
    assert result.summary_row() == [1.5, 2.5, 1.25, 41.0, -100.0]
    assert list(result.profile.columns) == ["z", "vs", "x", "y"]
    np.testing.assert_allclose(result.profile["z"], [0.0, -50.0, -100.0])


def test_read_grid_result_reports_incomplete_and_malformed_directories(tmp_path: Path):
    from seispy.mcmc.collection import read_grid_result

    empty = tmp_path / "0.00_0.00"
    empty.mkdir()
    with pytest.raises(FileNotFoundError, match="Incomplete MCMC output"):
        read_grid_result(empty)

    bad_name = tmp_path / "not-a-coordinate"
    _write_grid(bad_name)
    with pytest.raises(ValueError, match="Invalid MCMC output directory name"):
        read_grid_result(bad_name)


def test_landing_depth_flags_short_profiles(tmp_path: Path):
    from seispy.mcmc.collection import read_grid_result

    _write_grid(tmp_path / "0.00_0.00", profile="0 3.0\n10 3.1\n")
    result = read_grid_result(tmp_path / "0.00_0.00")
    assert result.lab == -999.0
