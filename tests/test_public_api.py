import inspect
import os
import subprocess
import sys
from pathlib import Path


def test_top_level_import_is_lazy():
    root = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root / "src")
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, seispy; "
            "assert 'seispy.mcmc' not in sys.modules; "
            "assert 'seispy.waveform.decimation' not in sys.modules; "
            "assert not hasattr(seispy, 'stationxml'); "
            "assert not hasattr(seispy, 'response')",
        ],
        check=True,
        env=environment,
    )


def test_domain_packages_do_not_import_their_implementations_eagerly():
    root = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root / "src")
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, seispy.download, seispy.waveform, seispy.inventory, "
            "seispy.deconvolution, seispy.correct, seispy.mcmc; "
            "assert 'seispy.waveform.coverage' not in sys.modules; "
            "assert 'seispy.waveform.decimation' not in sys.modules; "
            "assert 'seispy.inventory.analysis' not in sys.modules; "
            "assert 'seispy.deconvolution.removal' not in sys.modules; "
            "assert 'seispy.correct.orientation' not in sys.modules; "
            "assert 'seispy.mcmc.workflow' not in sys.modules",
        ],
        check=True,
        env=environment,
    )


def test_domain_public_apis_use_canonical_names():
    from seispy import event
    from seispy import download
    from seispy import waveform

    assert callable(event.cut_event_waveforms)
    assert callable(event.cut_events_binary)
    assert callable(event.write_event_catalog)
    assert callable(download.mass_download_waveforms)
    assert download.BulkDownloadSummary.__name__ == "BulkDownloadSummary"
    assert callable(waveform.decimate_waveforms)
    assert callable(waveform.archive_waveforms)
    assert not hasattr(waveform, "sort_waveforms")
    assert callable(waveform.waveform_coverage)
    assert not hasattr(download, "download_status")
    assert not hasattr(waveform, "decimate_by_station")

    from seispy import mcmc

    assert callable(mcmc.init_grids)
    assert callable(mcmc.collect_results)
    assert not hasattr(mcmc, "prepare_inversion_grids")
    assert not hasattr(mcmc, "collect_inversion_results")


def test_waveform_interfaces_cannot_delete_or_replace_sources():
    from seispy import waveform

    for function in (
        waveform.archive_waveforms,
        waveform.convert_mseed_to_sac,
        waveform.decimate_waveforms,
        waveform.merge_waveforms_by_day,
    ):
        parameters = inspect.signature(function).parameters
        assert "remove_original" not in parameters
        assert "remove_src" not in parameters
