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
            "assert 'seispy.waveform.decimation' not in sys.modules",
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
            "seispy.correct, seispy.mcmc; "
            "assert 'seispy.download.availability' not in sys.modules; "
            "assert 'seispy.waveform.decimation' not in sys.modules; "
            "assert 'seispy.inventory.analysis' not in sys.modules; "
            "assert 'seispy.correct.orientation' not in sys.modules; "
            "assert 'seispy.mcmc.preparation' not in sys.modules",
        ],
        check=True,
        env=environment,
    )


def test_event_public_api_uses_descriptive_names():
    from seispy import event

    assert callable(event.cut_event_waveforms)
    assert callable(event.cut_events_binary)
    assert callable(event.write_event_catalog)


def test_download_public_api_exposes_experimental_mass_downloader():
    from seispy import download

    assert callable(download.mass_download_waveforms)
    assert download.BulkDownloadSummary.__name__ == "BulkDownloadSummary"


def test_decimation_public_interface_uses_workflow_name():
    from seispy import waveform

    assert callable(waveform.decimate_waveforms)
    assert not hasattr(waveform, "decimate_by_station")
