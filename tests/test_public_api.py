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
    assert not hasattr(waveform, "decimate_by_station")
