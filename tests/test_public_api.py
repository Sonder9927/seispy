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
            "assert 'seispy.decimate' not in sys.modules",
        ],
        check=True,
        env=environment,
    )


def test_event_public_api_uses_new_names():
    from seispy import event

    assert callable(event.cut_events)
    assert callable(event.cut_events_binary)
    assert callable(event.write_event_catalog)


def test_download_public_api_exposes_experimental_mass_downloader():
    from seispy import download

    assert callable(download.download_waveforms_mass)
    assert download.MassDownloadResult.__name__ == "MassDownloadResult"


def test_decimation_public_interface_uses_file_name():
    import seispy

    assert callable(seispy.decimate_files)
    assert not hasattr(seispy, "decimate_by_station")
