import os
import subprocess
import sys
from pathlib import Path


def test_top_level_import_is_lazy():
    root = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(root / "src"), str(root / "packages" / "rose" / "src")]
    )
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys, seispy; "
            "assert 'seispy.mcmc' not in sys.modules; "
            "assert 'seispy.resample' not in sys.modules",
        ],
        check=True,
        env=environment,
    )


def test_event_public_api_uses_new_names():
    from seispy import event

    assert callable(event.cut_events)
    assert callable(event.cut_events_binary)
    assert callable(event.write_event_catalog)
