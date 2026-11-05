import pytest
import os
import subprocess
import sys
from pathlib import Path


@pytest.mark.parametrize(
    "scenario",
    [
        "top_level_import_is_lazy",
        "domain_packages_do_not_import_their_implementations_eagerly",
    ],
)
def test_lazy_imports(scenario):
    root = Path(__file__).parents[1]
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(root / "src")
    if scenario == "top_level_import_is_lazy":
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
    elif scenario == "domain_packages_do_not_import_their_implementations_eagerly":
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, seispy.download, seispy.waveform, seispy.inventory, "
                "seispy.deconvolution, seispy.mcmc; "
                "assert 'seispy.waveform.coverage' not in sys.modules; "
                "assert 'seispy.waveform.decimation' not in sys.modules; "
                "assert 'seispy.inventory.analysis' not in sys.modules; "
                "assert 'seispy.deconvolution.removal' not in sys.modules; "
                "assert 'seispy.waveform.orientation' not in sys.modules; "
                "assert 'seispy.waveform.clock' not in sys.modules; "
                "assert 'seispy.mcmc.workflow' not in sys.modules",
            ],
            check=True,
            env=environment,
        )
