"""Entry scripts that spawn-based worker pools cannot re-import."""

import multiprocessing
import os
import subprocess
import sys
import textwrap
import types
from pathlib import Path

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

from seispy._main_guard import (
    require_reimport_safe_entry_point,
)

ROOT = Path(__file__).parents[1]
SUBPROCESS_TIMEOUT = 60


def _run_entry_script_module(tmp_path, monkeypatch, source):
    """Execute ``source`` as the ``__main__`` module of this process."""
    path = tmp_path / "mission.py"
    path.write_text(textwrap.dedent(source).lstrip("\n"), encoding="utf-8")
    module = types.ModuleType("__main__")
    module.__file__ = str(path)
    monkeypatch.setitem(sys.modules, "__main__", module)
    scope = {
        "__name__": "__main__",
        "__file__": str(path),
        "check": require_reimport_safe_entry_point,
    }
    exec(compile(path.read_text(encoding="utf-8"), str(path), "exec"), scope)


@pytest.mark.parametrize(
    "scenario",
    [
        "module_level_call_is_rejected",
        "guard_accepts_module_level_setup_and_call",
        "guard_accepts_an_inline_call",
        "fork_start_method_accepts_module_level_call",
    ],
)
def test_entry_script_policy(tmp_path, monkeypatch, scenario):
    if scenario == "module_level_call_is_rejected":
        with pytest.raises(RuntimeError) as error:
            _run_entry_script_module(
                tmp_path,
                monkeypatch,
                """
                import pathlib

                BASE = pathlib.Path("/data")
                check("deconvolve_waveforms", start_method="spawn")
                """,
            )

        message = str(error.value)
        assert "mission.py:4" in message
        assert 'if __name__ == "__main__":' in message
        assert "deconvolve_waveforms" in message
    elif scenario == "guard_accepts_module_level_setup_and_call":
        _run_entry_script_module(
            tmp_path,
            monkeypatch,
            """
            import pathlib

            BASE = pathlib.Path("/data")

            def main():
                check("deconvolve_waveforms", start_method="spawn")

            if __name__ == "__main__":
                main()
            """,
        )
    elif scenario == "guard_accepts_an_inline_call":
        _run_entry_script_module(
            tmp_path,
            monkeypatch,
            """
            import pathlib

            BASE = pathlib.Path("/data")

            if __name__ == "__main__":
                check("deconvolve_waveforms", start_method="spawn")
            """,
        )
    elif scenario == "fork_start_method_accepts_module_level_call":
        _run_entry_script_module(
            tmp_path,
            monkeypatch,
            """
            check("deconvolve_waveforms", start_method="fork")
            """,
        )


def test_bootstrapping_worker_is_rejected(monkeypatch):
    monkeypatch.setattr(
        multiprocessing.current_process(), "_inheriting", True, raising=False
    )

    with pytest.raises(RuntimeError, match="worker process was starting"):
        require_reimport_safe_entry_point("deconvolve_waveforms", start_method="spawn")


def _run_entry_script(directory, source):
    script = directory / "mission.py"
    script.write_text(textwrap.dedent(source).lstrip("\n"), encoding="utf-8")
    environment = os.environ.copy()
    environment["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.run(
        [sys.executable, str(script)],
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT,
        env=environment,
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "unguarded_script_reports_the_guard_instead_of_hanging",
        "unguarded_script_runs_with_fork",
        "guarded_script_processes_files_with_spawn",
    ],
)
def test_spawn_and_fork_scripts(tmp_path, scenario):
    if scenario == "unguarded_script_reports_the_guard_instead_of_hanging":
        waveforms = tmp_path / "waveforms"
        waveforms.mkdir()
        output = tmp_path / "deconvolved"
        completed = _run_entry_script(
            tmp_path,
            f'''
            """Entry script without a __main__ guard, on a spawn platform."""
            import multiprocessing

            multiprocessing.set_start_method("spawn", force=True)
            from seispy import deconvolution

            summary = deconvolution.deconvolve_waveforms(
                {str(waveforms)!r},
                {str(tmp_path / "stations.xml")!r},
                output_dir={str(output)!r},
                max_workers=1,
            )
            print(summary.succeeded)
            ''',
        )

        assert completed.returncode != 0
        assert "deconvolve_waveforms" in completed.stderr
        assert "mission.py:7" in completed.stderr
        assert 'if __name__ == "__main__":' in completed.stderr
        assert not output.exists()
    elif scenario == "unguarded_script_runs_with_fork":
        """The Linux default re-runs nothing, so no guard is required."""
        waveforms = tmp_path / "waveforms"
        _write_source_mseed(waveforms)
        output = tmp_path / "filtered"
        completed = _run_entry_script(
            tmp_path,
            f'''
            """Entry script without a __main__ guard, on a fork platform."""
            import multiprocessing

            multiprocessing.set_start_method("fork")
            from seispy import waveform

            summary = waveform.filter_waveforms(
                {str(waveforms)!r},
                {str(output)!r},
                trace_filter=waveform.TraceFilter(min_duration_seconds=5),
                max_workers=1,
                files_per_pool=1,
                save_report=False,
                save_log=False,
            )
            print(summary.succeeded)
            ''',
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip().endswith("1")
        assert len(list(output.rglob("*.mseed"))) == 1
    elif scenario == "guarded_script_processes_files_with_spawn":
        waveforms = tmp_path / "waveforms"
        _write_source_mseed(waveforms)
        output = tmp_path / "filtered"
        completed = _run_entry_script(
            tmp_path,
            f"""
            import multiprocessing

            multiprocessing.set_start_method("spawn", force=True)
            from seispy import waveform

            def main():
                summary = waveform.filter_waveforms(
                    {str(waveforms)!r},
                    {str(output)!r},
                    trace_filter=waveform.TraceFilter(min_duration_seconds=5),
                    max_workers=1,
                    files_per_pool=1,
                    save_report=False,
                    save_log=False,
                )
                print(summary.succeeded)

            if __name__ == "__main__":
                main()
            """,
        )

        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.strip().endswith("1")
        assert len(list(output.rglob("*.mseed"))) == 1


def _write_source_mseed(waveforms):
    station_dir = waveforms / "NZ" / "AAA"
    station_dir.mkdir(parents=True)
    trace = Trace(np.arange(1000, dtype=np.int32))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "BHZ"
    trace.stats.sampling_rate = 100.0
    trace.stats.starttime = UTCDateTime("2026-01-01")
    Stream([trace]).write(str(station_dir / "day.mseed"), format="MSEED")
