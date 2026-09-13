from importlib import import_module
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

remove_response = import_module("seispy.response.removal")


def test_response_removal_preserves_gap_and_processes_each_segment():
    first = Trace(np.arange(100, dtype=np.float32))
    first.stats.network = "NZ"
    first.stats.station = "ABAZ"
    first.stats.location = "11"
    first.stats.channel = "HHE"
    first.stats.sampling_rate = 100.0
    first.stats.starttime = UTCDateTime("2023-08-01")
    second = first.copy()
    second.stats.starttime = first.stats.endtime + 10.01
    stream = Stream([first, second])

    with (
        patch.object(remove_response.obspy, "read", return_value=stream),
        patch.object(
            remove_response,
            "_response_epoch_for_trace",
            return_value=(object(), object()),
        ),
        patch.object(Trace, "remove_response") as deconvolve,
    ):
        result = remove_response.remove_response_from_file("trace.mseed", object())

    assert len(result) == 2
    assert len(result.get_gaps()) == 1
    assert deconvolve.call_count == 2


def test_obspy_preprocesses_then_decimates_before_removing_response():
    calls = []
    trace = Mock()
    trace.stats = SimpleNamespace(sampling_rate=100.0, npts=10_000)
    trace.data = np.arange(10_000, dtype=np.float32)
    trace.detrend.side_effect = lambda *args, **kwargs: calls.append("detrend")
    trace.taper.side_effect = lambda *args, **kwargs: calls.append("taper")
    trace.remove_response.side_effect = lambda *args, **kwargs: calls.append("response")

    class Stream(list):
        def get_gaps(self):
            return []

        def merge(self, **kwargs):
            return self

    def decimate(item, factors):
        calls.append("decimate")
        item.stats.sampling_rate = 1.0

    with (
        patch.object(remove_response.obspy, "read", return_value=Stream([trace])),
        patch.object(
            remove_response,
            "_response_epoch_for_trace",
            return_value=(object(), object()),
        ),
        patch.object(
            remove_response, "_sac_compatible_decimate_trace", side_effect=decimate
        ),
    ):
        remove_response.remove_response_from_file(
            "trace.sac", object(), decimate_factors=[5, 5, 4]
        )

    assert calls == ["detrend", "detrend", "taper", "decimate", "response"]


def test_sac_decimation_commands_precede_response_removal(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    (station / "trace.sac").write_bytes(b"original")
    output_root = tmp_path / "processed"
    scripts = []

    def run_sac(command, *, input, **kwargs):
        script = input.decode()
        scripts.append(script)
        for line in script.splitlines():
            if line.startswith("w "):
                Path(line[2:]).write_bytes(b"processed")
        return SimpleNamespace(returncode=0, stderr=b"")

    header = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=10_000))
    with (
        patch.object(remove_response.obspy, "read", return_value=[header]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac),
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        result = remove_response.sac_deconv(
            station,
            "*.sac",
            object(),
            source_root,
            output_root,
            False,
            20,
            decimate_factors=[5, 5, 4],
        )

    assert result.succeeded == 1
    script = scripts[0]
    assert script.index("rmean; rtr; taper") < script.index("decimate 5")
    assert script.index("decimate 5") < script.index("trans from pol")
    assert script.index("decimate 4") < script.index("trans from pol")
