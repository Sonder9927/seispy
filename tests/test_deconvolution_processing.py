from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

remove_response = import_module("seispy.deconvolution.removal")


@pytest.mark.parametrize(
    "scenario",
    [
        "response_removal_preserves_gap_and_processes_each_segment",
        "obspy_preprocesses_then_decimates_before_removing_response",
    ],
)
def test_obspy_segment_processing(scenario):
    if scenario == "response_removal_preserves_gap_and_processes_each_segment":
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
    elif scenario == "obspy_preprocesses_then_decimates_before_removing_response":
        calls = []
        trace = Mock()
        trace.stats = SimpleNamespace(sampling_rate=100.0, npts=10_000)
        trace.data = np.arange(10_000, dtype=np.float32)
        trace.detrend.side_effect = lambda *args, **kwargs: calls.append("detrend")
        trace.taper.side_effect = lambda *args, **kwargs: calls.append("taper")
        trace.remove_response.side_effect = lambda *args, **kwargs: calls.append(
            "response"
        )

        class ProcessingStream(list):
            def get_gaps(self):
                return []

            def merge(self, **kwargs):
                return self

        def decimate(item, factors):
            calls.append("decimate")
            item.stats.sampling_rate = 1.0

        with (
            patch.object(
                remove_response.obspy, "read", return_value=ProcessingStream([trace])
            ),
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


@pytest.mark.parametrize("post", [(), (5,)])
def test_sac_decimation_commands_precede_response_removal(tmp_path, post):
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
    source = station / "trace.sac"
    combined_pz = tmp_path / "responses.pz"
    combined_pz.write_text("combined")
    with (
        patch.object(remove_response.obspy, "read", return_value=[header]),
        patch.object(remove_response, "_response_epoch_for_trace"),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac),
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        result = remove_response._process_sac_batch(
            [source],
            object(),
            combined_pz,
            source_root,
            output_root,
            20,
            remove_response.DEFAULT_PRE_FILTER,
            {},
            (5, 5, 4),
            post_decimate_factors=post,
        )

    assert result.succeeded == 1
    script = scripts[0]
    assert script.index("rmean; rtr; taper") < script.index("decimate 5")
    assert script.index("decimate 5") < script.index("trans from pol")
    assert script.index("decimate 4") < script.index("trans from pol")
    if post:
        assert (
            script.index("mul 1.0e9") < script.rindex("decimate 5") < script.index("w ")
        )


@pytest.mark.parametrize("rate", [100, 25])
def test_response_epoch_allows_prior_decimation_with_warning(rate):
    import warnings
    from obspy.core.inventory import Channel, Inventory, Network, Station, Response

    trace = Trace(np.arange(100, dtype=np.float32))
    trace.stats.update(
        dict(
            network="XX",
            station="AAA",
            location="",
            channel="BHZ",
            sampling_rate=rate,
            starttime=UTCDateTime("2024-01-01"),
        )
    )
    channel = Channel(
        "BHZ",
        "",
        0,
        0,
        0,
        0,
        sample_rate=100,
        response=Response.from_paz([0j], [-1 + 0j], 1, 1),
    )
    inventory = Inventory(
        [Network("XX", [Station("AAA", 0, 0, 0, channels=[channel])])], "test"
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        selected, result = remove_response._response_epoch_for_trace(inventory, trace)
    assert result.sample_rate == 100
    assert selected.get_response(trace.id, trace.stats.starttime)
    assert len(caught) == (1 if rate == 25 else 0)
    if caught:
        assert "sample rate mismatch" in str(caught[0].message)
