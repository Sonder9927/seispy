import importlib.util
import inspect
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
import numpy as np
from obspy import Trace, UTCDateTime

_MODULE_PATH = (
    Path(__file__).parents[1] / "src" / "seispy" / "response" / "remove_response.py"
)
_SPEC = importlib.util.spec_from_file_location(
    "remove_response_under_test", _MODULE_PATH
)
assert _SPEC is not None and _SPEC.loader is not None
remove_response = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = remove_response
_SPEC.loader.exec_module(remove_response)


class _WritableTrace:
    def __init__(self, content: bytes = b"processed"):
        self.content = content

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(self.content)


class _WritableStream(list):
    def __init__(self, content: bytes = b"processed"):
        super().__init__([_WritableTrace(content)])


def test_obspy_deconv_writes_to_mirrored_output_directory(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA" / "2026" / "001"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original")
    output_root = tmp_path / "processed"

    with (
        patch.object(
            remove_response, "stream_removed_response", return_value=_WritableStream()
        ),
        patch.object(remove_response, "_validate_output_trace"),
    ):
        results = remove_response.obspy_deconv(
            source_root / "STA",
            "*.sac",
            object(),
            source_root,
            output_root,
            False,
            20,
        )

    destination = output_root / "STA" / "2026" / "001" / "trace.sac"
    assert source.read_bytes() == b"original"
    assert destination.read_bytes() == b"processed"
    assert results.total == 1
    assert results.succeeded == 1
    assert results.failed == 0


def test_obspy_deconv_accepts_miniseed_and_writes_sac_extension(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.mseed"
    source.write_bytes(b"miniseed")
    output_root = tmp_path / "processed"

    with (
        patch.object(
            remove_response, "stream_removed_response", return_value=_WritableStream()
        ),
        patch.object(remove_response, "_validate_output_trace"),
    ):
        result = remove_response.obspy_deconv(
            station,
            "*.mseed",
            object(),
            source_root,
            output_root,
            False,
            20,
        )

    assert result.succeeded == 1
    assert (output_root / "STA" / "trace.sac").read_bytes() == b"processed"
    assert not (output_root / "STA" / "trace.mseed").exists()


def test_sac_backend_rejects_miniseed_before_reading_inventory(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.mseed"
    source.write_bytes(b"miniseed")

    with (
        patch.object(remove_response.obspy, "read_inventory") as read_inventory,
        pytest.raises(ValueError, match='backend="sac" does not support MiniSEED'),
    ):
        remove_response.deconvolution_by_station(
            source_root,
            tmp_path / "stations.xml",
            backend="sac",
            pattern="*.mseed",
            output_dir=tmp_path / "output",
            save_report=False,
        )

    read_inventory.assert_not_called()


def test_multitrace_miniseed_gets_one_unique_sac_name_per_trace(tmp_path):
    source_root = tmp_path / "source"
    target = source_root / "STA" / "day.mseed"
    output_root = tmp_path / "output"

    def trace(channel):
        return SimpleNamespace(
            stats=SimpleNamespace(
                network="NZ",
                station="AAA",
                location="",
                channel=channel,
                starttime=UTCDateTime("2026-01-01"),
            )
        )

    destinations = remove_response._obspy_destinations(
        target,
        [trace("BHZ"), trace("BHN")],
        source_root,
        output_root,
        False,
    )

    assert len(set(destinations)) == 2
    assert all(path.suffix == ".sac" for path in destinations)
    assert any("BHZ" in path.name for path in destinations)
    assert any("BHN" in path.name for path in destinations)


def test_remove_original_failure_preserves_source_and_is_reported(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original")

    def fail(*args, **kwargs):
        raise RuntimeError("response unavailable")

    with patch.object(remove_response, "stream_removed_response", side_effect=fail):
        results = remove_response.obspy_deconv(
            station, "*.sac", object(), source_root, None, True, 20
        )

    assert source.read_bytes() == b"original"
    assert not source.with_suffix(".deconv.sac").exists()
    assert results.failed == 1
    assert results.succeeded == 0
    assert results.issue_samples[0].destination == source.with_suffix(".deconv.sac")
    assert results.issue_samples[0].error == "RuntimeError: response unavailable"


def test_remove_original_success_creates_deconv_and_removes_source(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original")
    with (
        patch.object(
            remove_response, "stream_removed_response", return_value=_WritableStream()
        ),
        patch.object(remove_response, "_validate_output_trace"),
    ):
        results = remove_response.obspy_deconv(
            station, "*.sac", object(), source_root, None, True, 20
        )

    assert not source.exists()
    assert source.with_suffix(".deconv.sac").read_bytes() == b"processed"
    assert results.succeeded == 1
    assert results.failed == 0
    assert results.removal_failed == 0


def test_remove_original_rejects_output_directory(tmp_path):
    try:
        remove_response._resolve_output_dir(
            tmp_path / "source", tmp_path / "output", True
        )
    except ValueError as exc:
        assert "output_dir" in str(exc)
    else:
        raise AssertionError("ValueError was not raised")


def test_previous_deconv_result_is_not_processed_again(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    previous = station / "trace.deconv.sac"
    source.write_bytes(b"original")
    previous.write_bytes(b"previous")
    output_root = tmp_path / "processed"

    with (
        patch.object(
            remove_response, "stream_removed_response", return_value=_WritableStream()
        ),
        patch.object(remove_response, "_validate_output_trace"),
    ):
        results = remove_response.obspy_deconv(
            station, "*.sac", object(), source_root, output_root, False, 20
        )

    assert results.total == 1
    assert previous.read_bytes() == b"previous"


def test_pre_filter_is_unchanged_when_below_nyquist():
    requested = (0.004, 0.006, 30.0, 35.0)

    assert remove_response._effective_pre_filt(requested, 100.0) == requested


def test_default_pre_filter_and_daily_taper_are_conservative():
    assert remove_response.DEFAULT_PRE_FILTER == (0.004, 0.006, 4.0, 5.0)
    trace = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
    assert remove_response._sac_taper_width(trace) == pytest.approx(150 / 86_400)


def test_pre_filter_upper_corners_are_reduced_below_nyquist():
    result = remove_response._effective_pre_filt(
        (0.004, 0.006, 30.0, 35.0), sampling_rate=50.0
    )

    assert result == pytest.approx((0.004, 0.006, 20.0, 23.75))


@pytest.mark.parametrize(
    "pre_filt",
    [
        (0.004, 0.006, 30.0),
        (0.004, 0.006, 35.0, 30.0),
        (0.0, 0.006, 30.0, 35.0),
    ],
)
def test_invalid_pre_filter_is_rejected(pre_filt):
    with pytest.raises(ValueError, match="pre_filt"):
        remove_response._validate_pre_filt(pre_filt)


def test_pre_filter_low_corner_must_be_below_nyquist():
    with pytest.raises(ValueError, match="Nyquist"):
        remove_response._effective_pre_filt((0.4, 0.6, 3.0, 3.5), sampling_rate=1.0)


def test_pre_filter_requires_high_frequency_rolloff_room_below_nyquist():
    with pytest.raises(ValueError, match="rolloff"):
        remove_response._effective_pre_filt(
            (0.4, 0.96, 1.2, 1.5), sampling_rate=2.0
        )


def test_adjusted_pre_filter_remains_strictly_increasing():
    result = remove_response._effective_pre_filt(
        (0.4, 0.94, 1.2, 1.5), sampling_rate=2.0
    )

    assert all(left < right for left, right in zip(result, result[1:]))
    assert result[-1] < 1.0


@pytest.mark.parametrize("factors", [0, 1, 8, [5, 1], [2, 8], [2.0]])
def test_deconvolution_rejects_non_sac_decimation_factors(tmp_path, factors):
    source = tmp_path / "source"
    source.mkdir()

    with pytest.raises(ValueError, match="integers from 2 through 7"):
        remove_response.deconvolution_by_station(
            source,
            tmp_path / "stations.xml",
            output_dir=tmp_path / "output",
            decimate_factors=factors,
        )


def test_decimation_must_leave_passband_below_nyquist():
    with pytest.raises(ValueError, match="decimation leaves Nyquist"):
        remove_response._final_sampling_rate(
            1.0, (5, 5, 4), remove_response.DEFAULT_PRE_FILTER
        )


def test_deconvolution_decimation_is_optional():
    signature = inspect.signature(remove_response.deconvolution_by_station)

    assert signature.parameters["decimate_factors"].default is None


def test_deconvolution_public_interface_uses_backend_term():
    signature = inspect.signature(remove_response.deconvolution_by_station)

    assert signature.parameters["backend"].default == "obspy"
    assert "method" not in signature.parameters


def test_unknown_deconvolution_backend_is_rejected():
    with pytest.raises(ValueError, match="Unknown backend"):
        remove_response._deconvolution_backend("unknown")


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
            remove_response, "_response_epoch_for_trace", return_value=(object(), object())
        ),
        patch.object(
            remove_response, "_sac_compatible_decimate_trace", side_effect=decimate
        ),
    ):
        remove_response.stream_removed_response(
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


def test_deconvolution_summary_status_includes_removal_failures():
    summary = remove_response.DeconvolutionSummary(
        run_id="run",
        total=1,
        succeeded=1,
        failed=0,
        removal_failed=1,
        response_conflicts=0,
        issue_samples=(),
        output_dir=None,
        remove_original=True,
        duration_seconds=0.1,
    )

    assert summary.has_issues
    assert not summary.ok


def test_sac_deconv_reuses_process_for_bounded_file_batches(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    for index in range(5):
        (station / f"trace-{index}.sac").write_bytes(b"original")
    output_root = tmp_path / "processed"

    def run_sac(command, *, input, **kwargs):
        assert command == ["sac"]
        for line in input.decode().splitlines():
            if line.startswith("w "):
                Path(line[2:]).write_bytes(b"processed")
        return SimpleNamespace(returncode=0, stderr=b"")

    sampling_rate = SimpleNamespace(
        stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000)
    )
    with (
        patch.object(remove_response.obspy, "read", return_value=[sampling_rate]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac) as run,
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        summary = remove_response.sac_deconv(
            station,
            "*.sac",
            "response.pz",
            source_root,
            output_root,
            False,
            20,
            batch_size=2,
        )

    assert run.call_count == 3
    assert summary.total == 5
    assert summary.succeeded == 5
    assert summary.failed == 0
    assert len(list(output_root.rglob("*.sac"))) == 5


def test_sac_batch_failure_preserves_all_sources(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")

    failed_process = SimpleNamespace(returncode=1, stderr=b"SAC batch failed")
    sampling_rate = SimpleNamespace(
        stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000)
    )
    with (
        patch.object(remove_response.obspy, "read", return_value=[sampling_rate]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", return_value=failed_process),
    ):
        summary = remove_response.sac_deconv(
            station,
            "*.sac",
            "response.pz",
            source_root,
            None,
            True,
            20,
        )

    assert summary.failed == 3
    assert summary.succeeded == 0
    assert all(source.read_bytes() == b"original" for source in sources)
    assert not list(station.glob("*.deconv.sac"))


def test_sac_batch_failure_is_bisected_to_isolate_bad_file(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(4)]
    for source in sources:
        source.write_bytes(b"original")
    bad = sources[2]
    output_root = tmp_path / "processed"

    def run_sac(command, *, input, **kwargs):
        script = input.decode()
        if f"r {bad}" in script:
            return SimpleNamespace(returncode=1, stderr=b"bad trace")
        for line in script.splitlines():
            if line.startswith("w "):
                Path(line[2:]).write_bytes(b"processed")
        return SimpleNamespace(returncode=0, stderr=b"")

    header = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
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
        summary = remove_response.sac_deconv(
            station,
            "*.sac",
            object(),
            source_root,
            output_root,
            False,
            20,
            batch_size=4,
        )

    assert summary.succeeded == 3
    assert summary.failed == 1
    assert len(list(output_root.rglob("*.sac"))) == 3


def test_deconvolved_output_validation_rejects_non_finite_data(tmp_path):
    source = tmp_path / "source.sac"
    output = tmp_path / "output.sac"
    header = {
        "sampling_rate": 100.0,
        "starttime": UTCDateTime("2026-01-01"),
    }
    Trace(data=np.arange(10, dtype=np.float32), header=header).write(
        str(source), format="SAC"
    )
    invalid = np.arange(10, dtype=np.float32)
    invalid[4] = np.nan
    invalid_trace = Trace(data=invalid, header=header)
    invalid_trace.write(str(output), format="SAC")

    with pytest.raises(ValueError, match="NaN or infinite"):
        remove_response._validate_deconvolved_file(
            source, output, processed=[invalid_trace]
        )


def test_response_preflight_counts_only_affected_files(tmp_path):
    station = tmp_path / "AAA"
    station.mkdir()
    targets = [station / "one.sac", station / "two.sac"]
    for target in targets:
        target.write_bytes(b"trace")
    inv = Mock()
    inv.select.return_value = inv
    traces = [SimpleNamespace(id="NZ.AAA..BHZ"), SimpleNamespace(id="NZ.AAA..BHZ")]

    with (
        patch.object(
            remove_response, "_inventory_has_overlapping_epochs", return_value=True
        ),
        patch.object(
            remove_response.obspy, "read", side_effect=[[item] for item in traces]
        ),
        patch.object(
            remove_response,
            "_response_epoch_for_trace",
            side_effect=[ValueError("ambiguous"), (object(), object())],
        ),
    ):
        count = remove_response._preflight_response_conflicts([station], "*.sac", inv)

    assert count == 1


def test_sac_response_is_selected_by_full_id_and_epoch_and_cached(tmp_path):
    class Container(list):
        pass

    channel = SimpleNamespace(
        start_date=remove_response.obspy.UTCDateTime("2024-01-01"),
        end_date=remove_response.obspy.UTCDateTime("2025-01-01"),
    )
    selected = Container([Container([Container([channel])])])
    selected.get_response = Mock()

    def write_pz(filename, format):
        assert format == "SACPZ"
        Path(filename).write_text("* INPUT UNIT : M\nZEROS 3\n")

    selected.write = Mock(side_effect=write_pz)
    inventory = Mock()
    inventory.select.return_value = selected
    stats = SimpleNamespace(
        network="NZ",
        station="AAA",
        location="10",
        channel="BHZ",
        starttime=remove_response.obspy.UTCDateTime("2024-06-01"),
        endtime=remove_response.obspy.UTCDateTime("2024-06-02"),
    )
    trace = SimpleNamespace(id="NZ.AAA.10.BHZ", stats=stats)
    cache = {}

    first = remove_response._sac_pz_for_trace(inventory, trace, tmp_path, cache)
    second = remove_response._sac_pz_for_trace(inventory, trace, tmp_path, cache)

    assert first == second
    assert selected.write.call_count == 1
    inventory.select.assert_called_with(
        network="NZ",
        station="AAA",
        location="10",
        channel="BHZ",
        time=stats.starttime,
    )
    assert selected.get_response.call_count == 2


def test_obspy_uses_epoch_covering_the_complete_trace():
    class Container(list):
        pass

    channel = SimpleNamespace(
        start_date=remove_response.obspy.UTCDateTime("2024-01-01"),
        end_date=remove_response.obspy.UTCDateTime("2025-01-01"),
    )
    selected = Container([Container([Container([channel])])])
    selected.get_response = Mock()
    inventory = Mock()
    inventory.select.return_value = selected
    stats = SimpleNamespace(
        network="NZ",
        station="AAA",
        location="10",
        channel="BHZ",
        sampling_rate=100.0,
        starttime=remove_response.obspy.UTCDateTime("2024-06-01"),
        endtime=remove_response.obspy.UTCDateTime("2024-06-02"),
    )
    trace = Mock()
    trace.id = "NZ.AAA.10.BHZ"
    trace.stats = stats
    trace.data = 1.0

    class Stream(list):
        def get_gaps(self):
            return []

        def merge(self, **kwargs):
            return self

    with patch.object(remove_response.obspy, "read", return_value=Stream([trace])):
        remove_response.stream_removed_response("trace.sac", inventory)

    assert trace.remove_response.call_args.kwargs["inventory"] is selected


def test_obspy_rejects_trace_crossing_response_epoch():
    class Container(list):
        pass

    epoch_end = remove_response.obspy.UTCDateTime("2024-06-01T12:00:00")
    channel = SimpleNamespace(
        start_date=remove_response.obspy.UTCDateTime("2024-01-01"),
        end_date=epoch_end,
    )
    selected = Container([Container([Container([channel])])])
    selected.get_response = Mock()
    inventory = Mock()
    inventory.select.return_value = selected
    trace = SimpleNamespace(
        id="NZ.AAA.10.BHZ",
        stats=SimpleNamespace(
            network="NZ",
            station="AAA",
            location="10",
            channel="BHZ",
            starttime=remove_response.obspy.UTCDateTime("2024-06-01"),
            endtime=remove_response.obspy.UTCDateTime("2024-06-02"),
        ),
    )

    with pytest.raises(ValueError, match="response changes within"):
        remove_response._response_epoch_for_trace(inventory, trace)
