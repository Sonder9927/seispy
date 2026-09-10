import importlib.util
import inspect
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

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


class _WritableStream:
    def __init__(self, content: bytes = b"processed"):
        self.content = content

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(self.content)


def test_obspy_deconv_writes_to_mirrored_output_directory(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA" / "2026" / "001"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original")
    output_root = tmp_path / "processed"

    with patch.object(
        remove_response, "stream_removed_response", return_value=_WritableStream()
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
    with patch.object(
        remove_response, "stream_removed_response", return_value=_WritableStream()
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

    with patch.object(
        remove_response, "stream_removed_response", return_value=_WritableStream()
    ):
        results = remove_response.obspy_deconv(
            station, "*.sac", object(), source_root, output_root, False, 20
        )

    assert results.total == 1
    assert previous.read_bytes() == b"previous"


def test_pre_filter_is_unchanged_when_below_nyquist():
    requested = (0.004, 0.006, 30.0, 35.0)

    assert remove_response._effective_pre_filt(requested, 100.0) == requested


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


@pytest.mark.parametrize(
    "function",
    [
        remove_response.deconvolution_by_station,
        remove_response.obspy_deconv,
        remove_response.sac_deconv,
        remove_response.stream_removed_response,
    ],
)
def test_response_removal_interfaces_do_not_accept_resample(function):
    assert "resample" not in inspect.signature(function).parameters


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

    sampling_rate = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0))
    with (
        patch.object(remove_response.obspy, "read", return_value=[sampling_rate]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac) as run,
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
    sampling_rate = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0))
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
    selected.get_response.assert_called_once_with(trace.id, stats.starttime)
