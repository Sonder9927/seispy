import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_MODULE_PATH = (
    Path(__file__).parents[1] / "src" / "seispy" / "response" / "remove_response.py"
)
_SPEC = importlib.util.spec_from_file_location("remove_response_under_test", _MODULE_PATH)
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
            None,
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

    def fail(*args):
        raise RuntimeError("response unavailable")

    with patch.object(remove_response, "stream_removed_response", side_effect=fail):
        results = remove_response.obspy_deconv(
            station, "*.sac", object(), None, source_root, None, True, 20
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
            station, "*.sac", object(), None, source_root, None, True, 20
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
            station, "*.sac", object(), None, source_root, output_root, False, 20
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
        remove_response._effective_pre_filt(
            (0.4, 0.6, 3.0, 3.5), sampling_rate=1.0
        )
