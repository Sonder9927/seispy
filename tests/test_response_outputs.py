from importlib import import_module
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Trace, UTCDateTime

remove_response = import_module("seispy.response.removal")


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
            remove_response, "remove_response_from_file", return_value=_WritableStream()
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
            remove_response, "remove_response_from_file", return_value=_WritableStream()
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
        remove_response.remove_instrument_response(
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

    with patch.object(remove_response, "remove_response_from_file", side_effect=fail):
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
            remove_response, "remove_response_from_file", return_value=_WritableStream()
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
            remove_response, "remove_response_from_file", return_value=_WritableStream()
        ),
        patch.object(remove_response, "_validate_output_trace"),
    ):
        results = remove_response.obspy_deconv(
            station, "*.sac", object(), source_root, output_root, False, 20
        )

    assert results.total == 1
    assert previous.read_bytes() == b"previous"
