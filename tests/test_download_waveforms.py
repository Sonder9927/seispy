import inspect
import io
import json
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

from seispy.download import stations, waveforms


def _mseed_payload(npts=100):
    trace = Trace(np.arange(npts, dtype=np.int32))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = 1.0
    buffer = io.BytesIO()
    Stream([trace]).write(buffer, format="MSEED")
    return buffer.getvalue()


def _fragmented_mseed_payload():
    first = Trace(np.arange(100, dtype=np.int32))
    first.stats.network = "NZ"
    first.stats.station = "AAA"
    first.stats.location = "10"
    first.stats.channel = "HHZ"
    first.stats.starttime = UTCDateTime("2026-01-01T00:00:00")
    first.stats.sampling_rate = 1.0
    second = first.copy()
    second.stats.starttime = UTCDateTime("2026-01-01T01:00:00")
    buffer = io.BytesIO()
    Stream([first, second]).write(buffer, format="MSEED")
    return buffer.getvalue()


def _stage(client, destination, **kwargs):
    with patch.object(waveforms, "_thread_client", return_value=client):
        return waveforms._download_raw_response(
            "client",
            None,
            None,
            destination,
            "NZ",
            "AAA",
            "10",
            "HHZ",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            False,
            1,
            0,
            0,
            None,
            **kwargs,
        )


def test_download_is_raw_only_and_defaults_to_ten_network_workers():
    parameters = inspect.signature(waveforms.download_waveforms).parameters

    assert parameters["network_workers"].default == 10
    assert "output_format" not in parameters
    assert "validation_workers" not in parameters


def test_raw_response_is_saved_byte_for_byte(tmp_path):
    payload = _mseed_payload()
    client = Mock()

    def download(**kwargs):
        Path(kwargs["filename"]).write_bytes(payload)

    client.get_waveforms.side_effect = download
    destination = tmp_path / "NZ.AAA.2026.001.mseed.raw"
    with patch.object(waveforms, "_thread_client", return_value=client):
        result = waveforms._download_raw_response(
            stations.EARTHSCOPE_URL,
            None,
            None,
            destination,
            "NZ",
            "AAA",
            "*",
            "HH?",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            False,
            1,
            0,
            0,
            None,
        )

    assert destination.read_bytes() == payload
    assert result.succeeded == 1
    assert result.files_written == 1


def test_existing_raw_response_is_skipped_without_request(tmp_path):
    destination = tmp_path / "NZ.AAA.2026.001.mseed.raw"
    destination.write_bytes(b"existing")

    with patch.object(waveforms, "_fetch_waveform_file") as fetch:
        result = waveforms._download_raw_response(
            "client",
            None,
            None,
            destination,
            "NZ",
            "AAA",
            "*",
            "HH?",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            False,
            1,
            0,
            0,
            None,
        )

    fetch.assert_not_called()
    assert result.skipped == 1


def test_transient_request_is_retried(tmp_path):
    payload = _mseed_payload()
    client = Mock()

    def fail_then_write(**kwargs):
        if client.get_waveforms.call_count == 1:
            raise TimeoutError("temporary")
        Path(kwargs["filename"]).write_bytes(payload)

    client.get_waveforms.side_effect = fail_then_write
    destination = tmp_path / "response.mseed.raw"
    with (
        patch.object(waveforms, "_thread_client", return_value=client),
        patch.object(waveforms.time, "sleep") as sleep,
    ):
        waveforms._fetch_waveform_file(
            "client",
            None,
            None,
            "NZ",
            "AAA",
            "10",
            "HHZ",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            destination,
            2,
            0.5,
        )

    assert destination.read_bytes() == payload
    assert client.get_waveforms.call_count == 2
    assert sleep.call_count == 1


def test_non_miniseed_response_is_rejected(tmp_path):
    client = Mock()
    client.get_waveforms.side_effect = lambda **kwargs: Path(
        kwargs["filename"]
    ).write_bytes(b"Error 500: Internal Server Error")
    destination = tmp_path / "response.mseed.raw"

    with patch.object(waveforms, "_thread_client", return_value=client):
        result = waveforms._download_raw_response(
            "client",
            None,
            None,
            destination,
            "NZ",
            "AAA",
            "10",
            "HHZ",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            False,
            1,
            0,
            0,
            None,
        )

    assert result.failed == 1
    assert not destination.exists()
    assert "miniSEED" in result.samples[0].error


def test_content_after_a_valid_record_is_rejected(tmp_path):
    payload = _mseed_payload() + b"Error 500: Internal Server Error"
    client = Mock()
    client.get_waveforms.side_effect = lambda **kwargs: Path(
        kwargs["filename"]
    ).write_bytes(payload)
    destination = tmp_path / "response.mseed.raw"

    with patch.object(waveforms, "_thread_client", return_value=client):
        result = waveforms._download_raw_response(
            "client",
            None,
            None,
            destination,
            "NZ",
            "AAA",
            "10",
            "HHZ",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            False,
            1,
            0,
            0,
            None,
        )

    assert result.failed == 1
    assert not destination.exists()


def test_inventory_task_uses_self_describing_raw_name(tmp_path):
    task = waveforms._InventoryTask(
        "NZ",
        "AAA",
        "10",
        "HHZ",
        100.0,
        UTCDateTime("2026-01-01T06:00:00"),
        UTCDateTime("2026-01-01T18:00:00"),
    )

    path = waveforms._inventory_raw_path(tmp_path, task)

    assert path.name == "NZ.AAA.10.HHZ.2026.001.060000.mseed.raw"


def test_download_summary_and_lifecycle_artifacts(tmp_path):
    completed = waveforms._Counts(total=1, succeeded=1, files_written=1)
    with (
        patch.object(waveforms, "_client"),
        patch.object(waveforms, "_download_day", return_value=completed) as worker,
    ):
        summary = waveforms.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-03",
            station=["AAA"],
            network_workers=2,
        )

    assert worker.call_count == 2
    assert summary.total == summary.succeeded == summary.files_written == 2
    assert json.loads(summary.report_path.read_text())["status"] == "completed"
    assert summary.log_path.is_file()


def test_interrupted_download_leaves_interrupted_report(tmp_path):
    with (
        patch.object(waveforms, "_client"),
        patch.object(waveforms, "_download_day", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        waveforms.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-02",
            station=["AAA"],
            network_workers=1,
        )

    report = next((tmp_path / "logs" / "reports").glob("*.json"))
    assert json.loads(report.read_text())["status"] == "interrupted"


def test_fragmented_response_is_rejected(tmp_path):
    payload = _fragmented_mseed_payload()
    client = Mock()
    client.get_waveforms.side_effect = lambda **kwargs: Path(
        kwargs["filename"]
    ).write_bytes(payload)
    destination = tmp_path / "response.mseed.raw"

    result = _stage(client, destination)

    assert result.failed == 1
    assert not destination.exists()
    assert "gap ratio" in result.samples[0].error


def test_fragmented_response_is_allowed_when_disabled(tmp_path):
    payload = _fragmented_mseed_payload()
    client = Mock()
    client.get_waveforms.side_effect = lambda **kwargs: Path(
        kwargs["filename"]
    ).write_bytes(payload)
    destination = tmp_path / "response.mseed.raw"

    result = _stage(client, destination, max_gap_ratio=None, max_segments=None)

    assert result.succeeded == 1
    assert destination.exists()


def test_segment_limit_rejects_many_traces(tmp_path):
    payload = _fragmented_mseed_payload()
    client = Mock()
    client.get_waveforms.side_effect = lambda **kwargs: Path(
        kwargs["filename"]
    ).write_bytes(payload)
    destination = tmp_path / "response.mseed.raw"

    result = _stage(client, destination, max_segments=1)

    assert result.failed == 1
    assert "segments" in result.samples[0].error


def test_download_rejects_invalid_gap_ratio_setting(tmp_path):
    with pytest.raises(ValueError, match="max_gap_ratio"):
        waveforms.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-02",
            station=["AAA"],
            max_gap_ratio=2.0,
        )
