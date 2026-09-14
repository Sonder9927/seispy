import inspect
import json
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from obspy import UTCDateTime

from seispy.download import stations, waveforms


def test_download_is_raw_only_and_defaults_to_ten_network_workers():
    parameters = inspect.signature(waveforms.download_waveforms).parameters

    assert parameters["network_workers"].default == 10
    assert "output_format" not in parameters
    assert "validation_workers" not in parameters


def test_raw_response_is_saved_byte_for_byte(tmp_path):
    payload = b"arbitrary response bytes that must not be decoded"
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
    client = Mock()

    def fail_then_write(**kwargs):
        if client.get_waveforms.call_count == 1:
            raise TimeoutError("temporary")
        Path(kwargs["filename"]).write_bytes(b"raw")

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

    assert destination.read_bytes() == b"raw"
    assert client.get_waveforms.call_count == 2
    assert sleep.call_count == 1


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
