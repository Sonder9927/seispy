import json
import warnings
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Stream as ObsPyStream
from obspy import Trace, UTCDateTime
from obspy.clients.fdsn.header import FDSNForbiddenException
from obspy.core.inventory import Channel, Inventory, Network, Site, Station
from obspy.io.mseed import InternalMSEEDWarning

from seispy.download import inventory, waveform


class _Stream(list):
    def get_gaps(self):
        return []

    def write(self, filename, format):
        assert format == "MSEED"
        ObsPyStream([item.as_obspy() for item in self]).write(filename, format=format)

    def merge(self, **kwargs):
        return self


class _SacTrace:
    def __init__(self, channel, *, location="10", sampling_rate=1.0):
        self.stats = SimpleNamespace(
            network="NZ",
            station="AAA",
            location=location,
            channel=channel,
            starttime=UTCDateTime("2026-01-01"),
            sampling_rate=sampling_rate,
            mseed=SimpleNamespace(dataquality="D"),
        )

    def write(self, filename, format):
        assert format == "SAC"
        self.as_obspy().write(str(filename), format=format)

    def as_obspy(self):
        trace = Trace(data=np.arange(10, dtype=np.float32))
        trace.stats.network = self.stats.network
        trace.stats.station = self.stats.station
        trace.stats.location = self.stats.location
        trace.stats.channel = self.stats.channel
        trace.stats.starttime = self.stats.starttime
        trace.stats.sampling_rate = self.stats.sampling_rate
        return trace


def _inventory_channel(
    code,
    *,
    location="10",
    rate=100.0,
    start="2026-01-01",
    end=None,
):
    return Channel(
        code=code,
        location_code=location,
        latitude=-40,
        longitude=175,
        elevation=0,
        depth=0,
        sample_rate=rate,
        start_date=UTCDateTime(start) if start else None,
        end_date=UTCDateTime(end) if end else None,
    )


def _inventory(*channels):
    station = Station(
        code="AAA",
        latitude=-40,
        longitude=175,
        elevation=0,
        site=Site(name="AAA"),
        channels=list(channels),
    )
    return Inventory([Network(code="NZ", stations=[station])], source="test")


def test_waveform_worker_reuses_authenticated_client_and_writes_mseed(tmp_path):
    client = Mock()
    client.get_waveforms.return_value = _Stream([_SacTrace("BHZ")])
    with patch.object(waveform, "_client", return_value=client) as factory:
        result = waveform._download_station(
            inventory.EARTHSCOPE_URL,
            "user",
            "password",
            tmp_path,
            "NZ",
            "AAA",
            "*",
            "BH?",
            [UTCDateTime("2026-01-01")],
            UTCDateTime("2026-01-02"),
            False,
            1,
        )
    factory.assert_called_once_with(inventory.EARTHSCOPE_URL, "user", "password")
    assert result.downloaded == 1
    assert result.files_written == 1
    assert (tmp_path / "NZ" / "AAA" / "2026" / "NZ.AAA.2026.001.mseed").is_file()


def test_waveform_worker_can_write_one_sac_per_channel(tmp_path):
    client = Mock()
    client.get_waveforms.return_value = _Stream([_SacTrace("BHZ"), _SacTrace("BHN")])
    with patch.object(waveform, "_client", return_value=client):
        result = waveform._download_station(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            tmp_path,
            "NZ",
            "AAA",
            "*",
            "BH?",
            [UTCDateTime("2026-01-01")],
            UTCDateTime("2026-01-02"),
            False,
            1,
            "sac",
        )
    assert result.downloaded == 1
    assert result.files_written == 2
    assert (
        tmp_path / "NZ" / "AAA" / "2026" / "NZ.AAA.10.BHZ.D.2026.001.000000.sac"
    ).is_file()


def test_download_rejects_trace_header_that_disagrees_with_request(tmp_path):
    day = UTCDateTime("2026-01-01")
    trace = _SacTrace("BHZ")
    trace.stats.station = "WRONG"

    with pytest.raises(ValueError, match="does not match request"):
        waveform._write_waveforms(
            _Stream([trace]),
            tmp_path,
            "NZ",
            "AAA",
            day,
            "sac",
            False,
        )


def test_invalid_waveform_format_is_rejected(tmp_path):
    with patch.object(waveform, "_client"):
        try:
            waveform.download_waveforms(
                tmp_path, "NZ", "2026-01-01", "2026-01-02", output_format="wav"
            )
        except ValueError as exc:
            assert "output_format" in str(exc)
        else:
            raise AssertionError("ValueError was not raised")


def test_existing_mseed_is_skipped_without_network_request(tmp_path):
    day = UTCDateTime("2026-01-01")
    destination = tmp_path / "NZ" / "AAA" / "2026" / "NZ.AAA.2026.001.mseed"
    destination.parent.mkdir(parents=True)
    _Stream([_SacTrace("BHZ")]).write(destination, "MSEED")

    with patch.object(waveform, "_fetch_waveforms") as fetch:
        result = waveform._download_day(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            tmp_path,
            "NZ",
            "AAA",
            "*",
            "BH?",
            day,
            day + 86400,
            False,
            1,
            "mseed",
            2,
            0,
        )

    fetch.assert_not_called()
    assert result.skipped == 1
    assert result.total == 1


def test_waveform_request_retries_transient_failure():
    client = Mock()
    client.get_waveforms.side_effect = [TimeoutError("temporary"), _Stream([object()])]

    with (
        patch.object(waveform, "_thread_client", return_value=client),
        patch.object(waveform.time, "sleep") as sleep,
        patch.object(waveform.random, "uniform", return_value=0),
    ):
        result = waveform._fetch_waveforms(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            "NZ",
            "AAA",
            "*",
            "BH?",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            2,
            0.5,
        )

    assert len(result) == 1
    assert client.get_waveforms.call_count == 2
    sleep.assert_called_once_with(0.5)


def test_waveform_request_retries_mseed_integrity_warning():
    client = Mock()
    journal = Mock()
    valid_stream = _Stream([object()])

    def corrupt_then_valid(*args, **kwargs):
        if client.get_waveforms.call_count == 1:
            warnings.warn(
                "data integrity check for Steim1 failed, last sample=-791353968, "
                "xn=-348",
                InternalMSEEDWarning,
                stacklevel=2,
            )
        return valid_stream

    client.get_waveforms.side_effect = corrupt_then_valid
    with (
        warnings.catch_warnings(),
        patch.object(waveform, "_thread_client", return_value=client),
        patch.object(waveform.time, "sleep") as sleep,
        patch.object(waveform.random, "uniform", return_value=0),
    ):
        warnings.simplefilter("error", InternalMSEEDWarning)
        result = waveform._fetch_waveforms(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            "NZ",
            "ABAZ",
            "11",
            "HHZ",
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
            2,
            0.5,
            journal,
        )

    assert result is valid_stream
    assert client.get_waveforms.call_count == 2
    sleep.assert_called_once_with(0.5)
    assert "retrying request" in journal.warning.call_args.args[0]


def test_persistent_mseed_integrity_warning_fails_without_output(tmp_path):
    client = Mock()

    def corrupt(*args, **kwargs):
        warnings.warn(
            "data integrity check for Steim1 failed, last sample=1347756467, xn=-597",
            InternalMSEEDWarning,
            stacklevel=2,
        )
        return _Stream([_SacTrace("HH1", location="12", sampling_rate=100.0)])

    client.get_waveforms.side_effect = corrupt
    task = waveform._InventoryTask(
        "NZ",
        "ABAZ",
        "12",
        "HH1",
        100.0,
        UTCDateTime("2026-01-01"),
        UTCDateTime("2026-01-02"),
    )
    with (
        warnings.catch_warnings(),
        patch.object(waveform, "_thread_client", return_value=client),
        patch.object(waveform.time, "sleep"),
        patch.object(waveform.random, "uniform", return_value=0),
    ):
        warnings.simplefilter("error", InternalMSEEDWarning)
        result = waveform._download_inventory_task(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            tmp_path,
            task,
            False,
            1,
            "mseed",
            2,
            0,
        )

    assert client.get_waveforms.call_count == 3
    assert result.failed == 1
    assert result.files_written == 0
    assert "InternalMSEEDWarning" in result.samples[0].error
    assert not list(tmp_path.rglob("*.mseed"))


def test_waveform_request_does_not_retry_permanent_fdsn_failure():
    client = Mock()
    client.get_waveforms.side_effect = FDSNForbiddenException("forbidden")

    with (
        patch.object(waveform, "_thread_client", return_value=client),
        patch.object(waveform.time, "sleep") as sleep,
    ):
        try:
            waveform._fetch_waveforms(
                inventory.EARTHSCOPE_URL,
                None,
                None,
                "NZ",
                "AAA",
                "*",
                "BH?",
                UTCDateTime("2026-01-01"),
                UTCDateTime("2026-01-02"),
                2,
                0.5,
            )
        except FDSNForbiddenException:
            pass
        else:
            raise AssertionError("FDSNForbiddenException was not raised")

    client.get_waveforms.assert_called_once()
    sleep.assert_not_called()


def test_existing_sac_files_allow_network_free_resume_without_metadata_files(tmp_path):
    day = UTCDateTime("2026-01-01")
    stream = _Stream([_SacTrace("BHZ"), _SacTrace("BHN")])
    written, skipped = waveform._write_waveforms(
        stream, tmp_path, "NZ", "AAA", day, "sac", False, "*", "BH?"
    )

    assert written == 2
    assert not skipped
    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BH?")
    assert not waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "HH?")
    assert not list(tmp_path.rglob("*.json"))


def test_sac_check_requires_each_explicit_channel(tmp_path):
    day = UTCDateTime("2026-01-01")
    waveform._write_waveforms(
        _Stream([_SacTrace("BHZ")]),
        tmp_path,
        "NZ",
        "AAA",
        day,
        "sac",
        False,
        "*",
        "BHZ",
    )

    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BHZ")
    assert not waveform._day_is_complete(
        tmp_path, "NZ", "AAA", day, "sac", "*", "BHZ,BHN"
    )


def test_incomplete_sac_day_downloads_only_missing_files(tmp_path):
    day = UTCDateTime("2026-01-01")
    existing = waveform._sac_destination(_SacTrace("BHZ"), tmp_path)
    existing.parent.mkdir(parents=True)
    _SacTrace("BHZ").write(existing, "SAC")

    written, skipped = waveform._write_waveforms(
        _Stream([_SacTrace("BHZ"), _SacTrace("BHN")]),
        tmp_path,
        "NZ",
        "AAA",
        day,
        "sac",
        False,
        "*",
        "BH?",
    )

    assert written == 1
    assert not skipped
    assert existing.is_file()
    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BH?")


def test_download_waveforms_aggregates_station_day_tasks(tmp_path):
    def completed(*args, **kwargs):
        return waveform._Counts(total=1, downloaded=1, files_written=1)

    with (
        patch.object(waveform, "_client"),
        patch.object(waveform, "_download_day", side_effect=completed) as worker,
    ):
        summary = waveform.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-03",
            station=["AAA"],
            max_workers=2,
        )

    assert worker.call_count == 2
    assert summary.total == 2
    assert summary.downloaded == 2
    assert summary.files_written == 2
    assert summary.status == "completed"
    assert summary.report_path.is_file()
    assert summary.log_path.is_file()
    assert json.loads(summary.report_path.read_text())["status"] == "completed"
    assert "progress=2/2" in summary.log_path.read_text()
    assert "completed duration_seconds=" in summary.log_path.read_text()
    assert summary.ok
    assert not replace(summary, no_data=1).ok


def test_interrupted_download_leaves_report_and_log(tmp_path):
    with (
        patch.object(waveform, "_client"),
        patch.object(waveform, "_download_day", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        waveform.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-02",
            station=["AAA"],
            max_workers=1,
        )

    reports = list((tmp_path / "logs" / "reports").glob("*.json"))
    logs = list((tmp_path / "logs").glob("*.log"))
    assert len(reports) == 1
    assert len(logs) == 1
    report = json.loads(reports[0].read_text())
    assert report["status"] == "interrupted"
    assert report["total"] == 1
    assert report["downloaded"] == 0
    assert "interrupted progress=0/1" in logs[0].read_text()


def test_waveform_run_artifacts_can_be_disabled(tmp_path):
    with (
        patch.object(waveform, "_client"),
        patch.object(
            waveform,
            "_download_day",
            return_value=waveform._Counts(total=1, downloaded=1, files_written=1),
        ),
    ):
        summary = waveform.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-02",
            station=["AAA"],
            max_workers=1,
            save_report=False,
            save_log=False,
        )

    assert summary.report_path is None
    assert summary.log_path is None
    assert not (tmp_path / "logs").exists()


def test_waveform_inventory_manifest_replaces_remote_station_lookup(tmp_path):
    manifest = Mock()
    guided_tasks = (
        waveform._InventoryTask(
            "NZ",
            "AAA",
            "10",
            "HHZ",
            100.0,
            UTCDateTime("2026-01-01"),
            UTCDateTime("2026-01-02"),
        ),
    )

    with (
        patch.object(waveform, "_client"),
        patch.object(waveform, "read_inventory", return_value=manifest) as read,
        patch.object(waveform, "_station_codes") as station_codes,
        patch.object(waveform, "_inventory_tasks", return_value=guided_tasks),
        patch.object(
            waveform,
            "_download_inventory_task",
            return_value=waveform._Counts(total=1, downloaded=1, files_written=1),
        ) as worker,
    ):
        summary = waveform.download_waveforms(
            tmp_path,
            "NZ",
            "2026-01-01",
            "2026-01-03",
            inventory=tmp_path / "stations.xml",
            max_workers=1,
        )

    read.assert_called_once_with(str(tmp_path / "stations.xml"), format="STATIONXML")
    station_codes.assert_not_called()
    assert worker.call_args.args[4] == guided_tasks[0]
    assert summary.total == 1


def test_inventory_tasks_are_exact_and_clip_channel_epochs_to_days():
    manifest = _inventory(
        _inventory_channel(
            "HHZ", location="10", start="2026-01-01T12:00:00", end="2026-01-03"
        ),
        _inventory_channel("HHN", location="10"),
        _inventory_channel("HHZ", location="11"),
    )

    tasks = waveform._inventory_tasks(
        manifest,
        "NZ",
        ["AAA"],
        "10",
        "HHZ",
        UTCDateTime("2026-01-01"),
        UTCDateTime("2026-01-04"),
    )

    assert [(task.location, task.channel) for task in tasks] == [
        ("10", "HHZ"),
        ("10", "HHZ"),
    ]
    assert tasks[0].starttime == UTCDateTime("2026-01-01T12:00:00")
    assert tasks[0].endtime == UTCDateTime("2026-01-02")
    assert tasks[1].starttime == UTCDateTime("2026-01-02")
    assert tasks[1].endtime == UTCDateTime("2026-01-03")


def test_inventory_guided_mseed_path_prevents_nslc_collisions(tmp_path):
    base = dict(
        network="NZ",
        station="AAA",
        sample_rate=100.0,
        starttime=UTCDateTime("2026-01-01"),
        endtime=UTCDateTime("2026-01-02"),
    )
    left = waveform._InventoryTask(location="10", channel="HHZ", **base)
    right = waveform._InventoryTask(location="11", channel="HHZ", **base)

    assert waveform._inventory_mseed_path(
        tmp_path, left
    ) != waveform._inventory_mseed_path(tmp_path, right)
    assert waveform._inventory_mseed_path(tmp_path, left).name == (
        "NZ.AAA.10.HHZ.2026.001.000000-2026002T000000.mseed"
    )


def test_inventory_worker_requests_and_validates_exact_xml_channel(tmp_path):
    task = waveform._InventoryTask(
        "NZ",
        "AAA",
        "10",
        "HHZ",
        100.0,
        UTCDateTime("2026-01-01"),
        UTCDateTime("2026-01-02"),
    )
    stream = _Stream([_SacTrace("HHZ", sampling_rate=100.0)])
    with patch.object(waveform, "_fetch_waveforms", return_value=stream) as fetch:
        result = waveform._download_inventory_task(
            inventory.EARTHSCOPE_URL,
            None,
            None,
            tmp_path,
            task,
            False,
            1,
            "mseed",
            2,
            1.0,
        )

    assert fetch.call_args.args[5:9] == (
        "10",
        "HHZ",
        task.starttime,
        task.endtime,
    )
    assert result.downloaded == 1
    assert result.files_written == 1
    assert waveform._inventory_mseed_path(tmp_path, task).is_file()
