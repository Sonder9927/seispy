from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from obspy import UTCDateTime
from obspy.clients.fdsn.header import FDSNForbiddenException

from seispy.download import events, inventory, waveform


def test_earthscope_client_receives_restricted_credentials():
    with patch.object(inventory, "Client") as client:
        inventory._client(inventory.EARTHSCOPE_URL, "user", "password")
    client.assert_called_once_with(
        "https://service.earthscope.org", user="user", password="password"
    )


def test_inventory_download_has_no_logging_and_writes_stationxml(tmp_path):
    inv = Mock()
    client = Mock()
    client.get_stations.return_value = inv
    with patch.object(inventory, "_client", return_value=client):
        result = inventory.download_inventory(
            tmp_path / "inventory.xml", network="NZ", station="AAA"
        )
    assert result is inv
    inv.write.assert_called_once_with(
        str(tmp_path / "inventory.xml"), format="STATIONXML"
    )


def test_earthquake_events_are_returned_as_dataframe(tmp_path):
    origin = SimpleNamespace(
        time=UTCDateTime("2026-01-01"),
        longitude=1,
        latitude=2,
        depth=3000,
    )
    magnitude = SimpleNamespace(mag=4.5, magnitude_type="Mw")
    event = SimpleNamespace(
        preferred_origin=lambda: origin,
        preferred_magnitude=lambda: magnitude,
        origins=[origin],
        magnitudes=[magnitude],
    )
    client = Mock()
    client.get_events.return_value = [event]
    with patch.object(events, "Client", return_value=client):
        frame = events.download_earthquake_events(
            "2026-01-01", "2026-01-02", tmp_path / "events.csv"
        )
    assert len(frame) == 1
    assert frame.loc[0, "depth"] == 3
    assert (tmp_path / "events.csv").exists()


class _Stream(list):
    def write(self, filename, format):
        assert format == "MSEED"
        Path(filename).write_bytes(b"waveform")

    def merge(self, **kwargs):
        return self


class _SacTrace:
    def __init__(self, channel):
        self.stats = SimpleNamespace(
            network="NZ",
            station="AAA",
            location="10",
            channel=channel,
            starttime=UTCDateTime("2026-01-01"),
            mseed=SimpleNamespace(dataquality="D"),
        )

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(b"sac")


def test_waveform_worker_reuses_authenticated_client_and_writes_mseed(tmp_path):
    client = Mock()
    client.get_waveforms.return_value = _Stream([object()])
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
    assert len(list(tmp_path.rglob("*.mseed"))) == 1


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
    assert len(list(tmp_path.rglob("*.sac"))) == 2


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
    destination = tmp_path / "NZ" / "AAA" / "2026" / "001" / "NZ.AAA.2026.001.mseed"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")

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
    directory = tmp_path / "NZ" / "AAA" / "2026" / "001"
    existing = waveform._sac_destination(_SacTrace("BHZ"), directory)
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"existing")

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
    assert existing.read_bytes() == b"existing"
    assert waveform._day_is_complete(tmp_path, "NZ", "AAA", day, "sac", "*", "BH?")


def test_download_waveforms_aggregates_station_day_tasks(tmp_path):
    def completed(*args, **kwargs):
        return waveform._Counts(total=1, downloaded=1, files_written=1)

    with (
        patch.object(waveform, "_client"),
        patch.object(waveform, "get_logger", return_value=Mock()),
        patch.object(waveform, "_download_day", side_effect=completed) as worker,
        patch.object(
            waveform, "auto_save_report", side_effect=lambda report, *args: report
        ),
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
