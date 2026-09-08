from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from obspy import UTCDateTime

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
    inv.write.assert_called_once_with(str(tmp_path / "inventory.xml"), format="STATIONXML")


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
