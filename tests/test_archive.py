from types import SimpleNamespace

from obspy import UTCDateTime

from seispy.archive import (
    WaveformIdentity,
    channel_mseed_path,
    matches_mseed_path,
)


def test_channel_mseed_path_is_self_describing_and_unique_for_partials(tmp_path):
    full_day = channel_mseed_path(
        tmp_path,
        "NZ",
        "ABAZ",
        "11",
        "HHN",
        UTCDateTime("2023-08-25"),
        UTCDateTime("2023-08-26"),
    )
    partial = channel_mseed_path(
        tmp_path,
        "NZ",
        "ABAZ",
        "11",
        "HHN",
        UTCDateTime("2023-08-25T12:00:00"),
        UTCDateTime("2023-08-26"),
    )

    assert full_day.name == "NZ.ABAZ.11.HHN.2023.237.mseed"
    assert partial.name == "NZ.ABAZ.11.HHN.2023.237.120000.mseed"
    assert "180000" not in partial.name

    fractional_a = channel_mseed_path(
        tmp_path,
        "NZ",
        "ABAZ",
        "11",
        "HHN",
        UTCDateTime("2023-08-25T12:00:00.100000"),
        UTCDateTime("2023-08-25T18:00:00"),
    )
    fractional_b = channel_mseed_path(
        tmp_path,
        "NZ",
        "ABAZ",
        "11",
        "HHN",
        UTCDateTime("2023-08-25T12:00:00.200000"),
        UTCDateTime("2023-08-25T18:00:00"),
    )
    assert fractional_a != fractional_b


def _trace(
    *,
    network="NZ",
    station="WEL",
    location="10",
    channel="BHZ",
    starttime="2025-01-01T01:02:03",
):
    return SimpleNamespace(
        stats=SimpleNamespace(
            network=network,
            station=station,
            location=location,
            channel=channel,
            starttime=UTCDateTime(starttime),
        )
    )


def test_sac_path_uses_header_identity_and_flattens_julian_day(tmp_path):
    identity = WaveformIdentity.from_trace(_trace())

    path = identity.sac_path(tmp_path)

    assert path == (
        tmp_path / "NZ" / "WEL" / "2025" / "NZ.WEL.10.BHZ.2025.001.010203.sac"
    )
    assert identity.matches_sac_path(path, tmp_path)


def test_mass_downloader_channel_chunk_path_matches_headers(tmp_path):
    stream = [_trace(channel="BHZ")]
    path = tmp_path / "NZ" / "WEL" / "2025" / "NZ.WEL.10.BHZ.2025.001.010203.mseed"

    assert matches_mseed_path(path, tmp_path, stream)
