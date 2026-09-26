from datetime import date
from types import SimpleNamespace

from obspy import UTCDateTime

from seispy.archive import (
    WaveformIdentity,
    channel_mseed_path,
    coverage_day,
    matches_mseed_path,
    start_day_token,
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


def _sampled_stats(starttime, *, npts, sampling_rate):
    return SimpleNamespace(
        network="NZ",
        station="WEL",
        location="10",
        channel="BHZ",
        starttime=UTCDateTime(starttime),
        npts=npts,
        sampling_rate=sampling_rate,
    )


def test_coverage_day_follows_the_sample_majority_across_midnight():
    assert coverage_day(
        _sampled_stats("2026-01-01T23:59:57", npts=1000, sampling_rate=100)
    ) == date(2026, 1, 2)
    assert coverage_day(
        _sampled_stats("2026-01-02T23:59:00", npts=10000, sampling_rate=100)
    ) == date(2026, 1, 2)
    assert coverage_day(
        _sampled_stats("2026-01-01T23:59:57", npts=0, sampling_rate=100)
    ) == date(2026, 1, 1)
    assert coverage_day(
        _sampled_stats("2026-01-01T23:59:57", npts=1000, sampling_rate=0)
    ) == date(2026, 1, 1)


def test_identity_archive_day_drives_the_day_slot():
    identity = WaveformIdentity.from_trace(
        SimpleNamespace(
            stats=_sampled_stats("2026-01-01T23:59:57", npts=1000, sampling_rate=100)
        )
    )

    assert identity.archive_day == date(2026, 1, 2)
    assert (identity.year, identity.julday, identity.day) == (
        2026,
        2,
        date(2026, 1, 2),
    )
    assert identity.day_key[4:] == (2026, 2)


def test_channel_mseed_path_honours_explicit_archive_day(tmp_path):
    path = channel_mseed_path(
        tmp_path,
        "NZ",
        "ABAZ",
        "11",
        "HHN",
        UTCDateTime("2026-01-01T23:59:57"),
        UTCDateTime("2026-01-02T00:00:07"),
        archive_day=date(2026, 1, 2),
    )

    assert path == (
        tmp_path / "NZ" / "ABAZ" / "2026" / "NZ.ABAZ.11.HHN.2026.002.001T235957.mseed"
    )


def test_paths_mark_the_start_day_when_it_differs_from_the_archive_day(tmp_path):
    stats = _sampled_stats("2026-01-01T23:59:57", npts=1000, sampling_rate=100)
    identity = WaveformIdentity.from_trace(SimpleNamespace(stats=stats))

    assert identity.day == date(2026, 1, 2)
    assert identity.sac_path(tmp_path).name == "NZ.WEL.10.BHZ.2026.002.001T235957.sac"
    assert start_day_token(stats.starttime, identity.day) == "001T"
    assert start_day_token(UTCDateTime("2026-01-02T00:00:00"), date(2026, 1, 2)) == ""

    mseed = channel_mseed_path(
        tmp_path,
        "NZ",
        "WEL",
        "10",
        "BHZ",
        stats.starttime,
        stats.starttime + 1,
        archive_day=identity.day,
    )
    assert mseed.name == "NZ.WEL.10.BHZ.2026.002.001T235957.mseed"
    assert matches_mseed_path(mseed, tmp_path, [SimpleNamespace(stats=stats)])
