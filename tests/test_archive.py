from types import SimpleNamespace

import numpy as np
from obspy import Trace, UTCDateTime, read

from seispy.archive import (
    WaveformIdentity,
    channel_mseed_path,
    matches_mseed_path,
    preserve_sac_quality,
)


def test_channel_mseed_path_is_compact_for_full_days_and_unique_for_partials(tmp_path):
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

    assert full_day.name == "11.HHN.237.mseed"
    assert partial.name == "11.HHN.237.120000-2023238T000000.mseed"

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
            mseed=SimpleNamespace(dataquality="D"),
        )
    )


def test_sac_path_uses_header_identity_and_flattens_julian_day(tmp_path):
    identity = WaveformIdentity.from_trace(_trace())

    path = identity.sac_path(tmp_path)

    assert path == (
        tmp_path / "NZ" / "WEL" / "2025" / "NZ.WEL.10.BHZ.D.2025.001.010203.sac"
    )
    assert identity.matches_sac_path(path, tmp_path)


def test_mass_downloader_channel_chunk_path_matches_headers(tmp_path):
    stream = [_trace(channel="BHZ")]
    path = tmp_path / "NZ" / "WEL" / "2025" / "10.BHZ.001.010203-2025002T000000.mseed"

    assert matches_mseed_path(path, tmp_path, stream)


def test_miniseed_quality_survives_sac_round_trip(tmp_path):
    trace = Trace(data=np.arange(4, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = "WEL"
    trace.stats.location = "10"
    trace.stats.channel = "BHZ"
    trace.stats.starttime = UTCDateTime("2025-01-01")
    trace.stats.mseed = {"dataquality": "M"}
    preserve_sac_quality(trace)
    destination = WaveformIdentity.from_trace(trace).sac_path(tmp_path)
    destination.parent.mkdir(parents=True)

    trace.write(str(destination), format="SAC")
    restored = read(destination, headonly=True)[0]

    assert WaveformIdentity.from_trace(restored).quality == "M"
    assert WaveformIdentity.from_trace(restored).matches_sac_path(destination, tmp_path)


def test_preserving_quality_does_not_overwrite_existing_user_header():
    trace = _trace()
    trace.stats.sac = {"kuser0": "project"}

    preserve_sac_quality(trace)

    assert trace.stats.sac["kuser0"] == "project"
    assert trace.stats.sac["kuser1"] == "SQ:D"
