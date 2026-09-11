from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from hypothesis import given, strategies as st
from obspy import UTCDateTime

from seispy._archive import (
    WaveformIdentity,
    matches_mseed_path,
    mseed_path,
    stream_day_identity,
)
from seispy.event._archive_index import WaveformArchiveIndex, WaveformRecord


_CODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
_CODE = st.text(alphabet=_CODE_ALPHABET, min_size=1, max_size=5)
_LOCATION = st.one_of(st.just(""), _CODE)
_QUALITY = st.sampled_from(("D", "M", "Q", "R"))
_DATE = st.dates(min_value=date(2000, 1, 1), max_value=date(2035, 12, 30))


def _trace(identity: WaveformIdentity):
    return SimpleNamespace(
        stats=SimpleNamespace(
            network=identity.network,
            station=identity.station,
            location=identity.location,
            channel=identity.channel,
            starttime=identity.starttime,
            mseed=SimpleNamespace(dataquality=identity.quality),
        )
    )


@st.composite
def _identities(draw):
    day = draw(_DATE)
    second = draw(st.integers(min_value=0, max_value=86_399))
    starttime = UTCDateTime(
        datetime.combine(day, time(), tzinfo=timezone.utc) + timedelta(seconds=second)
    )
    return WaveformIdentity(
        network=draw(_CODE),
        station=draw(_CODE),
        location=draw(_LOCATION),
        channel=draw(_CODE),
        quality=draw(_QUALITY),
        starttime=starttime,
    )


@given(identity=_identities(), merged=st.booleans())
def test_sac_archive_path_round_trips_header_identity(identity, merged):
    root = Path("archive")
    path = identity.sac_path(root, merged=merged)
    suffix = "merged" if merged else identity.starttime.strftime("%H%M%S")
    expected_name = (
        f"{identity.network}.{identity.station}.{identity.location}."
        f"{identity.channel}.{identity.quality}.{identity.year}."
        f"{identity.julday:03d}.{suffix}.sac"
    )

    assert identity.day == identity.starttime.date
    assert path.parent == root / identity.network / identity.station / str(
        identity.year
    )
    assert path.name == expected_name
    assert identity.matches_sac_path(path, root)


@given(identity=_identities())
def test_sac_archive_path_rejects_wrong_station(identity):
    root = Path("archive")
    path = identity.sac_path(root)
    wrong_path = path.with_name(f"WRONG-{path.name}")

    assert not identity.matches_sac_path(wrong_path, root)


@given(
    identity=_identities(),
    channels=st.lists(_CODE, min_size=1, max_size=6, unique=True),
)
def test_miniseed_identity_accepts_same_station_day_in_any_channel(identity, channels):
    root = Path("archive")
    stream = [
        _trace(
            WaveformIdentity(
                identity.network,
                identity.station,
                identity.location,
                channel,
                identity.quality,
                identity.starttime,
            )
        )
        for channel in reversed(channels)
    ]
    expected = (
        identity.network,
        identity.station,
        identity.year,
        identity.julday,
    )

    assert stream_day_identity(stream) == expected
    assert mseed_path(root, stream) == (
        root
        / identity.network
        / identity.station
        / str(identity.year)
        / f"{identity.network}.{identity.station}.{identity.year}."
        f"{identity.julday:03d}.mseed"
    )
    assert matches_mseed_path(mseed_path(root, stream), root, stream)


@given(identity=_identities(), change_station=st.booleans())
def test_miniseed_identity_rejects_mixed_station_or_day(identity, change_station):
    other = WaveformIdentity(
        identity.network,
        f"{identity.station}X" if change_station else identity.station,
        identity.location,
        identity.channel,
        identity.quality,
        identity.starttime if change_station else identity.starttime + 86_400,
    )

    with pytest.raises(ValueError, match="multiple stations or start days"):
        stream_day_identity([_trace(identity), _trace(other)])


@given(
    base_day=_DATE,
    record_offset=st.integers(min_value=-172_800, max_value=172_800),
    record_duration=st.integers(min_value=0, max_value=259_200),
    query_offset=st.integers(min_value=-172_800, max_value=172_800),
    query_duration=st.integers(min_value=0, max_value=172_800),
)
def test_archive_index_returns_each_overlapping_record_once(
    base_day,
    record_offset,
    record_duration,
    query_offset,
    query_duration,
):
    origin = UTCDateTime(datetime.combine(base_day, time(), tzinfo=timezone.utc))
    record_start = origin + record_offset
    record_end = record_start + record_duration
    query_start = origin + query_offset
    query_end = query_start + query_duration
    identity = WaveformIdentity("NZ", "WEL", "10", "BHZ", "D", record_start)
    record = WaveformRecord(Path("record.sac"), identity, record_start, record_end)
    buckets = defaultdict(list)
    current = record_start.date
    while current <= record_end.date:
        buckets[(identity.station, current)].append(record)
        current += timedelta(days=1)
    index = WaveformArchiveIndex(
        {key: tuple(records) for key, records in buckets.items()}
    )

    result = index.overlapping(identity.station, query_start, query_end)
    overlaps = record_start <= query_end and record_end >= query_start

    assert result == ((record,) if overlaps else ())
