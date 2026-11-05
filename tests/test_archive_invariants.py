"""Cross-workflow archive invariant contracts."""

from dataclasses import replace
from datetime import date, datetime, time, timedelta, timezone
from types import SimpleNamespace

import pytest
from hypothesis import given, strategies as st
from obspy import UTCDateTime

from seispy.archive import (
    WaveformIdentity,
    stream_day_identity,
)


_CODE_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
_CODE = st.text(alphabet=_CODE_ALPHABET, min_size=1, max_size=5)
_LOCATION = st.one_of(st.just(""), _CODE)
_DATE = st.one_of(
    st.sampled_from(
        (
            date(2000, 2, 29),
            date(2024, 12, 31),
            date(2025, 1, 1),
            date(2032, 2, 29),
        )
    ),
    st.dates(min_value=date(2000, 1, 1), max_value=date(2035, 12, 31)),
)


def _trace(identity: WaveformIdentity):
    return SimpleNamespace(
        stats=SimpleNamespace(
            network=identity.network,
            station=identity.station,
            location=identity.location,
            channel=identity.channel,
            starttime=identity.starttime,
        )
    )


def _empty_trace(identity: WaveformIdentity):
    trace = _trace(identity)
    trace.stats.npts = 0
    return trace


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
        starttime=starttime,
    )


@given(
    identity=_identities(),
    channels=st.lists(_CODE, min_size=1, max_size=6, unique=True),
)
def test_miniseed_identity_accepts_same_station_day_in_any_channel(identity, channels):
    stream = [
        _trace(
            WaveformIdentity(
                identity.network,
                identity.station,
                identity.location,
                channel,
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
    for changed in (
        replace(identity, station=identity.station + "X"),
        replace(identity, starttime=identity.starttime + 86400),
    ):
        with pytest.raises(ValueError, match="multiple stations or start days"):
            stream_day_identity([_trace(identity), _trace(changed)])
    boundary = replace(identity, starttime=identity.starttime + 86400)
    assert stream_day_identity([*stream, _empty_trace(boundary)]) == expected
    with pytest.raises(ValueError, match="contains no samples"):
        stream_day_identity([_empty_trace(identity)])
