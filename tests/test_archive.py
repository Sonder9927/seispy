from datetime import date
from types import SimpleNamespace

from obspy import UTCDateTime

from seispy.archive import (
    coverage_day,
)


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


def test_archive_day():
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
