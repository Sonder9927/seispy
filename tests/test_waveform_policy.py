import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

from seispy._waveform import merge_short_gaps


def _segments(gap_samples):
    first = Trace(np.arange(100, dtype=np.float32))
    first.stats.sampling_rate = 100.0
    first.stats.starttime = UTCDateTime("2026-01-01")
    second = first.copy()
    second.stats.starttime = first.stats.endtime + (gap_samples + 1) * first.stats.delta
    return Stream([first, second])


def test_short_gap_is_interpolated():
    stream = merge_short_gaps(_segments(50))
    assert len(stream) == 1


def test_gap_longer_than_one_second_is_rejected():
    with pytest.raises(ValueError, match="longer than 1 s"):
        merge_short_gaps(_segments(101))
