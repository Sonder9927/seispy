"""Waveform integrity-policy contracts."""

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

from seispy.waveform.integrity import (
    merge_contiguous_segments,
    merge_short_gaps,
    unusable_sample_reason,
)


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


def test_contiguous_only_merge_preserves_short_and_long_gaps():
    for gap_samples in (1, 50, 101):
        stream = merge_contiguous_segments(_segments(gap_samples))
        assert len(stream) == 2
        assert len(stream.get_gaps()) == 1


def _overlapping_segments(*, identical):
    first = Trace(np.arange(100, dtype=np.int32))
    first.stats.network = "NZ"
    first.stats.station = "AAA"
    first.stats.channel = "HHZ"
    first.stats.sampling_rate = 100.0
    first.stats.starttime = UTCDateTime("2026-01-01")
    start = 90 if identical else 0
    second = Trace(np.arange(start, start + 100, dtype=np.int32))
    second.stats.update(first.stats)
    second.stats.starttime = first.stats.starttime + 0.9
    return Stream([first, second])


def test_identical_overlap_is_deduplicated_and_merged():
    stream = merge_contiguous_segments(_overlapping_segments(identical=True))

    assert len(stream) == 1
    np.testing.assert_array_equal(stream[0].data, np.arange(190))


def test_conflicting_overlap_remains_segmented():
    stream = merge_contiguous_segments(_overlapping_segments(identical=False))

    assert len(stream) == 2
    assert stream.get_gaps()[0][6] < 0


def test_unusable_sample_reason_classifies_arrays():
    assert unusable_sample_reason(np.arange(3, dtype=np.int32)) is None
    assert unusable_sample_reason(np.full(3, 7, dtype=np.int32)) == "is constant"
    assert (
        unusable_sample_reason(np.array([1.0, np.nan]))
        == "contains NaN or infinite samples"
    )
    assert (
        unusable_sample_reason(np.array([], dtype=np.float64)) == "contains no samples"
    )
