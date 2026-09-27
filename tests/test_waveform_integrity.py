"""Waveform integrity-policy contracts."""

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime

from seispy.waveform.integrity import (
    TraceFilter,
    merge_contiguous_segments,
    merge_short_gaps,
    trace_header_rejection_reason,
    trace_rejection_reason,
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


def _filter_trace(*, npts=1000, sampling_rate=100.0, data=None):
    trace = Trace(np.arange(npts, dtype=np.int32) if data is None else data)
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = sampling_rate
    return trace


def test_trace_header_filter_rejects_single_sample_trace():
    reason = trace_header_rejection_reason(_filter_trace(npts=1), TraceFilter())

    assert reason == "has 1 sample(s)"


def test_trace_header_filter_rejects_short_duration():
    reason = trace_header_rejection_reason(_filter_trace(npts=500), TraceFilter())

    assert reason is not None and "spans" in reason


def test_trace_header_filter_rejects_too_few_samples():
    policy = TraceFilter(min_duration_seconds=0.0)

    reason = trace_header_rejection_reason(_filter_trace(npts=50), policy)

    assert reason is not None and "samples" in reason


def test_trace_header_filter_min_periods_uses_low_frequency():
    policy = TraceFilter(min_duration_seconds=0.0, min_samples=0, min_periods=3.0)

    assert (
        trace_header_rejection_reason(
            _filter_trace(npts=1000, sampling_rate=1.0), policy, low_frequency=0.004
        )
        is None
    )
    assert (
        trace_header_rejection_reason(
            _filter_trace(npts=100, sampling_rate=1.0), policy, low_frequency=0.004
        )
        is not None
    )


def test_trace_rejection_reason_rejects_unusable_samples():
    policy = TraceFilter(min_duration_seconds=0.0, min_samples=0)

    constant = _filter_trace(data=np.full(1000, 7, dtype=np.int32))
    non_finite = _filter_trace(data=np.full(1000, np.nan, dtype=np.float64))

    assert trace_rejection_reason(constant, policy) == "is constant"
    assert "NaN" in trace_rejection_reason(non_finite, policy)
    assert trace_rejection_reason(_filter_trace(), policy) is None


def test_trace_rejection_reason_rejects_tolerant_flatline():
    policy = TraceFilter(
        min_duration_seconds=0.0,
        min_samples=0,
        reject_unusable_samples=False,
        max_flatline_amplitude=2.0,
    )

    jittering = _filter_trace(data=np.resize(np.array([5, 6, 4, 5, 6]), 1000))
    live = _filter_trace(data=np.arange(1000, dtype=np.int32))

    assert "flat-line" in trace_rejection_reason(jittering, policy)
    assert trace_rejection_reason(live, policy) is None


def test_trace_filter_rejects_negative_flatline_threshold():
    with pytest.raises(ValueError, match="max_flatline_amplitude"):
        TraceFilter(max_flatline_amplitude=-1.0)
