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
)


def _segments(gap_samples):
    first = Trace(np.arange(100, dtype=np.float32))
    first.stats.sampling_rate = 100.0
    first.stats.starttime = UTCDateTime("2026-01-01")
    second = first.copy()
    second.stats.starttime = first.stats.endtime + (gap_samples + 1) * first.stats.delta
    return Stream([first, second])


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


def _filter_trace(*, npts=1000, sampling_rate=100.0, data=None):
    trace = Trace(np.arange(npts, dtype=np.int32) if data is None else data)
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.channel = "HHZ"
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = sampling_rate
    return trace


@pytest.mark.parametrize("gap", [1, 50, 101])
def test_gap_policy(gap):
    preserved = merge_contiguous_segments(_segments(gap))
    assert len(preserved) == 2 and len(preserved.get_gaps()) == 1
    if gap > 100:
        with pytest.raises(ValueError, match="longer than 1 s"):
            merge_short_gaps(_segments(gap))
    else:
        assert len(merge_short_gaps(_segments(gap))) == 1


@pytest.mark.parametrize("identical", [True, False])
def test_overlap_policy(identical):
    stream = merge_contiguous_segments(_overlapping_segments(identical=identical))
    assert len(stream) == (1 if identical else 2)
    if identical:
        np.testing.assert_array_equal(stream[0].data, np.arange(190))
    else:
        assert stream.get_gaps()[0][6] < 0


@pytest.mark.parametrize(
    "data,threshold,error",
    [
        (np.arange(1000), None, None),
        (np.full(1000, 7), None, "constant"),
        (np.full(1000, np.nan), None, "NaN"),
        (np.array([], dtype=float), None, "0 sample"),
        (np.resize(np.array([5, 6, 4, 5, 6]), 1000), 2.0, "flat-line"),
        (np.arange(1000), 2.0, None),
    ],
)
def test_trace_quality(data, threshold, error):
    policy = TraceFilter(
        min_duration_seconds=0.0, min_samples=0, max_flatline_amplitude=threshold
    )
    reason = trace_rejection_reason(_filter_trace(data=data), policy)
    if error is None:
        assert reason is None
    else:
        assert reason is not None and error in reason
    with pytest.raises(ValueError, match="max_flatline_amplitude"):
        TraceFilter(max_flatline_amplitude=-1.0)


@pytest.mark.parametrize(
    "npts,rate,options,frequency,error",
    [
        (1, 100.0, {}, None, "1 sample"),
        (500, 100.0, {}, None, "spans"),
        (50, 100.0, dict(min_duration_seconds=0.0), None, "samples"),
        (
            1000,
            1.0,
            dict(min_duration_seconds=0.0, min_samples=0, min_periods=3.0),
            0.004,
            None,
        ),
        (
            100,
            1.0,
            dict(min_duration_seconds=0.0, min_samples=0, min_periods=3.0),
            0.004,
            "spans",
        ),
    ],
)
def test_trace_header_limits(npts, rate, options, frequency, error):
    reason = trace_header_rejection_reason(
        _filter_trace(npts=npts, sampling_rate=rate),
        TraceFilter(**options),
        low_frequency=frequency,
    )
    if error is None:
        assert reason is None
    else:
        assert reason is not None and error in reason
