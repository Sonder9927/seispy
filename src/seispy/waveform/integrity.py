"""Waveform integrity policies shared by waveform-consuming workflows."""

import math
from dataclasses import dataclass

import numpy as np

DEFAULT_MAX_GAP_SECONDS = 1.0
DEFAULT_MIN_DURATION_SECONDS = 10.0
DEFAULT_MIN_SAMPLES = 100


@dataclass(frozen=True)
class TraceFilter:
    """Thresholds deciding whether a trace can be processed meaningfully.

    A trace is rejected when it is too short, carries too few samples, has an
    invalid sampling rate, or (when samples are loaded) is empty, constant, or
    non-finite. Filtering is a deliberate skip, never a failure.

    Attributes:
        min_duration_seconds: Minimum covered duration.
        min_samples: Minimum number of samples.
        min_periods: Optional minimum number of periods at the lowest response
            frequency, enforced against low_frequency when it is supplied.
        reject_unusable_samples: Reject empty, constant, and non-finite traces.
        max_flatline_amplitude: Optional peak-to-peak amplitude at or below which
            a trace counts as flat-line. This catches stuck channels that jitter
            instead of being exactly constant. None disables the check.
    """

    min_duration_seconds: float = DEFAULT_MIN_DURATION_SECONDS
    min_samples: int = DEFAULT_MIN_SAMPLES
    min_periods: float | None = None
    reject_unusable_samples: bool = True
    max_flatline_amplitude: float | None = None

    def __post_init__(self):
        if self.max_flatline_amplitude is not None and self.max_flatline_amplitude < 0:
            raise ValueError("max_flatline_amplitude must be non-negative or None")


DEFAULT_TRACE_FILTER = TraceFilter()


def unusable_sample_reason(data) -> str | None:
    """Return why a sample array cannot be trusted, or None when usable.

    Empty, non-finite, and constant arrays are rejected: they either carry no
    samples or no recoverable signal. Callers prefix the reason with their own
    subject, such as "deconvolved output" or "trace".
    """
    if np.size(data) == 0:
        return "contains no samples"
    if not np.isfinite(data).all():
        return "contains NaN or infinite samples"
    if np.all(data == data[0]):
        return "is constant"
    return None


def trace_header_rejection_reason(
    trace, policy: TraceFilter, *, low_frequency: float | None = None
) -> str | None:
    """Return why a trace's headers make it unusable, or None.

    Only header fields are consulted, so the check also works for head-only
    reads that decoded no samples.
    """
    stats = trace.stats
    npts = int(getattr(stats, "npts", 0) or 0)
    if npts <= 1:
        return f"has {npts} sample(s)"
    rate = float(getattr(stats, "sampling_rate", 0.0) or 0.0)
    if not math.isfinite(rate) or rate <= 0.0:
        return f"has sampling rate {rate:g}"
    minimum = policy.min_duration_seconds
    if policy.min_periods is not None and low_frequency and low_frequency > 0:
        minimum = max(minimum, policy.min_periods / low_frequency)
    duration = npts / rate
    if duration < minimum:
        return f"spans {duration:g}s (< {minimum:g}s)"
    if npts < policy.min_samples:
        return f"has {npts} samples (< {policy.min_samples})"
    return None


def trace_rejection_reason(
    trace, policy: TraceFilter, *, low_frequency: float | None = None
) -> str | None:
    """Return why a trace cannot be processed, or None when it is usable."""
    reason = trace_header_rejection_reason(trace, policy, low_frequency=low_frequency)
    if reason is not None:
        return reason
    if policy.reject_unusable_samples:
        reason = unusable_sample_reason(trace.data)
        if reason is not None:
            return reason
    limit = policy.max_flatline_amplitude
    if limit is not None and np.size(trace.data):
        values = np.asarray(trace.data)
        if np.isfinite(values).all():
            amplitude = float(np.ptp(values))
            if amplitude <= limit:
                return f"is flat-line (peak-to-peak {amplitude:g})"
    return None


def split_traces_by_filter(
    stream, policy: TraceFilter, *, low_frequency: float | None = None
) -> tuple[list, list]:
    """Split a stream into kept traces and rejected (trace, reason) pairs.

    Header checks always run; sample checks run when the policy asks for them and
    the traces carry data. Filtering is a deliberate skip, never a failure.
    """
    kept = []
    rejected = []
    for trace in stream:
        reason = trace_rejection_reason(trace, policy, low_frequency=low_frequency)
        if reason is None:
            kept.append(trace)
        else:
            rejected.append((trace, reason))
    return kept, rejected


def merge_contiguous_segments(stream):
    """Merge losslessly compatible traces while preserving gaps and conflicts.

    Directly contiguous traces and sample-identical overlaps are merged. Every
    gap and every overlap containing different samples remains segmented.
    """
    return stream.merge(method=-1)


def merge_short_gaps(stream, max_gap_seconds=DEFAULT_MAX_GAP_SECONDS):
    """Merge a stream while refusing to fabricate data across long gaps."""
    if max_gap_seconds < 0:
        raise ValueError("max_gap_seconds cannot be negative")
    long_gaps = [gap for gap in stream.get_gaps() if gap[6] > max_gap_seconds]
    if long_gaps:
        largest = max(gap[6] for gap in long_gaps)
        raise ValueError(
            f"waveform contains {len(long_gaps)} gap(s) longer than "
            f"{max_gap_seconds:g} s; largest is {largest:g} s"
        )
    return stream.merge(method=1, fill_value="interpolate")
