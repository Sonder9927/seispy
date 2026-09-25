"""Waveform integrity policies shared by waveform-consuming workflows."""

import numpy as np

DEFAULT_MAX_GAP_SECONDS = 1.0


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
