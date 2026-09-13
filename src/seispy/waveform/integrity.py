"""Waveform integrity policies shared by waveform-consuming workflows."""

DEFAULT_MAX_GAP_SECONDS = 1.0


def merge_contiguous_segments(stream):
    """Merge only directly contiguous compatible traces, preserving every gap."""
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
