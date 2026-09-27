import numpy as np
from obspy import Stream, Trace, UTCDateTime, read

from seispy import waveform
from seispy.waveform import TraceFilter


def _trace(*, npts=1000, start="2026-01-01", sampling_rate=100.0):
    trace = Trace(np.arange(npts, dtype=np.int32))
    trace.stats.network = "NZ"
    trace.stats.station = "AAA"
    trace.stats.location = "10"
    trace.stats.channel = "BHZ"
    trace.stats.starttime = UTCDateTime(start)
    trace.stats.sampling_rate = sampling_rate
    return trace


def test_filter_waveforms_keeps_long_and_drops_short_mseed(tmp_path):
    source = tmp_path / "src"
    target = source / "NZ" / "AAA" / "day.mseed"
    target.parent.mkdir(parents=True)
    Stream([_trace(npts=100), _trace(npts=1000, start="2026-01-01T01:00:00")]).write(
        target, format="MSEED"
    )
    output = tmp_path / "out"

    summary = waveform.filter_waveforms(
        source, output, trace_filter=TraceFilter(min_duration_seconds=5)
    )

    assert summary.succeeded == 1
    assert summary.traces_total == 2
    assert summary.traces_written == 1
    assert summary.traces_filtered == 1
    kept = read(output / "NZ" / "AAA" / "day.mseed")
    assert len(kept) == 1
    assert kept[0].stats.npts == 1000


def test_filter_waveforms_skips_file_when_all_traces_filtered(tmp_path):
    source = tmp_path / "src"
    target = source / "NZ" / "AAA" / "day.mseed"
    target.parent.mkdir(parents=True)
    Stream([_trace(npts=100)]).write(target, format="MSEED")
    output = tmp_path / "out"

    summary = waveform.filter_waveforms(
        source, output, trace_filter=TraceFilter(min_duration_seconds=60)
    )

    assert summary.skipped == 1
    assert summary.succeeded == 0
    assert summary.traces_filtered == 1
    assert not list(output.rglob("*.mseed"))


def test_filter_waveforms_handles_sac_sources(tmp_path):
    source = tmp_path / "src"
    target = source / "trace.sac"
    target.parent.mkdir(parents=True)
    _trace(npts=1000).write(str(target), format="SAC")
    output = tmp_path / "out"

    summary = waveform.filter_waveforms(
        source, output, trace_filter=TraceFilter(min_duration_seconds=5)
    )

    assert summary.succeeded == 1
    assert summary.traces_written == 1
    assert (output / "trace.sac").is_file()


def test_filter_waveforms_reports_unreadable_file(tmp_path):
    source = tmp_path / "src"
    target = source / "bad.mseed"
    target.parent.mkdir(parents=True)
    target.write_bytes(b"not a waveform")
    output = tmp_path / "out"

    summary = waveform.filter_waveforms(source, output)

    assert summary.failed == 1
    assert summary.succeeded == 0
    assert summary.has_issues


def test_filter_waveforms_rejects_empty_extension_list(tmp_path):
    source = tmp_path / "src"
    source.mkdir()

    with np.testing.assert_raises_regex(ValueError, "at least one suffix"):
        waveform.filter_waveforms(source, tmp_path / "out", extensions=())


def test_filter_waveforms_min_periods_uses_low_frequency(tmp_path):
    source = tmp_path / "src"
    target = source / "day.mseed"
    target.parent.mkdir(parents=True)
    Stream([_trace(npts=1000, sampling_rate=1.0)]).write(target, format="MSEED")
    policy = TraceFilter(min_duration_seconds=0, min_samples=0, min_periods=3)

    filtered = waveform.filter_waveforms(
        source, tmp_path / "out-filtered", trace_filter=policy, low_frequency=0.001
    )
    kept = waveform.filter_waveforms(
        source, tmp_path / "out-kept", trace_filter=policy, low_frequency=0.01
    )

    assert (filtered.skipped, filtered.traces_filtered) == (1, 1)
    assert kept.succeeded == 1
    assert kept.traces_written == 1
