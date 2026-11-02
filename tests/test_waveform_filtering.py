import pytest
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


@pytest.mark.parametrize(
    "format,npts,duration",
    [("MSEED", [100, 1000], 5), ("MSEED", [100], 60), ("SAC", [1000], 5)],
)
def test_trace_filtering(tmp_path, format, npts, duration):
    source = tmp_path / "src"
    suffix = "mseed" if format == "MSEED" else "sac"
    target = source / "NZ" / "AAA" / f"day.{suffix}"
    target.parent.mkdir(parents=True)
    stream = Stream(
        [_trace(npts=n, start=f"2026-01-01T0{i}:00:00") for i, n in enumerate(npts)]
    )
    stream.write(str(target), format=format)
    original = target.read_bytes()
    output = tmp_path / "out"
    summary = waveform.filter_waveforms(
        source, output, trace_filter=TraceFilter(min_duration_seconds=duration)
    )
    accepted = [n for n in npts if (n - 1) / 100 >= duration]
    assert summary.traces_total == len(npts)
    assert summary.traces_written == len(accepted)
    assert summary.traces_filtered == len(npts) - len(accepted)
    assert summary.succeeded == bool(accepted)
    assert summary.skipped == (not accepted)
    assert target.read_bytes() == original
    outputs = list(output.rglob(f"*.{suffix}"))
    assert len(outputs) == bool(accepted)
    if accepted:
        assert [tr.stats.npts for tr in read(str(outputs[0]))] == accepted


@pytest.mark.parametrize(
    "scenario",
    [
        "filter_waveforms_reports_unreadable_file",
        "filter_waveforms_rejects_empty_extension_list",
    ],
)
def test_filter_input_failures(tmp_path, scenario):
    source = tmp_path / "src"
    if scenario == "filter_waveforms_reports_unreadable_file":
        target = source / "bad.mseed"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"not a waveform")
        output = tmp_path / "out"

        summary = waveform.filter_waveforms(source, output)

        assert summary.failed == 1
        assert summary.succeeded == 0
        assert summary.has_issues
    elif scenario == "filter_waveforms_rejects_empty_extension_list":
        source.mkdir()

        with np.testing.assert_raises_regex(ValueError, "at least one suffix"):
            waveform.filter_waveforms(source, tmp_path / "out", extensions=())
