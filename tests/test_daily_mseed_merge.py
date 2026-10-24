"""Archive-to-daily MiniSEED integration contracts."""

import numpy as np
from obspy import Trace, UTCDateTime, read

from seispy.archive import channel_mseed_path
from seispy.waveform import merge_waveforms_by_day


def chunk(root, start, data):
    tr = Trace(np.asarray(data, dtype=np.int32))
    tr.stats.update(
        dict(
            network="XX",
            station="AAA",
            location="",
            channel="BHZ",
            starttime=UTCDateTime(start),
            sampling_rate=1,
        )
    )
    path = channel_mseed_path(
        root, "XX", "AAA", "", "BHZ", tr.stats.starttime, tr.stats.endtime + 1
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    tr.write(str(path), format="MSEED")


def test_midnight_gaps_and_resume(tmp_path):
    source, output = tmp_path / "archive", tmp_path / "daily"
    chunk(source, "2025-01-01T23:59:58", [1, 2, 3, 4])
    chunk(source, "2025-01-02T00:00:02", [5, 6])
    chunk(source, "2025-01-02T00:00:06", [9, 10])
    result = merge_waveforms_by_day(
        source, output, "*.mseed", output_format="mseed", max_workers=1
    )
    assert result.written == 2 and not result.errors
    files = sorted(output.rglob("*.mseed"))
    assert read(files[0])[0].data.tolist() == [1, 2]
    traces = read(files[1])
    assert [t.data.tolist() for t in traces] == [[3, 4, 5, 6], [9, 10]]
    assert traces[0].stats.starttime == UTCDateTime("2025-01-02")
    result = merge_waveforms_by_day(
        source, output, "*.mseed", output_format="mseed", max_workers=1
    )
    assert result.skipped == 2 and not result.errors


def test_conflicting_overlap_fails_without_output(tmp_path):
    source, output = tmp_path / "archive", tmp_path / "daily"
    chunk(source, "2025-01-01", [1, 2, 3, 4])
    chunk(source, "2025-01-01T00:00:02", [30, 40, 50])
    result = merge_waveforms_by_day(
        source, output, "*.mseed", output_format="mseed", max_workers=1
    )
    assert result.failed == 1
    assert "overlap" in result.errors[0]
    assert not list(output.rglob("*.mseed"))


def test_identical_overlap_and_changed_input(tmp_path):
    source, output = tmp_path / "archive", tmp_path / "daily"
    chunk(source, "2025-01-01", [1, 2, 3, 4])
    chunk(source, "2025-01-01T00:00:02", [3, 4, 5])
    result = merge_waveforms_by_day(
        source, output, "*.mseed", output_format="mseed", max_workers=1
    )
    assert result.written == 1 and not result.errors
    destination = next(output.rglob("*.mseed"))
    assert read(destination)[0].data.tolist() == [1, 2, 3, 4, 5]
    chunk(source, "2025-01-01T00:00:05", [6, 7])
    result = merge_waveforms_by_day(
        source, output, "*.mseed", output_format="mseed", max_workers=1
    )
    assert result.failed == 1
    assert read(destination)[0].stats.npts == 5
    result = merge_waveforms_by_day(
        source,
        output,
        "*.mseed",
        output_format="mseed",
        max_workers=1,
        overwrite=True,
    )
    assert result.written == 1 and not result.errors
    assert read(destination)[0].data.tolist() == [1, 2, 3, 4, 5, 6, 7]
    assert not (output / "merge-errors.txt").exists()
