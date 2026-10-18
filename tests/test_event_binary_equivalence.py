"""Opt-in comparison against the locally built 20160209 C cutter.

Set SEISPY_CUT_BIN_DIR to a directory with mktraceiodb and cutevent.
These tests use disposable waveform copies; no production input is modified.
"""

import os
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from obspy import Trace, UTCDateTime, read
from obspy.io.sac.util import get_sac_reftime

from seispy.event import cut_event_waveforms, cut_events_binary, write_event_catalog


@pytest.fixture
def binaries(tmp_path, monkeypatch):
    configured = os.environ.get("SEISPY_CUT_BIN_DIR")
    if not configured:
        pytest.skip("Set SEISPY_CUT_BIN_DIR to run compiled-cutter comparisons")
    source = Path(configured).resolve()
    bindir = tmp_path / "bin"
    bindir.mkdir()
    for name in ("mktraceiodb", "cutevent"):
        executable = source / name
        if not executable.is_file():
            pytest.fail(f"Missing executable: {executable}")
        (bindir / name).symlink_to(executable)
    monkeypatch.chdir(tmp_path)


def _run_cutters(tmp_path, rate, fraction, gap_seconds=0):
    source = tmp_path / "source"
    station = source / "STA"
    station.mkdir(parents=True)
    base = UTCDateTime("2024-01-01T23:59:58")
    rng = np.random.default_rng(8492)
    # Two continuous records across midnight exercise merging and cache slicing.
    data = rng.normal(size=int(20 * rate)).astype(np.float32)
    for number, offset in enumerate((0, 10 + gap_seconds)):
        tr = Trace(
            data[int(offset * rate) : int((offset + 10) * rate)].copy(),
            header={
                "network": "XX",
                "station": "STA",
                "location": "10",
                "channel": "BHZ",
                "starttime": base + offset,
                "sampling_rate": rate,
            },
        )
        tr.stats.sac = {"stla": 30.0, "stlo": 110.0, "o": -20.0, "a": 1.0}
        tr.write(str(station / f"{number}.sac"), format="SAC")
    before = {p: p.read_bytes() for p in station.glob("*.sac")}
    origin = base + 8 + fraction
    frame = pd.DataFrame(
        [dict(time=str(origin), longitude=105.0, latitude=35.0, depth=12.0, mag=6.0)]
    )
    csv = tmp_path / "event.csv"
    frame.to_csv(csv, index=False)
    cat = tmp_path / "event.cat"
    write_event_catalog(frame, cat)
    # A five-column catalog restores event metadata; the public exporter keeps
    # its backwards-compatible time-only schema but now retains microseconds.
    cat.write_text(cat.read_text().strip() + " 105 35 6 12\n")
    summary = cut_event_waveforms(
        source,
        csv,
        output_dir=tmp_path / "obspy",
        time_window=5,
        max_workers=1,
        save_report=False,
        save_log=False,
    )
    cut_events_binary(source, tmp_path / "binary", cat, time_window=5, max_workers=1)
    assert summary.succeeded == 1
    a = read(next((tmp_path / "obspy").rglob("*.sac")))[0]
    b = read(next((tmp_path / "binary").rglob("*.sac")))[0]
    assert all(p.read_bytes() == content for p, content in before.items())
    return a, b, origin


@pytest.mark.integration
@pytest.mark.parametrize("rate", [1, 5, 20, 25, 40, 100])
@pytest.mark.parametrize("fraction", [0, 0.123, 0.125, 0.237, 0.123456, 0.999999])
def test_public_cutters_match_contiguous_samples_and_event_time(
    tmp_path, binaries, rate, fraction
):
    a, b, origin = _run_cutters(tmp_path, rate, fraction)
    np.testing.assert_array_equal(a.data, b.data)
    assert a.id == b.id
    assert a.stats.starttime == b.stats.starttime
    assert a.stats.endtime == b.stats.endtime
    assert a.stats.sampling_rate == b.stats.sampling_rate
    start = UTCDateTime(ns=origin.ns // 1_000_000_000 * 1_000_000_000)
    assert a.stats.starttime == start
    assert a.stats.endtime == start + 5
    assert a.stats.npts == 5 * rate + 1
    for trace in (a, b):
        assert (
            abs(get_sac_reftime(trace.stats.sac) + float(trace.stats.sac.o) - origin)
            < 1e-6
        )
        for key, value in [("evla", 35), ("evlo", 105), ("evdp", 12), ("mag", 6)]:
            assert trace.stats.sac[key] == value


@pytest.mark.integration
def test_integer_second_start_avoids_fractional_origin_endpoint_difference(
    tmp_path, binaries
):
    a, b, origin = _run_cutters(tmp_path, 100, 0.125)
    assert a.stats.npts == b.stats.npts == 501
    np.testing.assert_array_equal(a.data, b.data)
    assert a.stats.starttime.microsecond == b.stats.starttime.microsecond == 0


@pytest.mark.integration
def test_binary_restores_submillisecond_origin_without_changing_window(
    tmp_path, binaries
):
    a, b, origin = _run_cutters(tmp_path, 100, 0.123456)
    np.testing.assert_array_equal(a.data, b.data)
    assert a.stats.starttime == b.stats.starttime
    for trace in (a, b):
        assert (
            abs(get_sac_reftime(trace.stats.sac) + float(trace.stats.sac.o) - origin)
            < 1e-6
        )


@pytest.mark.integration
def test_integer_second_rule_does_not_hide_different_gap_policies(tmp_path, binaries):
    _, binary, _ = _run_cutters(tmp_path, 100, 0.123456, gap_seconds=2)
    segments = sorted(
        (read(path)[0] for path in (tmp_path / "obspy").rglob("*.sac")),
        key=lambda trace: trace.stats.starttime,
    )
    assert len(segments) == 2
    assert binary.stats.npts == 501
    np.testing.assert_array_equal(segments[0].data, binary.data[:200])
    np.testing.assert_array_equal(binary.data[200:400], np.zeros(200))
    np.testing.assert_array_equal(segments[1].data, binary.data[400:])
