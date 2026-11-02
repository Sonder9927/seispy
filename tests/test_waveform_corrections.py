"""Public-interface tests of sample-preserving clocks and physical axis recovery."""

import numpy as np
import obspy
import pytest
from obspy.core.inventory import Channel, Inventory, Network, Station

from seispy import waveform

START = obspy.UTCDateTime("2024-07-20T12:00:00")
NS = "https://github.com/Sonder9927/seispy/xmlns/obs/1"


def make_inventory(
    codes=("BHZ", "BHN", "BHE"),
    directions=((0, -90), (30, 0), (120, 0)),
    offset=0.0,
    rate=1e-7,
):
    sta = Station(
        "R41", -36.0, 177.0, -1300.0, start_date=START - 86400, end_date=START + 86400
    )
    fields = dict(
        clock_model="linear_recorded_timestamp",
        clock_reference_time=str(START - 1000),
        clock_end_time=str(START + 1000),
        clock_initial_offset_seconds=str(offset),
        clock_rate_seconds_per_second=str(rate),
        clock_sign="observed_time_minus_GPS_time",
        clock_model_argument="recorded timestamp",
        clock_correction_applied="false",
    )
    sta.extra = {k: dict(value=v, namespace=NS) for k, v in fields.items()}
    for code, (az, dip) in zip(codes, directions, strict=True):
        sta.channels.append(
            Channel(
                code,
                "LC",
                -36.0,
                177.0,
                -1300.0,
                0.0,
                azimuth=az,
                dip=dip,
                sample_rate=50.0,
                start_date=START - 86400,
                end_date=START + 86400,
            )
        )
    return Inventory([Network("1A", stations=[sta])], "test")


def put(root, channel, data, start=START, suffix="", station="R41"):
    root.mkdir(parents=True, exist_ok=True)
    tr = obspy.Trace(
        np.asarray(data, dtype=np.float32),
        header=dict(
            network="1A",
            station=station,
            location="LC",
            channel=channel,
            sampling_rate=50.0,
            starttime=start,
        ),
    )
    path = root / f"arbitrary-{channel}{suffix}.sac"
    tr.write(str(path), format="SAC")
    return path


def records(root):
    return {
        obspy.read(str(p), headonly=True)[0].stats.channel: obspy.read(str(p))[0]
        for p in root.rglob("*.sac")
    }


def options(tmp_path):
    return dict(
        output_dir=tmp_path / "out", max_workers=1, save_report=False, save_log=False
    )


def signals(n=200):
    t = np.arange(n)
    return np.array([np.sin(t / 9), np.cos(t / 11) * 2, np.sin(t / 13) * 3])


def put_axes(root, inv, physical=None):
    physical = signals() if physical is None else physical
    for ch in inv[0][0]:
        az, dip = np.deg2rad([ch.azimuth, ch.dip])
        axis = [-np.sin(dip), np.cos(az) * np.cos(dip), np.sin(az) * np.cos(dip)]
        put(root, ch.code, np.asarray(axis) @ physical)
    return physical


def test_clock_preserves_samples_rate_and_fractional_shift(tmp_path):
    src = tmp_path / "src"
    inv = make_inventory(offset=2.347812345, rate=-1.4382373561551107e-7)
    raw = put(src, "BHZ", signals()[0])
    original = raw.read_bytes()
    put(src, "BHH", signals()[0])
    result = waveform.correct_clock_drift(src, inv, **options(tmp_path))
    assert result.succeeded == 1 and result.ignored == 1 and result.failed == 0
    source, output = obspy.read(str(raw))[0], records(tmp_path / "out")["BHZ"]
    expected = 2.347812345 - 1.4382373561551107e-7 * (1000 + (len(source) - 1) / 100)
    assert abs(output.stats.starttime - (source.stats.starttime - expected)) <= 1e-6
    assert abs(expected / 0.02 - round(expected / 0.02)) > 0.01
    np.testing.assert_array_equal(output.data, source.data)
    assert output.stats.delta == source.stats.delta
    assert raw.read_bytes() == original
    assert not (tmp_path / "out/.corrections").exists()


@pytest.mark.parametrize(
    "change",
    [
        "clock_sign",
        "clock_end_time",
        "clock_reference_time",
        "clock_correction_applied",
    ],
)
def test_clock_rejects_unsafe_models_or_partial_coverage(tmp_path, change):
    inv = make_inventory()
    fields = inv[0][0].extra
    if change == "clock_sign":
        fields[change]["value"] = "GPS_minus_observed"
    elif change == "clock_end_time":
        fields[change]["value"] = str(START + 1)
    elif change == "clock_reference_time":
        del fields[change]
    else:
        fields[change]["value"] = "true"
    put(tmp_path / "src", "BHZ", signals()[0])
    result = waveform.correct_clock_drift(tmp_path / "src", inv, **options(tmp_path))
    assert result.failed == 1 and not list((tmp_path / "out").rglob("*.sac"))


def test_clock_conflicts_overwrite_and_prevents_double_application(tmp_path):
    put(tmp_path / "src", "BHZ", signals()[0])
    inv = make_inventory()
    first = waveform.correct_clock_drift(tmp_path / "src", inv, **options(tmp_path))
    second = waveform.correct_clock_drift(tmp_path / "src", inv, **options(tmp_path))
    assert first.succeeded == 1 and second.failed == 1
    assert "overwrite=True" in second.issue_samples[0].error
    duplicate = waveform.correct_clock_drift(
        tmp_path / "out", inv, output_dir=tmp_path / "twice", max_workers=1
    )
    assert (
        duplicate.failed == 1 and "already applied" in duplicate.issue_samples[0].error
    )
    target = next((tmp_path / "out").rglob("*.sac"))
    target.write_bytes(b"truncated")
    damaged = waveform.correct_clock_drift(tmp_path / "src", inv, **options(tmp_path))
    assert damaged.failed == 1 and damaged.skipped == 0
    restored = waveform.correct_clock_drift(
        tmp_path / "src", inv, overwrite=True, **options(tmp_path)
    )
    assert restored.succeeded == 1


def test_clock_identical_windows_have_identical_shifts(tmp_path):
    inv = make_inventory(rate=-1.4e-7)
    put_axes(tmp_path / "src", inv)
    result = waveform.correct_clock_drift(tmp_path / "src", inv, **options(tmp_path))
    assert result.succeeded == 3
    out = records(tmp_path / "out")
    assert len({tr.stats.starttime.ns for tr in out.values()}) == 1
    rotated = waveform.correct_orientation(
        tmp_path / "out", inv, output_dir=tmp_path / "rotated", max_workers=1
    )
    assert rotated.succeeded == 1 and rotated.files_written == 3


def test_horizontal_recovery_uses_xml_and_writes_vertical(tmp_path):
    inv = make_inventory(
        codes=("BH3", "BH1", "BH2"), directions=((0, -90), (17, 0), (131, 0))
    )
    expected = put_axes(tmp_path / "src", inv)
    raw_z = obspy.read(str(tmp_path / "src/arbitrary-BH3.sac"))[0].data
    put(tmp_path / "src", "BHH", expected[0])
    result = waveform.correct_orientation(
        tmp_path / "src", inv, components=("BH3", "BH1", "BH2"), **options(tmp_path)
    )
    assert result.succeeded == 1 and result.files_written == 3 and result.ignored == 1
    out = records(tmp_path / "out")
    for i, code in enumerate(("BHZ", "BHN", "BHE")):
        np.testing.assert_allclose(out[code].data, expected[i], atol=3e-7)
        assert out[code].stats.sac.cmpaz == (0, 0, 90)[i]
        assert out[code].stats.sac.cmpinc == (0, 90, 90)[i]
    np.testing.assert_array_equal(out["BHZ"].data, raw_z)
    again = waveform.correct_orientation(
        tmp_path / "src", inv, components=("BH3", "BH1", "BH2"), **options(tmp_path)
    )
    assert again.failed == 1
    assert "overwrite=True" in again.issue_samples[0].error
    replaced = waveform.correct_orientation(
        tmp_path / "src",
        inv,
        components=("BH3", "BH1", "BH2"),
        overwrite=True,
        **options(tmp_path),
    )
    assert replaced.succeeded == 1 and replaced.files_written == 3
    assert not (tmp_path / "out/.corrections").exists()
    for i, code in enumerate(("BHZ", "BHN", "BHE")):
        np.testing.assert_allclose(
            records(tmp_path / "out")[code].data, expected[i], atol=3e-7
        )
    # Channel names changed, but corrected outputs still cannot be rotated twice.
    duplicate_inv = make_inventory()
    duplicate = waveform.correct_orientation(
        tmp_path / "out", duplicate_inv, output_dir=tmp_path / "twice", max_workers=1
    )
    assert (
        duplicate.failed == 1 and "already applied" in duplicate.issue_samples[0].error
    )


def test_full_3d_recovery_requires_measured_dips(tmp_path):
    inv = make_inventory(directions=((10, -75), (100, 5), (200, 8)))
    expected = put_axes(tmp_path / "src", inv)
    result = waveform.correct_orientation(
        tmp_path / "src", inv, apply_tilt=True, **options(tmp_path)
    )
    assert result.succeeded == 1
    out = records(tmp_path / "out")
    for i, code in enumerate(("BHZ", "BHN", "BHE")):
        np.testing.assert_allclose(out[code].data, expected[i], atol=5e-7)
    inv[0][0].extra["dip_status"] = dict(value="nominal; not measured", namespace=NS)
    rejected = waveform.correct_orientation(
        tmp_path / "src",
        inv,
        apply_tilt=True,
        output_dir=tmp_path / "nominal",
        max_workers=1,
    )
    assert rejected.failed == 1 and "measured" in rejected.issue_samples[0].error


@pytest.mark.parametrize(
    "kind", ["missing", "fractional", "rate", "singular", "unknown"]
)
def test_orientation_rejects_ambiguous_data_without_partial_outputs(tmp_path, kind):
    inv = make_inventory()
    put_axes(tmp_path / "src", inv)
    path = tmp_path / "src/arbitrary-BHE.sac"
    if kind == "missing":
        path.unlink()
    elif kind == "fractional":
        tr = obspy.read(str(path))[0]
        tr.stats.starttime += 0.005
        tr.write(str(path), format="SAC")
    elif kind == "rate":
        tr = obspy.read(str(path))[0]
        tr.stats.sampling_rate = 25.0
        tr.write(str(path), format="SAC")
    elif kind == "singular":
        inv[0][0][2].azimuth = inv[0][0][1].azimuth
    else:
        inv[0][0][2].azimuth = None
    result = waveform.correct_orientation(tmp_path / "src", inv, **options(tmp_path))
    assert result.failed == 1 and result.files_written == 0
    assert not list((tmp_path / "out").rglob("*.sac"))


def test_orientation_trims_integer_offsets_without_interpolation(tmp_path):
    inv = make_inventory()
    expected = put_axes(tmp_path / "src", inv)
    path = tmp_path / "src/arbitrary-BHE.sac"
    tr = obspy.read(str(path))[0]
    tr.data = tr.data[2:]
    tr.stats.starttime += 0.04
    tr.write(str(path), format="SAC")
    result = waveform.correct_orientation(tmp_path / "src", inv, **options(tmp_path))
    assert result.succeeded == 1
    for i, code in enumerate(("BHZ", "BHN", "BHE")):
        tr = records(tmp_path / "out")[code]
        assert tr.stats.starttime == START + 0.04 and len(tr) == 198
        np.testing.assert_allclose(tr.data, expected[i, 2:], atol=3e-7)


def test_orientation_preserves_gaps_and_rejects_conflicts(tmp_path):
    inv = make_inventory()
    physical = signals(20)
    for ch in inv[0][0]:
        az, dip = np.deg2rad([ch.azimuth, ch.dip])
        data = (
            np.asarray(
                [-np.sin(dip), np.cos(az) * np.cos(dip), np.sin(az) * np.cos(dip)]
            )
            @ physical
        )
        put(tmp_path / "src", ch.code, data, suffix="-a")
        put(tmp_path / "src", ch.code, data, start=START + 3, suffix="-b")
    result = waveform.correct_orientation(tmp_path / "src", inv, **options(tmp_path))
    assert result.succeeded == 1 and result.files_written == 6
    assert all(
        len(obspy.read(str(p))[0]) == 20 for p in (tmp_path / "out").rglob("*.sac")
    )
    put(
        tmp_path / "src", "BHN", np.ones(20) * 90, start=START + 0.1, suffix="-conflict"
    )
    rejected = waveform.correct_orientation(
        tmp_path / "src", inv, output_dir=tmp_path / "conflict", max_workers=1
    )
    assert rejected.failed == 1 and "overlap" in rejected.issue_samples[0].error.lower()


def test_group_write_failure_rolls_back_outputs(tmp_path, monkeypatch):
    import seispy.waveform._correction as support

    inv = make_inventory()
    put_axes(tmp_path / "src", inv)
    real_commit = support.commit_output
    n = 0

    def fail_second(*args, **kwargs):
        nonlocal n
        n += 1
        if n == 2:
            raise OSError("simulated output failure")
        return real_commit(*args, **kwargs)

    monkeypatch.setattr(support, "commit_output", fail_second)
    result = waveform.correct_orientation(tmp_path / "src", inv, **options(tmp_path))
    assert result.failed == 1 and not list((tmp_path / "out").rglob("*.sac"))
    assert not list((tmp_path / "out/.corrections").glob("*.json"))


def test_clock_process_pool_and_xml_roundtrip(tmp_path):
    inv = make_inventory()
    xml = tmp_path / "stations.xml"
    inv.write(str(xml), format="STATIONXML", nsmap={"obs": NS})
    put_axes(tmp_path / "src", inv)
    result = waveform.correct_clock_drift(
        tmp_path / "src",
        xml,
        output_dir=tmp_path / "out",
        max_workers=2,
        files_per_pool=2,
    )
    assert result.succeeded == 3 and result.ok and result.report_path.is_file()
    rotated = waveform.correct_orientation(
        tmp_path / "out",
        xml,
        output_dir=tmp_path / "rotated",
        max_workers=2,
        files_per_pool=2,
    )
    assert rotated.succeeded == 1 and rotated.ok


def test_clock_distant_sac_reference_preserves_subsample_time(tmp_path):
    from obspy.io.sac import SACTrace

    path = put(tmp_path / "src", "BHZ", signals()[0])
    sac = SACTrace.read(str(path))
    sac.reftime = START - 100000
    sac.o = 100000.5
    original_origin = sac.reftime + sac.o
    sac.write(str(path))
    source = obspy.read(str(path))[0]
    inv = make_inventory(offset=2.347812345, rate=0.0)
    result = waveform.correct_clock_drift(tmp_path / "src", inv, **options(tmp_path))
    assert result.succeeded == 1
    output = records(tmp_path / "out")["BHZ"]
    assert (
        abs(output.stats.starttime.ns - (source.stats.starttime - 2.347812345).ns)
        <= 1000
    )
    np.testing.assert_array_equal(source.data, output.data)
    written = SACTrace.read(str(next((tmp_path / "out").rglob("*.sac"))))
    assert abs((written.reftime + written.o).ns - original_origin.ns) <= 1000


def test_clock_rejects_duplicate_canonical_paths_even_with_overwrite(tmp_path):
    put(tmp_path / "src", "BHZ", signals()[0])
    put(tmp_path / "src", "BHZ", signals()[0] * 2, suffix="-conflict")
    result = waveform.correct_clock_drift(
        tmp_path / "src", make_inventory(), overwrite=True, **options(tmp_path)
    )
    assert result.succeeded == 1 and result.failed == 1
    assert "same corrected SAC path" in result.issue_samples[0].error
    assert len(list((tmp_path / "out").rglob("*.sac"))) == 1


@pytest.mark.parametrize("max_workers", [1, 2])
def test_event_corrections_isolate_overlapping_events_and_preserve_paths(
    tmp_path, max_workers
):
    inv = make_inventory(offset=0.123456789, rate=0)
    source = tmp_path / "events"
    expected = {}
    for name, scale in (("event_a", 1), ("event_b", 2)):
        expected[name] = put_axes(source / name, inv, signals() * scale)
    (source / "logs").mkdir()
    (source / "logs" / "ignored.sac").write_bytes(b"not a waveform")
    clock = tmp_path / "clock"
    result = waveform.correct_clock_drift(
        source,
        inv,
        output_dir=clock,
        layout="event",
        max_workers=max_workers,
        save_report=False,
        save_log=False,
    )
    assert result.succeeded == 6 and not result.has_issues
    assert {p.relative_to(clock) for p in clock.rglob("*.sac")} == {
        p.relative_to(source) for p in source.rglob("*.sac") if p.parent.name != "logs"
    }
    oriented = tmp_path / "oriented"
    result = waveform.correct_orientation(
        clock,
        inv,
        output_dir=oriented,
        layout="event",
        max_workers=max_workers,
        save_report=False,
        save_log=False,
    )
    assert result.total == 2 and result.files_written == 6 and not result.has_issues
    for name in expected:
        for i, code in enumerate(("BHZ", "BHN", "BHE")):
            target = oriented / name / f"1A.R41.LC.{code}.{name}.sac"
            tr = obspy.read(str(target))[0]
            np.testing.assert_allclose(tr.data, expected[name][i], atol=6e-7)
            assert abs(tr.stats.starttime - (START - 0.123456789)) < 1e-6
            assert tr.stats.sac.kuser0 == "SPCLK" and tr.stats.sac.kuser1 == "SPORI"


def test_event_orientation_merges_cross_midnight_segments_within_event(tmp_path):
    inv = make_inventory()
    physical = signals()
    root = tmp_path / "events" / "cross_midnight"
    for channel in inv[0][0]:
        az, dip = np.deg2rad([channel.azimuth, channel.dip])
        axis = [-np.sin(dip), np.cos(az) * np.cos(dip), np.sin(az) * np.cos(dip)]
        data = np.asarray(axis) @ physical
        put(root, channel.code, data[:100], start=START + 43198, suffix="a")
        put(root, channel.code, data[100:], start=START + 43200, suffix="b")
    result = waveform.correct_orientation(
        tmp_path / "events",
        inv,
        layout="event",
        **options(tmp_path),
    )
    assert result.total == 1 and result.files_written == 3 and not result.has_issues
    for i, code in enumerate(("BHZ", "BHN", "BHE")):
        target = (
            tmp_path / "out/cross_midnight" / f"1A.R41.LC.{code}.cross_midnight.sac"
        )
        np.testing.assert_allclose(
            obspy.read(str(target))[0].data, physical[i], atol=3e-7
        )


@pytest.mark.parametrize(
    "function", [waveform.correct_clock_drift, waveform.correct_orientation]
)
def test_corrections_validate_layout_and_event_root(tmp_path, function):
    put(tmp_path / "src", "BHZ", signals()[0])
    with pytest.raises(ValueError, match="layout must"):
        function(
            tmp_path / "src", make_inventory(), layout="unknown", **options(tmp_path)
        )
    with pytest.raises(ValueError, match="event subdirectories"):
        function(
            tmp_path / "src", make_inventory(), layout="event", **options(tmp_path)
        )


def test_event_orientation_names_gapped_segments(tmp_path):
    inv = make_inventory()
    root = tmp_path / "events" / "gapped"
    put_axes(root, inv)
    for channel in inv[0][0]:
        original = obspy.read(str(root / f"arbitrary-{channel.code}.sac"))[0]
        put(root, channel.code, original.data, start=START + 10, suffix="later")
    result = waveform.correct_orientation(
        tmp_path / "events",
        inv,
        layout="event",
        **options(tmp_path),
    )
    assert not result.has_issues and result.files_written == 6
    for channel in ("BHZ", "BHN", "BHE"):
        paths = sorted((tmp_path / "out/gapped").glob(f"*.{channel}.*.sac"))
        assert [p.name for p in paths] == [
            f"1A.R41.LC.{channel}.gapped.s01.sac",
            f"1A.R41.LC.{channel}.gapped.s02.sac",
        ]
        assert obspy.read(str(paths[1]))[0].stats.starttime == START + 10
