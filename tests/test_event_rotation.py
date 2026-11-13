"""In-place rotation must preserve sources until verified N/E files exist."""

from pathlib import Path
from unittest.mock import patch

import numpy as np
import obspy
import pytest
from obspy.core.inventory import Channel, Inventory, Network, Station

from seispy.waveform import rotate_event_horizontals
from seispy.waveform import event_rotation as rotation

START = obspy.UTCDateTime("2024-01-01")


def inventory(angles=(30, 120), dip=0):
    channels = [
        Channel(
            code,
            "10",
            0,
            0,
            0,
            0,
            azimuth=az,
            dip=dip,
            sample_rate=100,
            start_date=START - 86400,
            end_date=START + 86400,
        )
        for code, az in zip(("HH1", "HH2"), angles, strict=True)
    ]
    return Inventory(
        [Network("NZ", stations=[Station("STA", 0, 0, 0, channels=channels)])], "test"
    )


def put(root, code, data, start=START, segment=""):
    root.mkdir(parents=True, exist_ok=True)
    tr = obspy.Trace(
        np.asarray(data, dtype=np.float32),
        header=dict(
            network="NZ",
            station="STA",
            location="10",
            channel=code,
            starttime=start,
            sampling_rate=1,
        ),
    )
    tr.stats.sac = dict(evdp=35, gcarc=45, o=-10)
    path = root / f"NZ.STA.10.{code}.20240101000000{segment}.sac"
    tr.write(str(path), format="SAC")
    return path


def pair(tmp_path, angles=(30, 120), offset=0):
    north = np.sin(np.arange(100) / 8)
    east = np.cos(np.arange(100) / 9)
    paths = []
    for i, az in enumerate(angles):
        data = np.cos(np.deg2rad(az)) * north + np.sin(np.deg2rad(az)) * east
        paths.append(
            put(tmp_path / "event", f"HH{i + 1}", data, START + (offset if i else 0))
        )
    return paths, north, east


def test_rotation_numbers_headers_untouched_vertical(tmp_path):
    paths, north, east = pair(tmp_path, offset=1e-6)
    vertical = put(tmp_path / "event", "HHZ", np.arange(150))
    original = vertical.read_bytes()
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.rotated_pairs == 1
    assert not result.has_issues
    assert all(not path.exists() for path in paths)
    assert vertical.read_bytes() == original
    for suffix, expected, az in zip("NE", (north, east), (0, 90), strict=True):
        path = next(tmp_path.rglob(f"*.HH{suffix}.*.sac"))
        tr = obspy.read(str(path))[0]
        np.testing.assert_allclose(tr.data, expected, atol=2e-7)
        assert tr.stats.channel == tr.stats.sac.kcmpnm == f"HH{suffix}"
        assert tr.stats.sac.cmpaz == az
        assert tr.stats.sac.cmpinc == 90
        assert tr.stats.sac.evdp == 35
        assert tr.stats.sac.kuser1 == "SPORI"
        assert tr.stats.starttime == START
    assert rotate_event_horizontals(tmp_path, inventory()).total_pairs == 0


@pytest.mark.parametrize(
    "mode", ["existing", "singular", "tilt", "missing_metadata", "write_failure"]
)
def test_failure_retains_originals(tmp_path, mode):
    paths, _, _ = pair(tmp_path)
    originals = [p.read_bytes() for p in paths]
    inv = inventory()
    if mode == "existing":
        put(tmp_path / "event", "HHN", np.arange(100))
    elif mode == "singular":
        inv = inventory((30, 30))
    elif mode == "tilt":
        inv = inventory(dip=5)
    elif mode == "missing_metadata":
        inv = Inventory([], "empty")
    if mode == "write_failure":
        with patch(
            "seispy.waveform._correction.commit_output",
            side_effect=[None, OSError("disk")],
        ):
            result = rotate_event_horizontals(tmp_path, inv)
    else:
        result = rotate_event_horizontals(tmp_path, inv)
    assert result.failed_pairs == 1
    assert [p.read_bytes() for p in paths] == originals
    assert not list(tmp_path.rglob("*.HHE.*.sac"))


@pytest.mark.parametrize("offset", [0.01, 1])
def test_mismatched_grid_does_not_trim(tmp_path, offset):
    paths, _, _ = pair(tmp_path, offset=offset)
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.unpaired_files == 2
    assert all(p.exists() for p in paths)


def test_deletion_failure_retains_both_outputs(tmp_path):
    paths, _, _ = pair(tmp_path)
    original_unlink = Path.unlink

    def unlink(path, *args, **kwargs):
        if path == paths[1]:
            raise PermissionError("cannot remove original")
        return original_unlink(path, *args, **kwargs)

    with patch.object(Path, "unlink", unlink):
        result = rotate_event_horizontals(tmp_path, inventory())
    assert result.deletion_failed_pairs == result.failed_pairs == 1
    assert len(list(tmp_path.rglob("*.HH[NE].*.sac"))) == 2
    assert paths[1].exists()


def test_commit_failure_rolls_back_first_output(tmp_path):
    paths, _, _ = pair(tmp_path)
    real_commit = rotation.commit_output
    calls = 0

    def commit(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise OSError("disk error")
        return real_commit(*args, **kwargs)

    with patch("seispy.waveform._correction.commit_output", commit):
        result = rotate_event_horizontals(tmp_path, inventory())
    assert result.failed_pairs == 1
    assert all(p.exists() for p in paths)
    assert not list(tmp_path.rglob("*.HH[NE].*.sac"))


def test_segments_and_pattern(tmp_path):
    paths, _, _ = pair(tmp_path)
    for code in ("HH1", "HH2"):
        put(tmp_path / "event", code, np.arange(10), START + 200, ".s02")
    result = rotate_event_horizontals(
        tmp_path,
        inventory(),
        pattern="*.HH1.*.sac",
        errors_csv=tmp_path.parent / f"{tmp_path.name}-errors.csv",
    )
    assert result.unpaired_files == 2
    assert result.errors_csv.read_text().startswith("source,error")
    assert all(p.exists() for p in paths)
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.rotated_pairs == 2


def test_ambiguous_and_overlapping_segments(tmp_path):
    paths, _, _ = pair(tmp_path)
    for code in ("HH1", "HH2"):
        put(tmp_path / "event", code, np.arange(10), START + 50, ".s02")
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.failed_pairs == 2
    assert all(p.exists() for p in paths)


def test_epoch_must_cover_entire_trace(tmp_path):
    paths, _, _ = pair(tmp_path)
    inv = inventory()
    inv[0][0][0].end_date = START + 10
    result = rotate_event_horizontals(tmp_path, inv)
    assert result.failed_pairs == 1
    assert "full-coverage" in result.issue_samples[0].error
    assert all(path.exists() for path in paths)


def test_sync_failure_before_deletion_rolls_back_outputs(tmp_path):
    paths, _, _ = pair(tmp_path)
    with patch.object(rotation, "_sync_directory", side_effect=OSError("sync error")):
        result = rotate_event_horizontals(tmp_path, inventory())
    assert result.failed_pairs == 1
    assert result.deletion_failed_pairs == 0
    assert all(path.exists() for path in paths)
    assert not list(tmp_path.rglob("*.HH[NE].*.sac"))


def test_ambiguous_pairing_preserves_all_sources(tmp_path):
    paths, _, _ = pair(tmp_path)
    put(tmp_path / "event", "HH2", np.arange(100), segment=".duplicate")
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.unpaired_files == 3
    assert result.total_pairs == 0
    assert all(path.exists() for path in paths)


def test_report_cannot_overwrite_source(tmp_path):
    paths, _, _ = pair(tmp_path)
    with pytest.raises(ValueError, match="outside src"):
        rotate_event_horizontals(tmp_path, inventory(), errors_csv=paths[0])


@pytest.mark.parametrize("rate", [25, 100])
def test_microsecond_timestamp_roundoff_at_high_sample_rate(tmp_path, rate):
    paths, north, east = pair(tmp_path, offset=1e-6)
    for path in paths:
        trace = obspy.read(str(path))[0]
        trace.stats.sampling_rate = rate
        trace.write(str(path), format="SAC")
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.rotated_pairs == 1
    assert not result.has_issues
    for suffix, expected in zip("NE", (north, east), strict=True):
        output = obspy.read(str(next(tmp_path.rglob(f"*.HH{suffix}.*.sac"))))[0]
        assert output.stats.starttime == START
        assert output.stats.npts == len(expected)
        np.testing.assert_allclose(output.data, expected, atol=2e-7)


def test_larger_timestamp_offset_at_high_sample_rate_is_rejected(tmp_path):
    paths, _, _ = pair(tmp_path, offset=20e-6)
    for path in paths:
        trace = obspy.read(str(path))[0]
        trace.stats.sampling_rate = 25
        trace.write(str(path), format="SAC")
    result = rotate_event_horizontals(tmp_path, inventory())
    assert result.unpaired_files == 2
    assert all(path.exists() for path in paths)
