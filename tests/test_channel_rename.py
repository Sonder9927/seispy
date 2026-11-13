"""Channel renaming changes only kcmpnm and never overwrites destinations."""

import csv
from pathlib import Path
from unittest.mock import patch

import numpy as np
import obspy
import pytest

from seispy.waveform import rename_channels
from seispy.waveform import channel_rename as rename


def put(root, code="HHX", *, station="NZ29", byteorder="<"):
    root.mkdir(parents=True, exist_ok=True)
    tr = obspy.Trace(
        np.arange(200, dtype=np.float32),
        header=dict(
            network="NZ",
            station=station,
            location="10",
            channel=code,
            starttime=obspy.UTCDateTime("2024-01-01"),
            sampling_rate=25,
        ),
    )
    tr.stats.sac = dict(cmpaz=123, cmpinc=45, evdp=30, o=-10)
    path = root / f"NZ.{station}.10.{code}.20240101000000.sac"
    tr.write(str(path), format="SAC", byteorder=byteorder)
    return path


@pytest.mark.parametrize("byteorder", ["<", ">"])
def test_only_channel_bytes_change(tmp_path, byteorder):
    source = tmp_path / "input"
    path = put(source / "event", byteorder=byteorder)
    before = path.read_bytes()
    result = rename_channels(source, {"HHX": "BHE"})
    assert result.renamed == 1 and not result.has_issues
    assert not path.exists()
    destination = path.with_name(path.name.replace(".HHX.", ".BHE."))
    after = destination.read_bytes()
    assert after[:600] == before[:600]
    assert after[600:608] == b"BHE     "
    assert after[608:] == before[608:]
    trace = obspy.read(str(destination))[0]
    assert trace.stats.channel == trace.stats.sac.kcmpnm == "BHE"
    assert trace.stats.sac.cmpaz == 123 and trace.stats.sac.cmpinc == 45
    np.testing.assert_array_equal(trace.data, np.arange(200, dtype=np.float32))


def test_pattern_noop_and_single_application(tmp_path):
    source = tmp_path / "input"
    path = put(source / "event")
    other = put(source / "event", station="OTHER")
    vertical = put(source / "event", "HHZ")
    before = {p: p.read_bytes() for p in (other, vertical)}
    result = rename_channels(
        source, {"HHX": "BHE", "BHE": "BHN", "HHZ": "HHZ"}, pattern="*.NZ29.*.sac"
    )
    assert result.renamed == 1 and result.skipped == 1
    assert path.with_name(path.name.replace(".HHX.", ".BHE.")).exists()
    assert not list(source.rglob("*.BHN.*"))
    assert all(p.read_bytes() == content for p, content in before.items())


def test_many_to_one_and_existing_conflicts(tmp_path):
    source = tmp_path / "input"
    first = put(source / "event", "HHX")
    second = put(source / "event", "HHY")
    result = rename_channels(source, {"HHX": "BHE", "HHY": "BHE"})
    assert result.failed == 2
    assert first.exists() and second.exists()
    target = put(source / "event", "BHE")
    original = target.read_bytes()
    result = rename_channels(source, {"HHX": "BHE"})
    assert result.failed == 1
    assert first.exists() and target.read_bytes() == original


def test_swap_and_occupied_chain_preflight(tmp_path):
    source = tmp_path / "input"
    first = put(source, "HHX")
    second = put(source, "HHY")
    originals = {p: p.read_bytes() for p in (first, second)}
    result = rename_channels(source, {"HHX": "HHY", "HHY": "HHX"})
    assert result.failed == 2
    assert all(p.read_bytes() == content for p, content in originals.items())
    result = rename_channels(source, {"HHX": "HHY", "HHY": "BHN"})
    assert result.failed == result.renamed == 1
    assert first.read_bytes() == originals[first]
    assert not second.exists()


@pytest.mark.parametrize("stage", ["verify", "commit", "sync"])
def test_failures_preserve_original(tmp_path, stage):
    source = tmp_path / "input"
    path = put(source)
    original = path.read_bytes()
    target = {
        "verify": "_verify_bytes",
        "commit": "commit_output",
        "sync": "_sync_directory",
    }[stage]
    with patch.object(rename, target, side_effect=OSError("injected failure")):
        result = rename_channels(source, {"HHX": "BHE"})
    assert result.failed == 1 and result.deletion_failed == 0
    assert path.read_bytes() == original
    assert list(source.iterdir()) == [path]


def test_deletion_failure_preserves_verified_target(tmp_path):
    source = tmp_path / "input"
    path = put(source)
    real_unlink = Path.unlink

    def unlink(p, *args, **kwargs):
        if p == path:
            raise PermissionError("cannot delete original")
        return real_unlink(p, *args, **kwargs)

    with patch.object(Path, "unlink", unlink):
        result = rename_channels(source, {"HHX": "BHE"})
    assert result.failed == result.deletion_failed == 1
    assert path.exists()
    assert len(list(source.glob("*.BHE.*"))) == 1


def test_errors_csv_and_invalid_filename(tmp_path):
    source = tmp_path / "input"
    path = put(source)
    path.rename(source / "bad.sac")
    report = tmp_path / "errors.csv"
    result = rename_channels(source, {"HHX": "BHE"}, errors_csv=report)
    assert result.failed == 1
    with report.open() as handle:
        rows = list(csv.reader(handle))
    assert rows[0] == ["source", "error"] and len(rows) == 2
    assert (source / "bad.sac").exists()


@pytest.mark.parametrize(
    "mapping", [{}, {"hhx": "BHE"}, {"HHX": "BHEE"}, {"HHX": "B.E"}, {"HHX": 1}]
)
def test_invalid_mapping(tmp_path, mapping):
    with pytest.raises(ValueError):
        rename_channels(tmp_path, mapping)


def test_report_cannot_overwrite_source(tmp_path):
    path = put(tmp_path)
    with pytest.raises(ValueError, match="outside src"):
        rename_channels(tmp_path, {"HHX": "BHE"}, errors_csv=path)


def test_occupied_chain_is_not_freed_by_earlier_task(tmp_path):
    first = put(tmp_path, "HHX")
    second = put(tmp_path, "HHY")
    original = second.read_bytes()
    result = rename_channels(tmp_path, {"HHX": "BHE", "HHY": "HHX"}, max_workers=1)
    assert result.renamed == result.failed == 1
    assert not first.exists()
    assert second.read_bytes() == original


@pytest.mark.parametrize("byteorder", ["<", ">"])
def test_sac_v7_footer_preserved(tmp_path, byteorder):
    import struct

    path = put(tmp_path, byteorder=byteorder)
    content = bytearray(path.read_bytes())
    content[304:308] = struct.pack(byteorder + "i", 7)  # nvhdr
    footer = np.arange(22, dtype=byteorder + "f8").tobytes()
    path.write_bytes(content + footer)
    result = rename_channels(tmp_path, {"HHX": "BHE"})
    assert result.renamed == 1
    target = next(tmp_path.glob("*.BHE.*"))
    after = target.read_bytes()
    assert after[:600] == content[:600]
    assert after[608:] == (content + footer)[608:]
