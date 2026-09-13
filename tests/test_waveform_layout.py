"""Canonical waveform organization and merge contracts."""

from pathlib import Path

import numpy as np
from obspy import Trace, UTCDateTime, read

from seispy.archive import WaveformIdentity
from seispy.waveform.merge import _group_targets, _merge_targets
from seispy.waveform.organization import _copy_targets


def _sac(path: Path, *, channel="BHZ", starttime="2025-01-01T00:00:00"):
    trace = Trace(data=np.arange(5, dtype=np.float32))
    trace.stats.network = "NZ"
    trace.stats.station = "WEL"
    trace.stats.location = "10"
    trace.stats.channel = channel
    trace.stats.starttime = UTCDateTime(starttime)
    trace.stats.sampling_rate = 1
    path.parent.mkdir(parents=True, exist_ok=True)
    trace.write(str(path), format="SAC")
    return trace


def test_sort_uses_sac_header_instead_of_source_filename(tmp_path):
    source = tmp_path / "source" / "misleading-name.sac"
    trace = _sac(source)

    _copy_targets([source], tmp_path / "archive")

    expected = WaveformIdentity.from_trace(trace).sac_path(tmp_path / "archive")
    assert expected.is_file()
    assert expected.parent == tmp_path / "archive" / "NZ" / "WEL" / "2025"


def test_merge_groups_flat_archive_by_header_channel_and_day(tmp_path):
    root = tmp_path / "archive"
    first_trace = _sac(tmp_path / "first.sac")
    first = WaveformIdentity.from_trace(first_trace).sac_path(root)
    first.parent.mkdir(parents=True)
    Path(tmp_path / "first.sac").replace(first)

    second_trace = _sac(tmp_path / "second.sac", starttime="2025-01-01T00:00:05")
    second = WaveformIdentity.from_trace(second_trace).sac_path(root)
    Path(tmp_path / "second.sac").replace(second)

    other_trace = _sac(tmp_path / "other.sac", channel="BHN")
    other = WaveformIdentity.from_trace(other_trace).sac_path(root)
    Path(tmp_path / "other.sac").replace(other)

    groups, errors = _group_targets(root, "*.sac")

    assert not errors
    assert sorted(len(files) for files in groups.values()) == [1, 2]
    bhz = next(files for key, files in groups.items() if key[3] == "BHZ")
    assert _merge_targets(bhz, root, remove_src=False) is None
    merged = WaveformIdentity.from_trace(first_trace).sac_path(root, merged=True)
    assert merged.is_file()
    assert len(read(merged)) == 1


def test_merge_rejects_path_that_disagrees_with_header(tmp_path):
    root = tmp_path / "archive"
    wrong = root / "NZ" / "WRONG" / "2025" / "wrong.sac"
    _sac(wrong)

    groups, errors = _group_targets(root, "*.sac")

    assert not groups
    assert "does not match the SAC header" in errors[0]
