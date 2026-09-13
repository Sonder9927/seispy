"""SAC header formatting contracts."""

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

module = import_module("seispy.waveform.headers")


class _Trace:
    def __init__(self):
        self.stats = SimpleNamespace(location="", sac=SimpleNamespace())

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(b"formatted")


def _metadata():
    event = {"latitude": 1, "longitude": 2, "mag": 3, "depth": 4}
    stations = {"AAA": {"latitude": 5, "longitude": 6}}
    return event, stations


def test_format_writes_safely_and_updates_header(tmp_path):
    event_dir = tmp_path / "source" / "20260102030405"
    event_dir.mkdir(parents=True)
    source = event_dir / "event.AAA.BHZ.sac"
    source.write_bytes(b"original")
    trace = _Trace()
    event, stations = _metadata()
    with patch.object(module.obspy, "read", return_value=[trace]):
        result = module.format_per_event(
            event_dir, event, tmp_path / "output", "*.sac", stations
        )
    destination = tmp_path / "output" / event_dir.name / f"{event_dir.name}.AAA.BHZ.sac"
    assert destination.read_bytes() == b"formatted"
    assert source.read_bytes() == b"original"
    assert trace.stats.station == "AAA"
    assert trace.stats.sac.evdp == 4
    assert result.succeeded == 1


def test_invalid_filename_does_not_delete_existing_output(tmp_path):
    event_dir = tmp_path / "source" / "20260102030405"
    event_dir.mkdir(parents=True)
    (event_dir / "invalid.sac").write_bytes(b"bad")
    output = tmp_path / "output" / event_dir.name / "existing.sac"
    output.parent.mkdir(parents=True)
    output.write_bytes(b"keep")
    event, stations = _metadata()
    result = module.format_per_event(
        event_dir, event, tmp_path / "output", "*.sac", stations
    )
    assert result.failed == 1
    assert output.read_bytes() == b"keep"


def test_conflict_is_not_overwritten(tmp_path):
    event_dir = tmp_path / "source" / "20260102030405"
    event_dir.mkdir(parents=True)
    (event_dir / "event.AAA.BHZ.sac").write_bytes(b"original")
    destination = tmp_path / "output" / event_dir.name / f"{event_dir.name}.AAA.BHZ.sac"
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")
    event, stations = _metadata()
    result = module.format_per_event(
        event_dir, event, tmp_path / "output", "*.sac", stations
    )
    assert result.conflicts == 1
    assert destination.read_bytes() == b"existing"
