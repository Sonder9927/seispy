import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

_MODULE_PATH = Path(__file__).parents[1] / "src" / "seispy" / "resample.py"
_SPEC = importlib.util.spec_from_file_location("resample_under_test", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
resample = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = resample
_SPEC.loader.exec_module(resample)


class _Stream:
    def resample(self, delta):
        assert delta == 1.0

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(b"smaller")


def test_output_directory_keeps_relative_path_and_name(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA" / "001"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original data")
    output = tmp_path / "output"
    with patch.object(resample.obspy, "read", return_value=_Stream()):
        result = resample.obspy_resample_by_station(
            source_root / "STA", "*.sac", (1.0,), source_root, output, False, 1
        )
    assert source.read_bytes() == b"original data"
    assert (output / "STA" / "001" / "trace.sac").read_bytes() == b"smaller"
    assert result.succeeded == 1


def test_remove_original_replaces_same_file_safely(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original data")
    with patch.object(resample.obspy, "read", return_value=_Stream()):
        result = resample.obspy_resample_by_station(
            station, "*.sac", (1.0,), source_root, None, True, 1
        )
    assert source.read_bytes() == b"smaller"
    assert result.succeeded == 1
    assert result.failed == 0


def test_failure_keeps_original_and_limits_samples(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")
    with patch.object(resample.obspy, "read", side_effect=RuntimeError("bad file")):
        result = resample.obspy_resample_by_station(
            station, "*.sac", (1.0,), source_root, None, True, 1
        )
    assert result.failed == 3
    assert len(result.error_samples) == 1
    assert all(source.read_bytes() == b"original" for source in sources)


def test_summary_exports_compact_json(tmp_path):
    summary = resample.ResampleSummary(
        "run-1", 2, 1, 1,
        (resample.ResampleResult(Path("a"), Path("b"), "bad"),),
        Path("output"), False, 1.2,
    )
    report = summary.to_json(tmp_path / "summary.json")
    data = json.loads(report.read_text())
    assert data["failed"] == 1
    assert data["error_samples"][0]["source"] == "a"
