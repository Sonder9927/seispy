import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

_PATH = Path(__file__).parents[1] / "src" / "seispy" / "collate" / "mseed2sac.py"
_SPEC = importlib.util.spec_from_file_location("mseed2sac_under_test", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
module = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = module
_SPEC.loader.exec_module(module)


class _Time:
    year = 2026
    julday = 8

    def strftime(self, value):
        return "010203"


class _Trace:
    stats = SimpleNamespace(
        network="NZ", station="AAA", location="", channel="BHZ",
        mseed=SimpleNamespace(dataquality="D"), starttime=_Time(),
    )

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(b"sac")


class _Stream(list):
    def get_gaps(self):
        return []

    def merge(self, **kwargs):
        return self


def test_conversion_writes_sac_and_can_remove_source(tmp_path):
    source = tmp_path / "input.miniseed"
    source.write_bytes(b"mseed")
    with patch.object(module.obspy, "read", return_value=_Stream([_Trace()])):
        result = module._convert_file(source, tmp_path / "output", True, 1)
    assert result.succeeded == 1
    assert result.traces_written == 1
    assert not source.exists()
    assert next((tmp_path / "output").rglob("*.sac")).read_bytes() == b"sac"


def test_conflict_does_not_overwrite_or_remove_source(tmp_path):
    source = tmp_path / "input.miniseed"
    source.write_bytes(b"mseed")
    destination = module._trace_destination(_Trace(), tmp_path / "output")
    destination.parent.mkdir(parents=True)
    destination.write_bytes(b"existing")
    with patch.object(module.obspy, "read", return_value=_Stream([_Trace()])):
        result = module._convert_file(source, tmp_path / "output", True, 1)
    assert result.failed == 1
    assert result.conflicts == 1
    assert source.exists()
    assert destination.read_bytes() == b"existing"


def test_partial_conversion_is_rolled_back(tmp_path):
    source = tmp_path / "input.miniseed"
    source.write_bytes(b"mseed")
    second = _Trace()
    second.stats = SimpleNamespace(**vars(_Trace.stats))
    second.stats.channel = "BHN"
    second.write = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("bad"))
    with patch.object(module.obspy, "read", return_value=_Stream([_Trace(), second])):
        result = module._convert_file(source, tmp_path / "output", True, 1)
    assert result.failed == 1
    assert source.exists()
    assert not list((tmp_path / "output").rglob("*.sac"))


def test_summary_json_is_compact(tmp_path):
    summary = module.Mseed2SacSummary(
        run_id="run",
        input_total=2,
        input_succeeded=1,
        input_failed=1,
        originals_removed=0,
        removal_failed=0,
        traces_written=1,
        output_conflicts=0,
        error_samples=(
            module.Mseed2SacIssue(Path("a"), "conversion_failed", "bad"),
        ),
        output_dir=Path("out"),
        duration_seconds=1.0,
    )
    data = json.loads(summary.to_json(tmp_path / "result.json").read_text())
    assert data["input_failed"] == 1
    assert len(data["error_samples"]) == 1
    assert not summary.ok
