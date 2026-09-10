import importlib.util
import json
import sys
from pathlib import Path
from unittest.mock import patch

from obspy import UTCDateTime

_PATH = Path(__file__).parents[1] / "src" / "seispy" / "event" / "cut.py"
_SPEC = importlib.util.spec_from_file_location("cut_event_under_test", _PATH)
assert _SPEC is not None and _SPEC.loader is not None
module = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = module
_SPEC.loader.exec_module(module)


def _event():
    start = UTCDateTime("2026-01-01T00:00:00")
    return {"start": start, "end": start + 10, "latitude": 1,
            "longitude": 2, "depth": 3, "mag": 4}


def test_no_data_is_counted_with_one_sample(tmp_path):
    result = module.cut_event_station(
        _event(), {"station": "AAA"}, tmp_path, tmp_path / "output"
    )
    assert result.failed == 1
    assert result.no_data == 1
    assert result.samples[0].status == "no_data"


def test_read_failures_are_compact(tmp_path):
    inputs = [tmp_path / f"{index}.sac" for index in range(3)]
    with patch.object(module, "_target_paths", return_value=inputs), patch.object(
        module.obspy, "read", side_effect=RuntimeError("bad SAC")
    ):
        result = module.cut_event_station(
            _event(), {"station": "AAA"}, tmp_path, tmp_path / "output",
            max_error_samples=1,
        )
    assert result.read_failed == 3
    assert result.failed == 1
    assert len(result.samples) == 1


def test_summary_exports_json(tmp_path):
    summary = module.CutEventSummary(
        "run", 2, 1, 1, 3, 1, 0,
        (module.CutEventResult("event", "AAA", "input_read_failed", "bad", Path("a")),),
        1.0,
    )
    data = json.loads(summary.to_json(tmp_path / "summary.json").read_text())
    assert data["tasks_failed"] == 1
    assert data["error_samples"][0]["source"] == "a"
    assert not summary.ok
