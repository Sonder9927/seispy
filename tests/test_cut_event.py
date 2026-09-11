from importlib import import_module
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from obspy import UTCDateTime

module = import_module("seispy.event.cut")


def _event():
    start = UTCDateTime("2026-01-01T00:00:00")
    return {
        "start": start,
        "end": start + 10,
        "latitude": 1,
        "longitude": 2,
        "depth": 3,
        "mag": 4,
    }


def test_no_data_is_counted_with_one_sample(tmp_path):
    result = module.cut_event_station(
        _event(), {"station": "AAA"}, tmp_path, tmp_path / "output"
    )
    assert result.failed == 1
    assert result.no_data == 1
    assert result.samples[0].status == "no_data"


def test_read_failures_are_compact(tmp_path):
    inputs = [tmp_path / f"{index}.sac" for index in range(3)]
    index = SimpleNamespace(
        overlapping=lambda *args: tuple(SimpleNamespace(path=path) for path in inputs)
    )
    with patch.object(module.obspy, "read", side_effect=RuntimeError("bad SAC")):
        result = module.cut_event_station(
            _event(),
            {"station": "AAA"},
            tmp_path,
            tmp_path / "output",
            archive_index=index,
            max_error_samples=1,
        )
    assert result.read_failed == 3
    assert result.failed == 1
    assert len(result.samples) == 1


def test_summary_exports_json(tmp_path):
    summary = module.CutEventSummary(
        run_id="run",
        tasks_total=2,
        tasks_succeeded=1,
        tasks_failed=1,
        outputs_written=3,
        input_read_failed=1,
        no_data=0,
        error_samples=(
            module.CutEventIssue("event", "AAA", "input_read_failed", "bad", Path("a")),
        ),
        duration_seconds=1.0,
    )
    data = json.loads(summary.to_json(tmp_path / "summary.json").read_text())
    assert data["tasks_failed"] == 1
    assert data["error_samples"][0]["source"] == "a"
    assert not summary.ok
