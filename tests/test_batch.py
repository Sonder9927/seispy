import json
from dataclasses import dataclass
from pathlib import Path

from seispy._batch import (
    BatchSummary,
    commit_output,
    temporary_output_path,
)


@dataclass(frozen=True)
class _Summary(BatchSummary):
    value: Path
    has_issues: bool = False


def test_commit_output_does_not_overwrite_by_default(tmp_path):
    destination = tmp_path / "output.sac"
    destination.write_bytes(b"existing")
    temporary = temporary_output_path(destination)
    temporary.write_bytes(b"new")
    try:
        commit_output(temporary, destination)
    except FileExistsError:
        pass
    else:
        raise AssertionError("FileExistsError was not raised")
    assert destination.read_bytes() == b"existing"


def test_commit_output_can_replace(tmp_path):
    destination = tmp_path / "output.sac"
    destination.write_bytes(b"existing")
    temporary = temporary_output_path(destination)
    temporary.write_bytes(b"new")
    commit_output(temporary, destination, overwrite=True)
    assert destination.read_bytes() == b"new"


def test_auto_report_and_path_serialization(tmp_path):
    summary = _Summary(value=Path("data"), run_id="run", duration_seconds=0)
    saved = summary.save_report("task", True, tmp_path)
    assert saved.report_path == tmp_path / "task-run.json"
    data = json.loads(saved.report_path.read_text())
    assert data["value"] == "data"
    assert data["report_path"] == str(saved.report_path)
    assert summary.save_report("task", False, tmp_path).report_path is None


def test_summary_status_and_automatic_issue_report(tmp_path):
    clean = _Summary(value=Path("data"), run_id="clean", duration_seconds=0)
    problem = _Summary(
        value=Path("data"), run_id="problem", duration_seconds=0, has_issues=True
    )

    assert clean.ok
    assert not problem.ok
    assert clean.save_report("task", None, tmp_path).report_path is None
    assert problem.save_report("task", None, tmp_path).report_path == (
        tmp_path / "task-problem.json"
    )
