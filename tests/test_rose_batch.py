import json
from dataclasses import dataclass
from pathlib import Path

from rose.batch import ReportMixin, auto_save_report, commit_output, temporary_output_path


@dataclass(frozen=True)
class _Summary(ReportMixin):
    run_id: str
    value: Path
    report_path: Path | None = None


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
    summary = _Summary("run", Path("data"))
    saved = auto_save_report(summary, "task", True, None, tmp_path)
    assert saved.report_path == tmp_path / "task-run.json"
    data = json.loads(saved.report_path.read_text())
    assert data["value"] == "data"
    assert data["report_path"] == str(saved.report_path)
    assert auto_save_report(summary, "task", False, None, tmp_path).report_path is None
