"""Observable workflow-run and safe-output contracts."""

import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    temporary_output_path,
)


@dataclass(frozen=True)
class _Issue:
    source: Path
    error: str


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


def test_batch_run_persists_progress_and_completes_summary(tmp_path):
    with BatchRun(
        "task",
        tmp_path,
        run_id="run",
        checkpoint_interval=0,
        progress_log_interval=0,
    ) as run:
        run.start(total=3, succeeded=0, failed=0)
        run.checkpoint(completed=1, total=3, succeeded=1, failed=0)
        progress = json.loads(run.report_path.read_text())
        assert progress["status"] == "running"
        assert progress["completed"] == 1
        summary = run.complete(
            _Summary(
                value=Path("data"),
                run_id="run",
                duration_seconds=0,
            )
        )

    assert summary.status == "completed"
    assert summary.report_path.is_file()
    assert summary.log_path.is_file()
    assert json.loads(summary.report_path.read_text())["status"] == "completed"
    assert "progress=1/3" in summary.log_path.read_text()


def test_batch_run_throttles_reports_and_progress_logs_independently(tmp_path):
    with BatchRun(
        "task",
        tmp_path,
        run_id="run",
        checkpoint_interval=5,
        progress_log_interval=60,
    ) as run:
        run.start(total=2, succeeded=0)
        run._last_checkpoint = 100
        run._last_progress_log = 100

        with patch("seispy.workflow.time.monotonic", return_value=101):
            run.checkpoint(completed=1, total=2, succeeded=1)

        report = json.loads(run.report_path.read_text())
        assert report["completed"] == 0
        assert "progress=1/2" not in run.log_path.read_text()

        with patch("seispy.workflow.time.monotonic", return_value=106):
            run.checkpoint(completed=1, total=2, succeeded=1)

        report = json.loads(run.report_path.read_text())
        assert report["completed"] == 1
        assert "progress=1/2" not in run.log_path.read_text()

        with patch("seispy.workflow.time.monotonic", return_value=107):
            run.checkpoint(completed=2, total=2, succeeded=2)

        report = json.loads(run.report_path.read_text())
        assert report["completed"] == 2
        assert "progress=2/2" in run.log_path.read_text()
        summary = run.complete(
            _Summary(value=Path("data"), run_id="run", duration_seconds=0)
        )

    assert summary.status == "completed"


def test_checkpoint_serializes_nested_dataclass_issues(tmp_path):
    issue = _Issue(Path("bad.mseed"), "InternalMSEEDWarning: invalid Steim1")

    with BatchRun("task", tmp_path, run_id="run", checkpoint_interval=0) as run:
        run.start(total=2, issue_samples=())
        run.checkpoint(
            completed=1,
            total=2,
            failed=1,
            issue_samples=(issue,),
        )
        report = json.loads(run.report_path.read_text())

    assert report["issue_samples"] == [
        {"source": "bad.mseed", "error": "InternalMSEEDWarning: invalid Steim1"}
    ]


def test_batch_run_marks_keyboard_interrupt(tmp_path):
    try:
        with BatchRun("task", tmp_path, run_id="run", checkpoint_interval=3600) as run:
            run.start(total=2, succeeded=0)
            run.checkpoint(completed=1, total=2, succeeded=1)
            stale = json.loads((tmp_path / "logs/reports/task-run.json").read_text())
            assert stale["completed"] == 0
            raise KeyboardInterrupt
    except KeyboardInterrupt:
        pass

    report = json.loads((tmp_path / "logs/reports/task-run.json").read_text())
    log = (tmp_path / "logs/task-run.log").read_text()
    assert report["status"] == "interrupted"
    assert report["completed"] == 1
    assert "interrupted" in log


def test_batch_run_can_disable_artifacts(tmp_path):
    with BatchRun(
        "task", tmp_path, run_id="run", save_report=False, save_log=False
    ) as run:
        run.start(total=0)
        summary = run.complete(
            _Summary(value=Path("data"), run_id="run", duration_seconds=0)
        )

    assert summary.report_path is None
    assert summary.log_path is None
    assert not (tmp_path / "logs").exists()


def test_batch_run_marks_unexpected_controller_failure(tmp_path):
    try:
        with BatchRun("task", tmp_path, run_id="failed") as run:
            run.start(total=2, succeeded=0)
            raise RuntimeError("controller stopped")
    except RuntimeError:
        pass

    report = json.loads(
        (tmp_path / "logs" / "reports" / "task-failed.json").read_text()
    )
    assert report["status"] == "failed"
    assert report["completed"] == 0
    assert report["error"] == "RuntimeError: controller stopped"


def test_issue_only_report_exists_during_run_and_is_removed_when_clean(tmp_path):
    report = tmp_path / "logs" / "reports" / "task-clean.json"
    with BatchRun(
        "task", tmp_path, run_id="clean", save_report=None, save_log=False
    ) as run:
        run.start(total=0)
        assert report.is_file()
        summary = run.complete(
            _Summary(value=Path("data"), run_id="clean", duration_seconds=0)
        )

    assert summary.report_path is None
    assert not report.exists()
