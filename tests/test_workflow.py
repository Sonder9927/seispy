"""Observable workflow-run and safe-output contracts."""

import pytest

import json
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import patch

from seispy.workflow import (
    BatchRun,
    BatchSummary,
    commit_output,
    resolve_separate_directory_trees,
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


@pytest.mark.parametrize(
    "scenario",
    [
        "separate_directory_trees_are_resolved",
        "overlapping_directory_trees_are_rejected",
    ],
)
def test_directory_safety(tmp_path, scenario):
    if scenario == "separate_directory_trees_are_resolved":
        source, output = resolve_separate_directory_trees(
            tmp_path / "raw" / ".." / "mseed",
            tmp_path / "sac",
        )

        assert source == tmp_path / "mseed"
        assert output == tmp_path / "sac"
    elif scenario == "overlapping_directory_trees_are_rejected":
        source = tmp_path / "mseed"

        for output in (source, source / "processed", tmp_path):
            try:
                resolve_separate_directory_trees(source, output)
            except ValueError as error:
                assert "separate directory trees" in str(error)
            else:
                raise AssertionError(f"overlapping output accepted: {output}")


@pytest.mark.parametrize(
    "scenario",
    ["commit_output_does_not_overwrite_by_default", "commit_output_can_replace"],
)
def test_atomic_output_commit(tmp_path, scenario):
    destination = tmp_path / "output.sac"
    destination.write_bytes(b"existing")
    temporary = temporary_output_path(destination)
    temporary.write_bytes(b"new")
    if scenario == "commit_output_does_not_overwrite_by_default":
        try:
            commit_output(temporary, destination)
        except FileExistsError:
            pass
        else:
            raise AssertionError("FileExistsError was not raised")
        assert destination.read_bytes() == b"existing"
    elif scenario == "commit_output_can_replace":
        commit_output(temporary, destination, overwrite=True)
        assert destination.read_bytes() == b"new"


@pytest.mark.parametrize(
    "scenario",
    [
        "auto_report_and_path_serialization",
        "summary_status_and_automatic_issue_report",
        "batch_run_persists_progress_and_completes_summary",
        "batch_run_throttles_reports_and_progress_logs_independently",
        "checkpoint_serializes_nested_dataclass_issues",
        "issue_only_report_exists_during_run_and_is_removed_when_clean",
    ],
)
def test_batch_reports(tmp_path, scenario):
    if scenario == "auto_report_and_path_serialization":
        summary = _Summary(value=Path("data"), run_id="run", duration_seconds=0)
        saved = summary.save_report("task", True, tmp_path)
        assert saved.report_path == tmp_path / "task-run.json"
        data = json.loads(saved.report_path.read_text())
        assert data["value"] == "data"
        assert data["report_path"] == str(saved.report_path)
        assert summary.save_report("task", False, tmp_path).report_path is None
    elif scenario == "summary_status_and_automatic_issue_report":
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
    elif scenario == "batch_run_persists_progress_and_completes_summary":
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
    elif scenario == "batch_run_throttles_reports_and_progress_logs_independently":
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
    elif scenario == "checkpoint_serializes_nested_dataclass_issues":
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
    elif scenario == "issue_only_report_exists_during_run_and_is_removed_when_clean":
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


@pytest.mark.parametrize(
    "scenario",
    [
        "batch_run_marks_keyboard_interrupt",
        "batch_run_can_disable_artifacts",
        "batch_run_marks_unexpected_controller_failure",
    ],
)
def test_batch_termination(tmp_path, scenario):
    if scenario == "batch_run_marks_keyboard_interrupt":
        try:
            with BatchRun(
                "task", tmp_path, run_id="run", checkpoint_interval=3600
            ) as run:
                run.start(total=2, succeeded=0)
                run.checkpoint(completed=1, total=2, succeeded=1)
                stale = json.loads(
                    (tmp_path / "logs/reports/task-run.json").read_text()
                )
                assert stale["completed"] == 0
                raise KeyboardInterrupt
        except KeyboardInterrupt:
            pass

        report = json.loads((tmp_path / "logs/reports/task-run.json").read_text())
        log = (tmp_path / "logs/task-run.log").read_text()
        assert report["status"] == "interrupted"
        assert report["completed"] == 1
        assert "interrupted" in log
    elif scenario == "batch_run_can_disable_artifacts":
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
    elif scenario == "batch_run_marks_unexpected_controller_failure":
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
