"""Event waveform-cutting contracts."""

import pytest

from importlib import import_module
from types import SimpleNamespace
from unittest.mock import patch

from obspy import UTCDateTime
from seispy.archive import WaveformIdentity
from seispy.event.archive_index import WaveformRecord

module = import_module("seispy.event.cutting")


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


@pytest.mark.parametrize(
    "scenario",
    [
        "no_data_is_counted_without_issue_samples",
        "read_failures_are_compact",
        "report_summarizes_no_data_without_hiding_errors",
    ],
)
def test_cutting_failures(tmp_path, scenario, caplog):
    if scenario == "no_data_is_counted_without_issue_samples":
        result = module.cut_event_station(
            _event(),
            {"network": "NZ", "station": "AAA"},
            tmp_path / "output",
            archive_index=SimpleNamespace(overlapping=lambda *args: ()),
        )
        assert result.failed == 0
        assert result.skipped == result.no_data == 1
        assert not result.samples
    elif scenario == "read_failures_are_compact":
        inputs = [tmp_path / f"{index}.sac" for index in range(3)]
        index = SimpleNamespace(
            overlapping=lambda *args: tuple(
                WaveformRecord(
                    path,
                    WaveformIdentity("NZ", "AAA", "", "BHZ", _event()["start"]),
                    _event()["start"],
                    _event()["end"],
                    1.0,
                )
                for path in inputs
            )
        )
        with patch.object(module.obspy, "read", side_effect=RuntimeError("bad SAC")):
            result = module.cut_event_station(
                _event(),
                {"network": "NZ", "station": "AAA"},
                tmp_path / "output",
                archive_index=index,
                max_error_samples=1,
            )
        assert result.read_failed == 3
        assert result.failed == 1
        assert len(result.samples) == 1

    elif scenario == "report_summarizes_no_data_without_hiding_errors":
        import json
        import numpy as np
        import pandas as pd
        from obspy import Trace

        source = tmp_path / "source"
        source.mkdir()
        trace = Trace(
            np.arange(101, dtype=np.float32),
            header={
                "network": "NZ",
                "station": "AAA",
                "channel": "BHZ",
                "starttime": _event()["start"],
                "sampling_rate": 1,
            },
        )
        trace.write(str(source / "input.sac"), format="SAC")
        catalog = tmp_path / "events.csv"
        missing = [
            dict(
                time=str(_event()["start"] - 86400 - i * 60),
                latitude=1,
                longitude=2,
                depth=3,
                mag=4,
            )
            for i in range(12)
        ]
        pd.DataFrame(missing).to_csv(catalog, index=False)
        summary = module.cut_event_waveforms(
            source,
            catalog,
            output_dir=tmp_path / "missing",
            time_window=10,
            max_workers=1,
            max_error_samples=1,
        )
        assert summary.failed == 0
        assert summary.total == summary.skipped == summary.no_data == 12
        assert summary.has_issues  # WARN coverage remains visible to automatic reports.
        warnings = [
            record
            for record in caplog.records
            if summary.run_id in record.getMessage()
            and "status=no_data" in record.getMessage()
        ]
        assert len(warnings) == 1 and warnings[0].levelname == "WARNING"
        assert not any(record.levelname == "ERROR" for record in caplog.records)
        assert not summary.issue_samples
        report = json.loads(summary.report_path.read_text())
        assert not report["issue_samples"]
        assert summary.src_path == source.resolve()
        assert summary.output_path == (tmp_path / "missing").resolve()
        assert report["src_path"] == str(summary.src_path)
        assert report["output_path"] == str(summary.output_path)
        assert "error_samples_truncated" not in summary.log_path.read_text()

        pd.DataFrame(
            [
                *missing,
                dict(
                    time=str(_event()["start"]), latitude=1, longitude=2, depth=3, mag=4
                ),
            ]
        ).to_csv(catalog, index=False)
        with patch.object(
            module, "_write_event_sac", side_effect=OSError("write failed")
        ):
            summary = module.cut_event_waveforms(
                source,
                catalog,
                output_dir=tmp_path / "mixed",
                time_window=10,
                max_workers=1,
                max_error_samples=1,
            )
        assert summary.skipped == summary.no_data == 12 and summary.failed == 1
        assert summary.total == summary.succeeded + summary.failed + summary.skipped
        warnings = [
            record
            for record in caplog.records
            if summary.run_id in record.getMessage()
            and "status=no_data" in record.getMessage()
        ]
        assert len(warnings) == 1 and warnings[0].levelname == "WARNING"
        assert any(
            record.levelname == "ERROR"
            and "status=output_failed" in record.getMessage()
            for record in caplog.records
        )
        assert len(summary.issue_samples) == 1
        assert summary.issue_samples[0].status == "output_failed"
        report = json.loads(summary.report_path.read_text())
        assert report["no_data"] == 12
        assert report["src_path"] == str(source.resolve())
        assert report["output_path"] == str((tmp_path / "mixed").resolve())
        assert [issue["status"] for issue in report["issue_samples"]] == [
            "output_failed"
        ]
        log = summary.log_path.read_text()
        assert log.count("status=no_data") == 1
        assert "error_samples_truncated" not in log
