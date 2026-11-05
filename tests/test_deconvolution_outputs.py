from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime
from obspy.core.inventory import Inventory

remove_response = import_module("seispy.deconvolution.removal")


class _WritableTrace:
    def __init__(self, content: bytes = b"processed"):
        self.content = content

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(self.content)


class _WritableStream(list):
    def __init__(self, content: bytes = b"processed"):
        super().__init__([_WritableTrace(content)])


def _readable_stream(npts=1000):
    trace = Trace(np.arange(npts, dtype=np.int32))
    trace.stats.network = "STA"
    trace.stats.station = "AAA"
    trace.stats.location = ""
    trace.stats.channel = "BHZ"
    trace.stats.starttime = UTCDateTime("2026-01-01")
    trace.stats.sampling_rate = 100.0
    return Stream([trace])


@pytest.mark.parametrize(
    "scenario",
    [
        "sac_backend_rejects_miniseed_before_reading_inventory",
        "failure_preserves_source_and_is_reported",
        "success_writes_output_and_preserves_source",
        "output_directory_must_not_overlap_source",
    ],
)
def test_source_protection(tmp_path, scenario):
    if scenario == "sac_backend_rejects_miniseed_before_reading_inventory":
        source_root = tmp_path / "source"
        station = source_root / "STA"
        station.mkdir(parents=True)
        source = station / "trace.mseed"
        source.write_bytes(b"miniseed")

        with (
            patch.object(remove_response.obspy, "read_inventory") as read_inventory,
            pytest.raises(ValueError, match='backend="sac" does not support MiniSEED'),
        ):
            remove_response.deconvolve_waveforms(
                source_root,
                tmp_path / "stations.xml",
                backend="sac",
                pattern="*.mseed",
                output_dir=tmp_path / "output",
                save_report=False,
            )

        read_inventory.assert_not_called()
    elif scenario == "failure_preserves_source_and_is_reported":
        source_root = tmp_path / "source"
        station = source_root / "STA"
        station.mkdir(parents=True)
        source = station / "trace.sac"
        source.write_bytes(b"original")

        def fail(*args, **kwargs):
            raise RuntimeError("response unavailable")

        with (
            patch.object(
                remove_response.obspy, "read", return_value=_readable_stream()
            ),
            patch.object(remove_response, "remove_response", side_effect=fail),
        ):
            results = remove_response._process_obspy_targets(
                [source],
                object(),
                source_root,
                tmp_path / "output",
                20,
                remove_response.DEFAULT_PRE_FILTER,
                (),
            )

        assert source.read_bytes() == b"original"
        assert not (tmp_path / "output" / "STA" / "trace.sac").exists()
        assert results.failed == 1
        assert results.succeeded == 0
        assert (
            results.issue_samples[0].destination
            == tmp_path / "output" / "STA" / "trace.sac"
        )
        assert results.issue_samples[0].error == "RuntimeError: response unavailable"
    elif scenario == "success_writes_output_and_preserves_source":
        source_root = tmp_path / "source"
        station = source_root / "STA"
        station.mkdir(parents=True)
        source = station / "trace.sac"
        source.write_bytes(b"original")
        with (
            patch.object(
                remove_response.obspy, "read", return_value=_readable_stream()
            ),
            patch.object(
                remove_response, "remove_response", return_value=_WritableStream()
            ),
            patch.object(remove_response, "_validate_output_trace"),
        ):
            results = remove_response._process_obspy_targets(
                [source],
                object(),
                source_root,
                tmp_path / "output",
                20,
                remove_response.DEFAULT_PRE_FILTER,
                (),
            )

        assert source.read_bytes() == b"original"
        assert (tmp_path / "output" / "STA" / "trace.sac").read_bytes() == b"processed"
        assert results.succeeded == 1
        assert results.failed == 0
    elif scenario == "output_directory_must_not_overlap_source":
        source = tmp_path / "source"
        source.mkdir()
        with pytest.raises(ValueError, match="separate directory trees"):
            remove_response._resolve_output_dir(source, source / "output")
        with pytest.raises(ValueError, match="separate directory trees"):
            remove_response._resolve_output_dir(source, tmp_path)


def _stream_with_short_and_long():
    def trace(npts, start):
        tr = Trace(np.arange(npts, dtype=np.int32))
        tr.stats.network = "STA"
        tr.stats.station = "AAA"
        tr.stats.location = ""
        tr.stats.channel = "BHZ"
        tr.stats.starttime = UTCDateTime(start)
        tr.stats.sampling_rate = 100.0
        return tr

    return Stream(
        [
            trace(10, "2026-01-01T00:00:00"),
            trace(1000, "2026-01-01T01:00:00"),
        ]
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "all_filtered_traces_skip_the_file",
        "partially_filtered_file_processes_remaining_traces",
        "trace_filter_can_be_disabled",
    ],
)
def test_trace_selection(tmp_path, scenario):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original")
    if scenario == "all_filtered_traces_skip_the_file":
        with patch.object(
            remove_response.obspy, "read", return_value=_readable_stream(npts=10)
        ):
            results = remove_response._process_obspy_targets(
                [source],
                object(),
                source_root,
                tmp_path / "output",
                20,
                remove_response.DEFAULT_PRE_FILTER,
                (),
            )

        assert results.skipped == 1
        assert results.succeeded == 0
        assert results.failed == 0
        assert results.traces_filtered == 1
        assert results.filter_samples[0].status == "trace_filtered"
        assert not (tmp_path / "output" / "STA" / "trace.sac").exists()
    elif scenario == "partially_filtered_file_processes_remaining_traces":
        with (
            patch.object(
                remove_response.obspy,
                "read",
                return_value=_stream_with_short_and_long(),
            ),
            patch.object(
                remove_response, "remove_response", return_value=_WritableStream()
            ),
            patch.object(remove_response, "_validate_output_trace"),
        ):
            results = remove_response._process_obspy_targets(
                [source],
                object(),
                source_root,
                tmp_path / "output",
                20,
                remove_response.DEFAULT_PRE_FILTER,
                (),
            )

        assert results.succeeded == 1
        assert results.skipped == 0
        assert results.traces_filtered == 1
        assert (tmp_path / "output" / "STA" / "trace.sac").read_bytes() == b"processed"
    elif scenario == "trace_filter_can_be_disabled":
        policy = remove_response.TraceFilter(
            min_duration_seconds=0.0, min_samples=0, reject_unusable_samples=False
        )

        with (
            patch.object(
                remove_response.obspy, "read", return_value=_readable_stream(npts=10)
            ),
            patch.object(
                remove_response, "remove_response", return_value=_WritableStream()
            ),
            patch.object(remove_response, "_validate_output_trace"),
        ):
            results = remove_response._process_obspy_targets(
                [source],
                object(),
                source_root,
                tmp_path / "output",
                20,
                remove_response.DEFAULT_PRE_FILTER,
                (),
                policy,
            )

        assert results.succeeded == 1
        assert results.traces_filtered == 0


@pytest.mark.parametrize(
    "scenario",
    [
        "input_files_are_discovered_recursively_once",
        "public_workflow_keeps_logs_under_output_directory",
    ],
)
def test_discovery_and_reports(tmp_path, scenario):
    if scenario == "input_files_are_discovered_recursively_once":
        source_root = tmp_path / "source"
        station = source_root / "STA"
        station.mkdir(parents=True)
        source = station / "trace.sac"
        source.write_bytes(b"original")
        assert remove_response._input_files(source_root, "*.sac") == (source,)
    elif scenario == "public_workflow_keeps_logs_under_output_directory":
        source = tmp_path / "waveforms"
        source.mkdir()
        suitability = SimpleNamespace(require_safe=Mock())

        with (
            patch.object(
                remove_response,
                "analyze_inventory",
                return_value=SimpleNamespace(response_suitability=suitability),
            ),
            patch.object(remove_response, "_input_files", return_value=()) as discover,
        ):
            summary = remove_response.deconvolve_waveforms(
                source,
                Inventory(networks=[], source="test"),
                output_dir=tmp_path / "deconvolved",
            )

        discover.assert_called_once_with(source.resolve(), "*.sac")
        assert summary.total == 0
        log_root = tmp_path / "deconvolved" / "logs"
        assert len(list(log_root.glob("deconvolution-*.log"))) == 1
        assert len(list((log_root / "reports").glob("deconvolution-*.json"))) == 1
        assert not (source / "logs").exists()
