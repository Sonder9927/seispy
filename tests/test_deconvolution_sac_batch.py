from importlib import import_module
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Trace, UTCDateTime

remove_response = import_module("seispy.deconvolution.removal")


def test_deconvolution_summary_status_includes_processing_failures(tmp_path):
    summary = remove_response.DeconvolutionSummary(
        run_id="run",
        total=1,
        succeeded=0,
        failed=1,
        issue_samples=(),
        output_dir=tmp_path,
        duration_seconds=0.1,
    )

    assert summary.has_issues
    assert not summary.ok


def test_sac_deconv_reuses_process_for_bounded_file_batches(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    for index in range(5):
        (station / f"trace-{index}.sac").write_bytes(b"original")
    output_root = tmp_path / "processed"

    def run_sac(command, *, input, **kwargs):
        assert command == ["sac"]
        for line in input.decode().splitlines():
            if line.startswith("w "):
                Path(line[2:]).write_bytes(b"processed")
        return SimpleNamespace(returncode=0, stderr=b"")

    header = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
    combined_pz = tmp_path / "responses.pz"
    combined_pz.write_text("combined")
    with (
        patch.object(remove_response.obspy, "read", return_value=[header]),
        patch.object(remove_response, "_response_epoch_for_trace"),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac) as run,
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        summaries = [
            remove_response._process_sac_batch(
                batch,
                object(),
                combined_pz,
                source_root,
                output_root,
                20,
                remove_response.DEFAULT_PRE_FILTER,
                {},
                (),
            )
            for batch in remove_response._batched(
                remove_response._input_files(source_root, "*.sac"), 2
            )
        ]
        summary = remove_response._combine_batches(summaries, 20)

    assert run.call_count == 3
    assert summary.total == 5
    assert summary.succeeded == 5
    assert summary.failed == 0
    assert len(list(output_root.rglob("*.sac"))) == 5


def test_sac_batch_failure_preserves_all_sources(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")

    failed_process = SimpleNamespace(returncode=1, stderr=b"SAC batch failed")
    header = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
    output_root = tmp_path / "processed"
    combined_pz = tmp_path / "responses.pz"
    combined_pz.write_text("combined")
    with (
        patch.object(remove_response.obspy, "read", return_value=[header]),
        patch.object(remove_response, "_response_epoch_for_trace"),
        patch.object(remove_response.subprocess, "run", return_value=failed_process),
    ):
        summary = remove_response._process_sac_batch(
            sources,
            object(),
            combined_pz,
            source_root,
            output_root,
            20,
            remove_response.DEFAULT_PRE_FILTER,
            {},
            (),
        )

    assert summary.failed == 3
    assert summary.succeeded == 0
    assert all(source.read_bytes() == b"original" for source in sources)
    assert not list(output_root.rglob("*.sac"))


def test_sac_batch_failure_is_bisected_to_isolate_bad_file(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(4)]
    for source in sources:
        source.write_bytes(b"original")
    bad = sources[2]
    output_root = tmp_path / "processed"

    def run_sac(command, *, input, **kwargs):
        script = input.decode()
        if f"r {bad}" in script:
            return SimpleNamespace(returncode=1, stderr=b"bad trace")
        for line in script.splitlines():
            if line.startswith("w "):
                Path(line[2:]).write_bytes(b"processed")
        return SimpleNamespace(returncode=0, stderr=b"")

    header = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
    combined_pz = tmp_path / "responses.pz"
    combined_pz.write_text("combined")
    with (
        patch.object(remove_response.obspy, "read", return_value=[header]),
        patch.object(remove_response, "_response_epoch_for_trace"),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac),
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        summary = remove_response._process_sac_batch(
            sources,
            object(),
            combined_pz,
            source_root,
            output_root,
            20,
            remove_response.DEFAULT_PRE_FILTER,
            {},
            (),
        )

    assert summary.succeeded == 3
    assert summary.failed == 1
    assert len(list(output_root.rglob("*.sac"))) == 3


def test_deconvolved_output_validation_rejects_non_finite_data(tmp_path):
    source = tmp_path / "source.sac"
    output = tmp_path / "output.sac"
    header = {
        "sampling_rate": 100.0,
        "starttime": UTCDateTime("2026-01-01"),
    }
    Trace(data=np.arange(10, dtype=np.float32), header=header).write(
        str(source), format="SAC"
    )
    invalid = np.arange(10, dtype=np.float32)
    invalid[4] = np.nan
    invalid_trace = Trace(data=invalid, header=header)
    invalid_trace.write(str(output), format="SAC")

    with pytest.raises(ValueError, match="NaN or infinite"):
        remove_response._validate_deconvolved_file(
            output,
            expected=Trace(data=np.arange(10), header=header),
            processed=[invalid_trace],
        )


def test_adaptive_batch_size_keeps_eight_scheduling_waves():
    assert remove_response._batch_size(10_000, 40, None) == 32
    assert remove_response._batch_size(80, 10, None) == 1
    assert remove_response._batch_size(10_000, 40, 7) == 7


def test_sac_inventory_is_exported_to_one_combined_pz(tmp_path):
    class InventoryStub(list):
        pass

    class StationStub(list):
        @property
        def channels(self):
            return self

    station = StationStub(
        [SimpleNamespace(response=None), SimpleNamespace(response=None)]
    )
    inventory = InventoryStub([[station]])
    inventory.write = Mock()
    inventory.copy = Mock(return_value=inventory)

    def write_pz(filename, format):
        assert format == "SACPZ"
        Path(filename).write_text(
            "* NETWORK : NZ\n* STATION : AAA\n* LOCATION : 10\n"
            "* CHANNEL : HHZ\n* START : 2020-01-01\n* END : 2021-01-01\n"
            "* INPUT UNIT : M\nZEROS 3\n"
            "* NETWORK : NZ\n* STATION : BBB\n* LOCATION : 10\n"
            "* CHANNEL : HHZ\n* START : 2020-01-01\n* END : 2021-01-01\n"
            "* INPUT UNIT : M\nZEROS 3\n"
        )

    inventory.write.side_effect = write_pz
    destination = remove_response._write_combined_sacpz(
        inventory, tmp_path / "responses.pz"
    )

    assert destination == tmp_path / "responses.pz"
    inventory.copy.assert_called_once_with()
    inventory.write.assert_called_once_with(str(destination), format="SACPZ")


def test_obspy_uses_epoch_covering_the_complete_trace():
    class Container(list):
        pass

    channel = SimpleNamespace(
        start_date=remove_response.obspy.UTCDateTime("2024-01-01"),
        end_date=remove_response.obspy.UTCDateTime("2025-01-01"),
    )
    selected = Container([Container([Container([channel])])])
    selected.get_response = Mock()
    inventory = Mock()
    inventory.select.return_value = selected
    stats = SimpleNamespace(
        network="NZ",
        station="AAA",
        location="10",
        channel="BHZ",
        sampling_rate=100.0,
        starttime=remove_response.obspy.UTCDateTime("2024-06-01"),
        endtime=remove_response.obspy.UTCDateTime("2024-06-02"),
    )
    trace = Mock()
    trace.id = "NZ.AAA.10.BHZ"
    trace.stats = stats
    trace.data = 1.0

    class Stream(list):
        def get_gaps(self):
            return []

        def merge(self, **kwargs):
            return self

    with patch.object(remove_response.obspy, "read", return_value=Stream([trace])):
        remove_response.remove_response_from_file("trace.sac", inventory)

    assert trace.remove_response.call_args.kwargs["inventory"] is selected


def test_obspy_rejects_trace_crossing_response_epoch():
    class Container(list):
        pass

    epoch_end = remove_response.obspy.UTCDateTime("2024-06-01T12:00:00")
    channel = SimpleNamespace(
        start_date=remove_response.obspy.UTCDateTime("2024-01-01"),
        end_date=epoch_end,
    )
    selected = Container([Container([Container([channel])])])
    selected.get_response = Mock()
    inventory = Mock()
    inventory.select.return_value = selected
    trace = SimpleNamespace(
        id="NZ.AAA.10.BHZ",
        stats=SimpleNamespace(
            network="NZ",
            station="AAA",
            location="10",
            channel="BHZ",
            starttime=remove_response.obspy.UTCDateTime("2024-06-01"),
            endtime=remove_response.obspy.UTCDateTime("2024-06-02"),
        ),
    )

    with pytest.raises(ValueError, match="response changes within"):
        remove_response._response_epoch_for_trace(inventory, trace)
