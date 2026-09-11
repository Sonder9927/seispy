from importlib import import_module
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Trace, UTCDateTime

remove_response = import_module("seispy.response.remove_response")


def test_deconvolution_summary_status_includes_removal_failures():
    summary = remove_response.DeconvolutionSummary(
        run_id="run",
        total=1,
        succeeded=1,
        failed=0,
        removal_failed=1,
        response_conflicts=0,
        issue_samples=(),
        output_dir=None,
        remove_original=True,
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

    sampling_rate = SimpleNamespace(
        stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000)
    )
    with (
        patch.object(remove_response.obspy, "read", return_value=[sampling_rate]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac) as run,
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        summary = remove_response.sac_deconv(
            station,
            "*.sac",
            "response.pz",
            source_root,
            output_root,
            False,
            20,
            batch_size=2,
        )

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
    sampling_rate = SimpleNamespace(
        stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000)
    )
    with (
        patch.object(remove_response.obspy, "read", return_value=[sampling_rate]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", return_value=failed_process),
    ):
        summary = remove_response.sac_deconv(
            station,
            "*.sac",
            "response.pz",
            source_root,
            None,
            True,
            20,
        )

    assert summary.failed == 3
    assert summary.succeeded == 0
    assert all(source.read_bytes() == b"original" for source in sources)
    assert not list(station.glob("*.deconv.sac"))


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
    with (
        patch.object(remove_response.obspy, "read", return_value=[header]),
        patch.object(
            remove_response,
            "_sac_pz_for_trace",
            return_value=tmp_path / "response.pz",
        ),
        patch.object(remove_response.subprocess, "run", side_effect=run_sac),
        patch.object(remove_response, "_validate_deconvolved_file"),
    ):
        summary = remove_response.sac_deconv(
            station,
            "*.sac",
            object(),
            source_root,
            output_root,
            False,
            20,
            batch_size=4,
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
            source, output, processed=[invalid_trace]
        )


def test_response_preflight_counts_only_affected_files(tmp_path):
    station = tmp_path / "AAA"
    station.mkdir()
    targets = [station / "one.sac", station / "two.sac"]
    for target in targets:
        target.write_bytes(b"trace")
    inv = Mock()
    inv.select.return_value = inv
    traces = [SimpleNamespace(id="NZ.AAA..BHZ"), SimpleNamespace(id="NZ.AAA..BHZ")]

    with (
        patch.object(
            remove_response, "_inventory_has_overlapping_epochs", return_value=True
        ),
        patch.object(
            remove_response.obspy, "read", side_effect=[[item] for item in traces]
        ),
        patch.object(
            remove_response,
            "_response_epoch_for_trace",
            side_effect=[ValueError("ambiguous"), (object(), object())],
        ),
    ):
        count = remove_response._preflight_response_conflicts([station], "*.sac", inv)

    assert count == 1


def test_sac_response_is_selected_by_full_id_and_epoch_and_cached(tmp_path):
    class Container(list):
        pass

    channel = SimpleNamespace(
        start_date=remove_response.obspy.UTCDateTime("2024-01-01"),
        end_date=remove_response.obspy.UTCDateTime("2025-01-01"),
    )
    selected = Container([Container([Container([channel])])])
    selected.get_response = Mock()

    def write_pz(filename, format):
        assert format == "SACPZ"
        Path(filename).write_text("* INPUT UNIT : M\nZEROS 3\n")

    selected.write = Mock(side_effect=write_pz)
    inventory = Mock()
    inventory.select.return_value = selected
    stats = SimpleNamespace(
        network="NZ",
        station="AAA",
        location="10",
        channel="BHZ",
        starttime=remove_response.obspy.UTCDateTime("2024-06-01"),
        endtime=remove_response.obspy.UTCDateTime("2024-06-02"),
    )
    trace = SimpleNamespace(id="NZ.AAA.10.BHZ", stats=stats)
    cache = {}

    first = remove_response._sac_pz_for_trace(inventory, trace, tmp_path, cache)
    second = remove_response._sac_pz_for_trace(inventory, trace, tmp_path, cache)

    assert first == second
    assert selected.write.call_count == 1
    inventory.select.assert_called_with(
        network="NZ",
        station="AAA",
        location="10",
        channel="BHZ",
        time=stats.starttime,
    )
    assert selected.get_response.call_count == 2


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
        remove_response.stream_removed_response("trace.sac", inventory)

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
