from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Trace, UTCDateTime

remove_response = import_module("seispy.deconvolution.removal")


@pytest.mark.parametrize("failure", [None, "all", "one"])
def test_sac_batches(tmp_path, failure):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(5)]
    for source in sources:
        source.write_bytes(b"original")
    output_root = tmp_path / "processed"
    bad = sources[2]

    def run_sac(command, *, input, **kwargs):
        assert command == ["sac"]
        script = input.decode()
        if failure == "all" or (failure == "one" and f"r {bad}" in script):
            return SimpleNamespace(returncode=1, stderr=b"bad trace")
        for line in script.splitlines():
            if line.startswith("w "):
                Path(line[2:]).write_bytes(b"processed")
        return SimpleNamespace(returncode=0, stderr=b"")

    header = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
    pz = tmp_path / "responses.pz"
    pz.write_text("combined")
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
                pz,
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
    failures = {None: 0, "all": 5, "one": 1}[failure]
    assert summary.total == 5 and summary.failed == failures
    assert summary.succeeded == 5 - failures
    assert len(list(output_root.rglob("*.sac"))) == 5 - failures
    assert all(source.read_bytes() == b"original" for source in sources)
    if failure is None:
        assert run.call_count == 3
    else:
        assert summary.failed > 0


@pytest.mark.parametrize(
    "scenario",
    [
        "obspy_uses_epoch_covering_the_complete_trace",
        "obspy_rejects_trace_crossing_response_epoch",
    ],
)
def test_response_epoch_selection(scenario):
    class Container(list):
        pass

    if scenario == "obspy_uses_epoch_covering_the_complete_trace":
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
    elif scenario == "obspy_rejects_trace_crossing_response_epoch":
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


def test_sac_binary_products(tmp_path):
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
