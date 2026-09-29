"""Workflow counts and outputs survive pool replacement."""

from importlib import import_module
from unittest.mock import Mock

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime, read

from seispy import waveform


def _decimate_stub(targets, factors, source, output, limit):
    module = import_module("seispy.waveform.decimation")
    for target in targets:
        (output / target.name).write_text("processed")
    return module._WorkerSummary(total=len(targets), succeeded=len(targets))


@pytest.mark.parametrize("operation", ["archive", "filter", "convert"])
def test_recycled_outputs_match_single_pool(tmp_path, operation):
    source = tmp_path / "source"
    source.mkdir()
    for day in range(1, 4):
        trace = Trace(
            np.arange(2000, dtype=np.int32),
            header=dict(
                network="NZ",
                station="AAA",
                location="10",
                channel="HHZ",
                sampling_rate=100,
                starttime=UTCDateTime(2024, 1, day),
            ),
        )
        Stream([trace]).write(str(source / f"{day}.mseed"), format="MSEED")
    outputs = []
    for quota in (None, 2):
        output = tmp_path / str(quota)
        kwargs = dict(
            max_workers=1, files_per_pool=quota, save_report=False, save_log=False
        )
        if operation == "archive":
            summary = waveform.archive_waveforms(
                source, output, pattern="*.mseed", **kwargs
            )
        elif operation == "filter":
            summary = waveform.filter_waveforms(source, output, batch_size=3, **kwargs)
        else:
            summary = waveform.convert_mseed_to_sac(
                source, output, pattern="*.mseed", batch_size=3, **kwargs
            )
        assert summary.total == summary.succeeded == 3
        assert summary.failed == 0
        files = {
            p.relative_to(output): read(p)[0] for p in output.rglob("*") if p.is_file()
        }
        outputs.append(files)
    assert outputs[0].keys() == outputs[1].keys()
    for name in outputs[0]:
        assert outputs[0][name].stats.starttime == outputs[1][name].stats.starttime
        np.testing.assert_array_equal(outputs[0][name].data, outputs[1][name].data)


def test_decimation_scheduler_aggregates_across_generations(tmp_path):
    module = import_module("seispy.waveform.decimation")
    targets = [tmp_path / str(i) for i in range(5)]
    result = module._run_decimation_batches(
        targets,
        _decimate_stub,
        (2,),
        tmp_path,
        tmp_path,
        1,
        1,
        1,
        Mock(),
        batch_size=3,
        files_per_pool=2,
    )
    assert result.total == result.succeeded == 5
    assert result.failed == 0
    assert all(target.read_text() == "processed" for target in targets)
