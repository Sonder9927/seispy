"""Waveform decimation contracts."""

from importlib import import_module
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

decimate = import_module("seispy.waveform.decimation")


class _Trace:
    def __init__(self):
        self.stats = type("Stats", (), {"sampling_rate": 100.0})()
        self.data = np.arange(1000, dtype=np.float32)


class _Stream(list):
    def __init__(self):
        super().__init__([_Trace()])

    def write(self, filename, format):
        assert format == "SAC"
        Path(filename).write_bytes(b"smaller")


def test_output_directory_keeps_relative_path_and_name(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA" / "001"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original data")
    output = tmp_path / "output"
    with (
        patch.object(decimate.obspy, "read", return_value=_Stream()),
        patch.object(decimate, "_sac_fir_coefficients", return_value=np.ones(1)),
    ):
        result = decimate._scipy_decimate_batch((source,), (5,), source_root, output, 1)
    assert source.read_bytes() == b"original data"
    assert (output / "STA" / "001" / "trace.sac").read_bytes() == b"smaller"
    assert result.succeeded == 1


def test_decimation_rejects_overlapping_output_tree(tmp_path):
    source_root = tmp_path / "source"
    source_root.mkdir()

    with np.testing.assert_raises_regex(ValueError, "separate directory trees"):
        decimate.decimate_waveforms(
            source_root, (5,), output_dir=source_root / "output"
        )


def test_failure_limits_issue_samples_and_writes_no_output(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")
    output = tmp_path / "output"
    with patch.object(decimate.obspy, "read", side_effect=RuntimeError("bad file")):
        result = decimate._scipy_decimate_batch(
            tuple(sources), (5,), source_root, output, 1
        )
    assert result.failed == 3
    assert len(result.issue_samples) == 1
    assert not list(output.rglob("*.sac"))


def test_factors_have_sac_compatible_range():
    assert decimate._normalize_factors(5) == (5,)
    assert decimate._normalize_factors([5, 5, 4]) == (5, 5, 4)


def test_scipy_decimation_preserves_impulse_alignment():
    trace = _Trace()
    trace.data[:] = 0
    trace.data[500] = 1

    with patch.object(decimate, "_sac_fir_coefficients", return_value=np.ones(1)):
        decimate._sac_compatible_decimate_trace(trace, (5,))

    assert trace.stats.sampling_rate == 20.0
    assert np.argmax(trace.data) == 100


def test_failed_sac_decimation_batch_writes_no_output(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")
    failed = SimpleNamespace(returncode=1, stderr=b"decimation failed")
    output = tmp_path / "output"

    with patch.object(decimate.subprocess, "run", return_value=failed):
        summary = decimate._sac_decimate_batch(
            tuple(sources), (5,), source_root, output, 20
        )

    assert summary.failed == 3
    assert not list(output.rglob("*.sac"))
