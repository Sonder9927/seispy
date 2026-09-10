import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np

_MODULE_PATH = Path(__file__).parents[1] / "src" / "seispy" / "decimate.py"
_SPEC = importlib.util.spec_from_file_location("decimate_under_test", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
decimate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = decimate
_SPEC.loader.exec_module(decimate)


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
        result = decimate._scipy_decimate_batch(
            (source,), (5,), source_root, output, False, 1
        )
    assert source.read_bytes() == b"original data"
    assert (output / "STA" / "001" / "trace.sac").read_bytes() == b"smaller"
    assert result.succeeded == 1


def test_remove_original_replaces_same_file_safely(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    source.write_bytes(b"original data")
    with (
        patch.object(decimate.obspy, "read", return_value=_Stream()),
        patch.object(decimate, "_sac_fir_coefficients", return_value=np.ones(1)),
    ):
        result = decimate._scipy_decimate_batch(
            (source,), (5,), source_root, None, True, 1
        )
    assert source.read_bytes() == b"smaller"
    assert result.succeeded == 1
    assert result.failed == 0


def test_failure_keeps_original_and_limits_samples(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")
    with patch.object(decimate.obspy, "read", side_effect=RuntimeError("bad file")):
        result = decimate._scipy_decimate_batch(
            tuple(sources), (5,), source_root, None, True, 1
        )
    assert result.failed == 3
    assert len(result.error_samples) == 1
    assert all(source.read_bytes() == b"original" for source in sources)


def test_summary_exports_compact_json(tmp_path):
    summary = decimate.DecimationSummary(
        "run-1",
        2,
        1,
        1,
        (decimate.DecimationResult(Path("a"), Path("b"), "bad"),),
        Path("output"),
        False,
        1.2,
    )
    report = summary.to_json(tmp_path / "summary.json")
    data = json.loads(report.read_text())
    assert data["failed"] == 1
    assert data["error_samples"][0]["source"] == "a"
    assert not summary.ok


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


def test_file_batches_cross_station_boundaries(tmp_path):
    source_root = tmp_path / "source"
    for station, count in (("A", 1), ("B", 2), ("C", 2)):
        directory = source_root / station
        directory.mkdir(parents=True)
        for index in range(count):
            (directory / f"tracec-{index}.sac").write_bytes(b"original")

    targets = decimate._input_files(source_root, "*.sac")
    batches = tuple(decimate._batched(targets, 2))

    assert tuple(map(len, batches)) == (2, 2, 1)
    assert batches[0][0].parent.name == "A"
    assert batches[0][1].parent.name == "B"


def test_failed_sac_decimation_batch_keeps_originals(tmp_path):
    source_root = tmp_path / "source"
    station = source_root / "STA"
    station.mkdir(parents=True)
    sources = [station / f"trace-{index}.sac" for index in range(3)]
    for source in sources:
        source.write_bytes(b"original")
    failed = SimpleNamespace(returncode=1, stderr=b"decimation failed")

    with patch.object(decimate.subprocess, "run", return_value=failed):
        summary = decimate._sac_decimate_batch(
            tuple(sources), (5,), source_root, None, True, 20
        )

    assert summary.failed == 3
    assert all(source.read_bytes() == b"original" for source in sources)
