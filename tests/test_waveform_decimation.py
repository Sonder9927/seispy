"""Waveform decimation contracts."""

from importlib import import_module
from importlib.resources import files
from pathlib import Path
import hashlib
import shutil
import tempfile
from types import SimpleNamespace
from unittest.mock import patch

import numpy as np
import pytest
from obspy import Trace, read

decimate = import_module("seispy.waveform.decimation")


@pytest.mark.parametrize("factor", range(2, 8))
def test_scipy_decimation_needs_no_external_sac_files(factor, monkeypatch):
    """The actual filter must load from the package, even without SAC setup."""
    monkeypatch.delenv("SACAUX", raising=False)
    monkeypatch.delenv("SACHOME", raising=False)
    read_text = Path.read_text
    package_root = Path(decimate.__file__).parent

    def package_only(path, *args, **kwargs):
        assert path.is_relative_to(package_root), f"External FIR dependency: {path}"
        return read_text(path, *args, **kwargs)

    decimate._sac_fir_coefficients.cache_clear()
    monkeypatch.setattr(Path, "read_text", package_only)
    with patch.object(
        decimate.subprocess, "run", side_effect=AssertionError("SAC invoked")
    ):
        trace = Trace(np.zeros(42001, dtype=np.float32))
        trace.stats.sampling_rate = 100
        trace.data[21000] = 1
        decimate._sac_compatible_decimate_trace(trace, (factor,))

    assert trace.stats.sampling_rate == 100 / factor
    assert len(trace.data) == 42000 // factor + 1
    assert np.argmax(trace.data) == 21000 // factor
    assert np.isfinite(trace.data).all()


@pytest.mark.parametrize(
    "factor,digest",
    [
        (2, "5441630c98e124562a9fb8ed910862a33a04c5793e2fae83d28f9783d2bd5dbc"),
        (3, "8ffbcec15546cc2e0164d8f93cbde01aff84e4189fc822ceda5efde7ee53df5e"),
        (4, "494d1d9dac4a7422813eb9cb51d4aa8627c7e0506e8c2f96bcdd2b3f7382ed69"),
        (5, "170bc1c91bcee037133951eee4e338506c2721e10dcbe2f6a6149694d3a32422"),
        (6, "5989cfc3e85d99733314a9dc527b07217055872669e8f3751a1afa415acca870"),
        (7, "b6a99fd75c1db529f63afebeadafbdc329f34f370dd49533587bef5d3a2c1976"),
    ],
)
def test_bundled_filters_match_pinned_upstream(factor, digest):
    resource = files("seispy.waveform").joinpath("_fir", f"dec{factor}")
    assert hashlib.sha256(resource.read_bytes()).hexdigest() == digest


@pytest.mark.integration
@pytest.mark.parametrize("factors", [(2,), (3,), (4,), (5,), (6,), (7,), (4, 5, 5)])
def test_bundled_filters_preserve_sac_waveform_equivalence(factors):
    if shutil.which("sac") is None:
        pytest.skip("SAC executable is not installed")
    # Short paths avoid SAC's filename length limits. All outputs are temporary.
    with tempfile.TemporaryDirectory(prefix="fir-") as folder:
        root = Path(folder)
        source_root = root / "in"
        source_root.mkdir()
        source = source_root / "a.sac"
        data = np.random.default_rng(42).normal(size=42001).astype(np.float32)
        Trace(data, header={"sampling_rate": 100.0}).write(str(source), format="SAC")
        for backend in ("sac", "scipy"):
            worker = decimate._decimation_backend(backend)
            result = worker((source,), factors, source_root, root / backend, 1)
            assert result.succeeded == 1, result.issue_samples
        sac = read(str(root / "sac" / source.name))[0]
        scipy = read(str(root / "scipy" / source.name))[0]
        assert sac.stats.npts == scipy.stats.npts
        assert sac.stats.starttime == scipy.stats.starttime
        assert sac.stats.sampling_rate == pytest.approx(scipy.stats.sampling_rate)
        relative_error = np.linalg.norm(sac.data - scipy.data) / np.linalg.norm(
            sac.data
        )
        assert relative_error < 2e-6


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
