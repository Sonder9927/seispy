"""Waveform decimation contracts."""

from importlib import import_module
from pathlib import Path
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


@pytest.mark.parametrize(
    "scenario",
    [
        "decimation_rejects_overlapping_output_tree",
        "failure_limits_issue_samples_and_writes_no_output",
        "failed_sac_decimation_batch_writes_no_output",
    ],
)
def test_decimation_outputs(tmp_path, scenario):
    source_root = tmp_path / "source"
    if scenario == "decimation_rejects_overlapping_output_tree":
        source_root.mkdir()
        with np.testing.assert_raises_regex(ValueError, "separate directory trees"):
            decimate.decimate_waveforms(
                source_root, (5,), output_dir=source_root / "output"
            )
    elif scenario == "failure_limits_issue_samples_and_writes_no_output":
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
    elif scenario == "failed_sac_decimation_batch_writes_no_output":
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


@pytest.mark.parametrize("output_format", [None, "mseed", "sac"])
def test_mseed_decimation_preserves_filtered_samples(tmp_path, output_format):
    from obspy import UTCDateTime

    source = tmp_path / "source"
    source.mkdir()
    trace = Trace(np.arange(4000, dtype=np.int32))
    trace.stats.update(
        dict(
            network="XX",
            station="AAA",
            channel="BHZ",
            sampling_rate=100,
            starttime=UTCDateTime("2024-01-01"),
        )
    )
    target = source / "day.mseed"
    trace.write(str(target), format="MSEED", encoding="STEIM2")
    expected = trace.copy()
    decimate._sac_compatible_decimate_trace(expected, (4,))
    output = tmp_path / "output"
    result = decimate._scipy_decimate_batch(
        [target],
        (4,),
        source,
        output,
        1,
        output_format=output_format,
    )
    assert result.succeeded == 1, result.issue_samples
    actual = read(str(output / ("day.sac" if output_format == "sac" else "day.mseed")))[
        0
    ]
    assert actual.stats.sampling_rate == 25
    assert actual.stats.starttime == trace.stats.starttime
    np.testing.assert_array_equal(actual.data, expected.data)
    if output_format != "sac":
        assert actual.stats.mseed.encoding == "FLOAT32"
    np.testing.assert_array_equal(read(str(target))[0].data, trace.data)
