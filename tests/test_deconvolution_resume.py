"""Resume from complete output sets using disposable SAC/MiniSEED fixtures."""

from importlib import import_module
import inspect
import json
from unittest.mock import patch

import numpy as np
import pytest
from obspy import Stream, Trace, UTCDateTime, read
from obspy.core.inventory import Channel, Inventory, Network, Station
from obspy.core.inventory.response import Response

from seispy.waveform import TraceFilter

removal = import_module("seispy.deconvolution.removal")
POLICY = TraceFilter(min_duration_seconds=0, min_samples=2)


@pytest.fixture(params=["SAC", "MSEED"])
def case(tmp_path, request):
    source = tmp_path / "source"
    station = source / "XX" / "TEST"
    station.mkdir(parents=True)
    stream = Stream()
    for index in range(1 if request.param == "SAC" else 2):
        stream += Trace(
            np.random.default_rng(index).normal(size=30001).astype(np.float32),
            header=dict(
                network="XX",
                station="TEST",
                location="",
                channel="BHZ",
                sampling_rate=100,
                starttime=UTCDateTime("2024-01-01") + index * 600,
            ),
        )
    target = station / ("day.sac" if request.param == "SAC" else "day.mseed")
    stream.write(str(target), format=request.param)
    return source, target, tmp_path / "output", stream


def process(stream, inventory, *, decimate_factors, **kwargs):
    stream = stream.copy()
    for trace in stream:
        if decimate_factors:
            removal._sac_compatible_decimate_trace(trace, decimate_factors)
        trace.data *= 2
    return stream


def run(case, *, factors=(), overwrite=True, post_factors=()):
    source, target, output, _ = case
    return removal._process_one_obspy_target(
        target,
        object(),
        source,
        output,
        20,
        removal.DEFAULT_PRE_FILTER,
        factors,
        POLICY,
        overwrite=overwrite,
        post_decimate_factors=post_factors,
    )


def destinations(case):
    source, target, output, stream = case
    return removal._obspy_destinations(target, stream, source, output)


@pytest.mark.parametrize("factors", [(), (4, 5, 5)])
def test_complete_outputs_skip_response_computation_and_preserve_bytes(case, factors):
    with patch.object(removal, "remove_response", side_effect=process):
        assert run(case, factors=factors).succeeded == 1
    outputs = destinations(case)
    before = [(p.read_bytes(), p.stat().st_mtime_ns) for p in outputs]
    with patch.object(
        removal, "remove_response", side_effect=AssertionError("recomputed")
    ):
        result = run(case, factors=factors, overwrite=False)
    assert result.total == result.skipped == result.existing_skipped == 1
    assert result.succeeded == result.failed == 0
    assert before == [(p.read_bytes(), p.stat().st_mtime_ns) for p in outputs]


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "truncated",
        "empty",
        "extra_bytes",
        "wrong_start",
        "wrong_rate",
        "wrong_id",
        "constant",
    ],
)
def test_one_bad_output_recomputes_the_whole_input(case, damage):
    with patch.object(removal, "remove_response", side_effect=process):
        assert run(case).succeeded == 1
    outputs = destinations(case)
    original = [p.read_bytes() for p in outputs]
    damaged = outputs[-1]
    if damage == "missing":
        damaged.unlink()
    elif damage in ("empty", "truncated", "extra_bytes"):
        contents = damaged.read_bytes()
        damaged.write_bytes(
            b""
            if damage == "empty"
            else contents[:-4]
            if damage == "truncated"
            else contents + b"extra"
        )
    else:
        trace = read(str(damaged))[0]
        if damage == "wrong_start":
            trace.stats.starttime += 1
        elif damage == "wrong_rate":
            trace.stats.sampling_rate = 50
        elif damage == "wrong_id":
            trace.stats.station = "OTHER"
        else:
            trace.data[:] = 1
        trace.write(str(damaged), format="SAC")
    # An incomplete MiniSEED input must also redo the still-valid sibling.
    if len(outputs) > 1:
        trace = read(str(outputs[0]))[0]
        trace.data *= 7
        trace.write(str(outputs[0]), format="SAC")
    with patch.object(removal, "remove_response", side_effect=process) as remove:
        result = run(case, overwrite=False)
    remove.assert_called_once()
    assert result.succeeded == 1 and result.existing_skipped == result.failed == 0
    assert [p.read_bytes() for p in outputs] == original


def test_default_overwrite_recomputes_complete_outputs(case):
    assert (
        inspect.signature(removal.deconvolve_waveforms).parameters["overwrite"].default
        is True
    )
    with patch.object(removal, "remove_response", side_effect=process) as remove:
        assert run(case).succeeded == 1
        assert run(case).succeeded == 1
    assert remove.call_count == 2


@pytest.mark.parametrize("pre,post", [((), ()), ((4,), (5,))])
def test_sac_resume_skips_external_process_for_complete_output(tmp_path, pre, post):
    source = tmp_path / "source"
    source.mkdir()
    target = source / "a.sac"
    output = tmp_path / "output"
    output.mkdir()
    trace = Trace(np.arange(1001, dtype=np.float32), header={"sampling_rate": 100})
    trace.write(str(target), format="SAC")
    processed = trace.copy()
    for factors in (pre, post):
        if factors:
            removal._sac_compatible_decimate_trace(processed, factors)
    processed.write(str(output / target.name), format="SAC")
    with patch.object(
        removal.subprocess, "run", side_effect=AssertionError("SAC invoked")
    ):
        result = removal._process_sac_batch(
            (target,),
            object(),
            tmp_path / "responses.pz",
            source,
            output,
            20,
            removal.DEFAULT_PRE_FILTER,
            {},
            pre,
            POLICY,
            overwrite=False,
            post_decimate_factors=post,
        )
    assert result.skipped == result.existing_skipped == 1
    assert result.failed == 0


@pytest.mark.parametrize("pre,post", [(None, None), (4, 5)])
def test_public_resume_counter_survives_worker_pool_and_report(tmp_path, pre, post):
    source = tmp_path / "source"
    source.mkdir()
    target = source / "day.mseed"
    stream = Stream(
        [
            Trace(
                np.random.default_rng(index).normal(size=30001).astype(np.float32),
                header=dict(
                    network="XX",
                    station="TEST",
                    location="",
                    channel="BHZ",
                    sampling_rate=100,
                    starttime=UTCDateTime("2024-01-01") + index * 600,
                ),
            )
            for index in range(2)
        ]
    )
    stream.write(str(target), format="MSEED")
    response = Response.from_paz(
        zeros=[0j, 0j],
        poles=[-0.2 + 0.2j, -0.2 - 0.2j],
        stage_gain=2e6,
        input_units="M",
        output_units="COUNTS",
    )
    channel = Channel(
        code="BHZ",
        location_code="",
        latitude=0,
        longitude=0,
        elevation=0,
        depth=0,
        sample_rate=100,
        response=response,
        start_date=UTCDateTime("2020-01-01"),
    )
    inventory = Inventory(
        [
            Network(
                code="XX",
                stations=[
                    Station(
                        code="TEST",
                        latitude=0,
                        longitude=0,
                        elevation=0,
                        channels=[channel],
                    )
                ],
            )
        ],
        source="test",
    )
    kwargs = dict(
        output_dir=tmp_path / "output",
        backend="obspy",
        pattern="*.mseed",
        max_workers=1,
        trace_filter=POLICY,
        overwrite=False,
        decimate_factors=pre,
        post_decimate_factors=post,
    )
    first = removal.deconvolve_waveforms(source, inventory, **kwargs)
    second = removal.deconvolve_waveforms(source, inventory, **kwargs)
    assert first.succeeded == 1
    expected_rate = 5 if post else 100
    assert all(
        read(str(p), headonly=True)[0].stats.sampling_rate == expected_rate
        for p in kwargs["output_dir"].rglob("*.sac")
    )
    assert second.skipped == second.existing_skipped == 1
    assert second.succeeded == second.failed == 0
    assert json.loads(second.report_path.read_text())["existing_skipped"] == 1
    outputs = removal._obspy_destinations(target, stream, source, kwargs["output_dir"])
    if post:
        reference = removal.remove_response_from_file(
            target, inventory, decimate_factors=pre
        )
        for trace, path in zip(reference, outputs, strict=True):
            assert trace.data.dtype == np.float64
            removal._sac_compatible_decimate_trace(trace, (post,))
            np.testing.assert_array_equal(read(str(path))[0].data, trace.data)
    outputs[-1].unlink()
    third = removal.deconvolve_waveforms(source, inventory, **kwargs)
    assert third.succeeded == 1 and third.existing_skipped == third.failed == 0


def test_overwrite_requires_boolean(tmp_path):
    with pytest.raises(ValueError, match="overwrite"):
        removal.deconvolve_waveforms(
            tmp_path,
            "unused.xml",
            output_dir=tmp_path.parent / "out",
            overwrite="false",
        )


@pytest.mark.parametrize("factors", [0, 1, 8, True, [4, 8], [2.5]])
def test_post_decimation_rejects_invalid_factors_before_inventory_read(
    tmp_path, factors
):
    with pytest.raises(ValueError, match="integers from 2 through 7"):
        removal.deconvolve_waveforms(
            tmp_path,
            "unused.xml",
            output_dir=tmp_path.parent / "out",
            post_decimate_factors=factors,
        )


def test_post_decimation_keeps_response_precision_and_writes_only_final_rate(case):
    from scipy.signal import convolve

    original_filter = removal._sac_compatible_decimate_trace
    original_write = Trace.write
    captured = []
    write_rates = []

    def corrected(stream, inventory, *, decimate_factors, **kwargs):
        assert decimate_factors == (4,)
        assert kwargs["pre_filt"] == removal.DEFAULT_PRE_FILTER
        stream = process(stream, inventory, decimate_factors=decimate_factors)
        for trace in stream:
            trace.data = trace.data.astype(np.float64) + np.linspace(
                0, 1e-7, len(trace.data)
            )
        return stream

    def filter_spy(trace, factors):
        if tuple(factors) == (5,):
            assert trace.data.dtype == np.float64
            assert np.any(trace.data != trace.data.astype(np.float32))
            captured.append(trace.data.copy())
        original_filter(trace, factors)

    def write_spy(trace, *args, **kwargs):
        write_rates.append(trace.stats.sampling_rate)
        return original_write(trace, *args, **kwargs)

    with (
        patch.object(removal, "remove_response", side_effect=corrected),
        patch.object(removal, "_sac_compatible_decimate_trace", side_effect=filter_spy),
        patch.object(Trace, "write", write_spy),
    ):
        result = run(case, factors=(4,), post_factors=(5,))
    assert result.succeeded == 1
    assert write_rates == [5] * len(captured)
    coefficients = import_module("seispy.waveform.decimation")._sac_fir_coefficients(5)
    for path, before in zip(destinations(case), captured, strict=True):
        actual = read(str(path))[0]
        assert actual.stats.sampling_rate == 5
        expected = convolve(before, coefficients, mode="same", method="direct")[::5]
        np.testing.assert_allclose(
            actual.data, expected.astype(np.float32), rtol=1e-7, atol=1e-8
        )


def test_resume_replaces_old_25hz_outputs_then_skips_final_5hz_outputs(case):
    with patch.object(removal, "remove_response", side_effect=process) as remove:
        assert run(case, factors=(4,)).succeeded == 1
        assert (
            run(case, factors=(4,), post_factors=(5,), overwrite=False).succeeded == 1
        )
        result = run(case, factors=(4,), post_factors=(5,), overwrite=False)
    assert result.existing_skipped == 1 and remove.call_count == 2
    assert all(
        read(str(p), headonly=True)[0].stats.sampling_rate == 5
        for p in destinations(case)
    )
