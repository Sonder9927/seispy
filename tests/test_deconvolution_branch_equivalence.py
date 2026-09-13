"""Numerical regression checks across response-removal processing branches."""

import shutil
import tempfile
from pathlib import Path

import numpy as np
import pytest
from obspy import Trace, UTCDateTime, read
from obspy.core.inventory import Channel, Inventory, Network, Site, Station
from obspy.core.inventory.response import Response

from seispy.waveform.decimation import (
    _sac_compatible_decimate_trace,
    _sac_decimate_batch,
    _sac_filter_file,
    _scipy_decimate_batch,
)
from seispy.deconvolution.removal import (
    obspy_deconv,
    sac_deconv,
    remove_response_from_file,
)

PRE_FILTER = (0.004, 0.006, 4.0, 5.0)


def _inventory():
    response = Response.from_paz(
        zeros=[0j, 0j],
        poles=[-0.2 + 0.2j, -0.2 - 0.2j],
        stage_gain=2e6,
        stage_gain_frequency=1.0,
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
        azimuth=0,
        dip=-90,
        sample_rate=100,
        response=response,
        start_date=UTCDateTime("2020-01-01"),
    )
    station = Station(
        code="TEST",
        latitude=0,
        longitude=0,
        elevation=0,
        site=Site(name="test"),
        channels=[channel],
    )
    return Inventory([Network(code="XX", stations=[station])], source="test")


def _trace():
    time = np.arange(120_000) / 100
    rng = np.random.default_rng(42)
    data = (
        1e5 * np.sin(2 * np.pi * 0.1 * time)
        + 2e4 * np.sin(2 * np.pi * 1.0 * time)
        + 1e4 * np.sin(2 * np.pi * 4.0 * time)
        + 5e3 * np.sin(2 * np.pi * 20.0 * time)
        + rng.normal(0, 100, len(time))
    ).astype(np.float32)
    return Trace(
        data=data,
        header={
            "network": "XX",
            "station": "TEST",
            "location": "",
            "channel": "BHZ",
            "sampling_rate": 100.0,
            "starttime": UTCDateTime("2024-01-01"),
        },
    )


def _metrics(first, second, edge_samples=1000):
    first_data = np.asarray(first.data[edge_samples:-edge_samples], dtype=float)
    second_data = np.asarray(second.data[edge_samples:-edge_samples], dtype=float)
    difference = first_data - second_data
    return (
        float(np.corrcoef(first_data, second_data)[0, 1]),
        float(np.sqrt(np.mean(difference**2)) / np.sqrt(np.mean(second_data**2))),
    )


def _require_sac_decimation():
    if shutil.which("sac") is None:
        pytest.skip("SAC executable is not installed")
    try:
        _sac_filter_file(4)
    except FileNotFoundError:
        pytest.skip("SAC decimation FIR coefficients are not installed")


@pytest.fixture
def comparison_path():
    """Use the short POSIX temporary path supported by SAC and CI runners."""
    with tempfile.TemporaryDirectory(prefix="seispy-equivalence-", dir="/tmp") as path:
        yield Path(path)


@pytest.mark.integration
def test_response_removal_branches_remain_numerically_equivalent(comparison_path):
    """Guard processing order and backend equivalence on a fixed signal."""
    _require_sac_decimation()
    inventory = _inventory()
    tmp_path = comparison_path
    source_root = tmp_path / "source"
    station = source_root / "TEST"
    station.mkdir(parents=True)
    source = station / "trace.sac"
    _trace().write(str(source), format="SAC")

    # ObsPy: integrated pre-decimation versus independent post-decimation.
    obspy_pre_memory = remove_response_from_file(
        source, inventory, pre_filt=PRE_FILTER, decimate_factors=4
    )[0]
    obspy_post_memory = remove_response_from_file(
        source, inventory, pre_filt=PRE_FILTER
    )[0]
    _sac_compatible_decimate_trace(obspy_post_memory, (4,))
    correlation, nrmse = _metrics(obspy_pre_memory, obspy_post_memory)
    assert correlation > 0.999999
    assert nrmse < 1e-5

    outputs = {}
    for name, backend, factors in (
        ("sac-pre", sac_deconv, (4,)),
        ("obspy-pre", obspy_deconv, (4,)),
        ("sac-100hz", sac_deconv, ()),
        ("obspy-100hz", obspy_deconv, ()),
    ):
        output = tmp_path / name
        output.mkdir()
        summary = backend(
            station,
            "*.sac",
            inventory,
            source_root,
            output,
            False,
            20,
            PRE_FILTER,
            decimate_factors=factors,
        )
        assert summary.succeeded == 1
        assert summary.failed == 0
        outputs[name] = output

    sac_post = tmp_path / "sac-post"
    obspy_post = tmp_path / "obspy-post"
    sac_post.mkdir()
    obspy_post.mkdir()
    assert (
        _sac_decimate_batch(
            (outputs["sac-100hz"] / "TEST" / "trace.sac",),
            (4,),
            outputs["sac-100hz"],
            sac_post,
            False,
            20,
        ).succeeded
        == 1
    )
    assert (
        _scipy_decimate_batch(
            (outputs["obspy-100hz"] / "TEST" / "trace.sac",),
            (4,),
            outputs["obspy-100hz"],
            obspy_post,
            False,
            20,
        ).succeeded
        == 1
    )

    traces = {
        "sac-pre": read(outputs["sac-pre"] / "TEST" / "trace.sac")[0],
        "obspy-pre": read(outputs["obspy-pre"] / "TEST" / "trace.sac")[0],
        "sac-post": read(sac_post / "TEST" / "trace.sac")[0],
        "obspy-post": read(obspy_post / "TEST" / "trace.sac")[0],
    }
    for trace in traces.values():
        assert trace.stats.sampling_rate == 25.0
        assert trace.stats.npts == 30_000
        assert np.isfinite(trace.data).all()

    for backend in ("sac", "obspy"):
        correlation, nrmse = _metrics(
            traces[f"{backend}-pre"], traces[f"{backend}-post"]
        )
        assert correlation > 0.999999
        assert nrmse < 1e-5

    for order in ("pre", "post"):
        correlation, nrmse = _metrics(traces[f"sac-{order}"], traces[f"obspy-{order}"])
        assert correlation > 0.999
        assert nrmse < 0.02
