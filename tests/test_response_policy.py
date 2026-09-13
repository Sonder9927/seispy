from importlib import import_module
import inspect
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import numpy as np
import pytest
from obspy import Trace, UTCDateTime

remove_response = import_module("seispy.response.removal")


def test_pre_filter_is_unchanged_when_below_nyquist():
    requested = (0.004, 0.006, 30.0, 35.0)

    assert remove_response._effective_pre_filt(requested, 100.0) == requested


def test_default_pre_filter_and_daily_taper_are_conservative():
    assert remove_response.DEFAULT_PRE_FILTER == (0.004, 0.006, 4.0, 5.0)
    trace = SimpleNamespace(stats=SimpleNamespace(sampling_rate=100.0, npts=8_640_000))
    assert remove_response._sac_taper_width(trace) == pytest.approx(150 / 86_400)


def test_pre_filter_upper_corners_are_reduced_below_nyquist():
    result = remove_response._effective_pre_filt(
        (0.004, 0.006, 30.0, 35.0), sampling_rate=50.0
    )

    assert result == pytest.approx((0.004, 0.006, 20.0, 23.75))


@pytest.mark.parametrize(
    "pre_filt",
    [
        (0.004, 0.006, 30.0),
        (0.004, 0.006, 35.0, 30.0),
        (0.0, 0.006, 30.0, 35.0),
    ],
)
def test_invalid_pre_filter_is_rejected(pre_filt):
    with pytest.raises(ValueError, match="pre_filt"):
        remove_response._validate_pre_filt(pre_filt)


def test_pre_filter_low_corner_must_be_below_nyquist():
    with pytest.raises(ValueError, match="Nyquist"):
        remove_response._effective_pre_filt((0.4, 0.6, 3.0, 3.5), sampling_rate=1.0)


def test_pre_filter_requires_high_frequency_rolloff_room_below_nyquist():
    with pytest.raises(ValueError, match="rolloff"):
        remove_response._effective_pre_filt((0.4, 0.96, 1.2, 1.5), sampling_rate=2.0)


def test_adjusted_pre_filter_remains_strictly_increasing():
    result = remove_response._effective_pre_filt(
        (0.4, 0.94, 1.2, 1.5), sampling_rate=2.0
    )

    assert all(left < right for left, right in zip(result, result[1:], strict=False))
    assert result[-1] < 1.0


@pytest.mark.parametrize("factors", [0, 1, 8, [5, 1], [2, 8], [2.0]])
def test_deconvolution_rejects_non_sac_decimation_factors(tmp_path, factors):
    source = tmp_path / "source"
    source.mkdir()

    with pytest.raises(ValueError, match="integers from 2 through 7"):
        remove_response.remove_instrument_response(
            source,
            tmp_path / "stations.xml",
            output_dir=tmp_path / "output",
            decimate_factors=factors,
        )


def test_decimation_must_leave_passband_below_nyquist():
    with pytest.raises(ValueError, match="decimation leaves Nyquist"):
        remove_response._final_sampling_rate(
            1.0, (5, 5, 4), remove_response.DEFAULT_PRE_FILTER
        )


def test_deconvolution_decimation_is_optional():
    signature = inspect.signature(remove_response.remove_instrument_response)

    assert signature.parameters["decimate_factors"].default is None


def test_deconvolution_public_interface_uses_backend_term():
    signature = inspect.signature(remove_response.remove_instrument_response)

    assert signature.parameters["backend"].default == "obspy"
    assert "method" not in signature.parameters


def test_unknown_deconvolution_backend_is_rejected():
    with pytest.raises(ValueError, match="Unknown backend"):
        remove_response._deconvolution_backend("unknown")
