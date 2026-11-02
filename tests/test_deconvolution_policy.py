from importlib import import_module

import pytest

remove_response = import_module("seispy.deconvolution.removal")


@pytest.mark.parametrize(
    "requested,rate,expected,error",
    [
        ((0.004, 0.006, 30.0, 35.0), 100.0, (0.004, 0.006, 30.0, 35.0), None),
        ((0.004, 0.006, 30.0, 35.0), 50.0, (0.004, 0.006, 20.0, 23.75), None),
        ((0.4, 0.94, 1.2, 1.5), 2.0, None, None),
        ((0.004, 0.006, 30.0), 100.0, None, "pre_filt"),
        ((0.004, 0.006, 35.0, 30.0), 100.0, None, "pre_filt"),
        ((0.0, 0.006, 30.0, 35.0), 100.0, None, "pre_filt"),
        ((0.4, 0.6, 3.0, 3.5), 1.0, None, "Nyquist"),
        ((0.4, 0.96, 1.2, 1.5), 2.0, None, "rolloff"),
    ],
)
def test_pre_filter_bounds(requested, rate, expected, error):
    if error:
        with pytest.raises(ValueError, match=error):
            remove_response._validate_pre_filt(requested)
            remove_response._effective_pre_filt(requested, rate)
    else:
        result = remove_response._effective_pre_filt(requested, rate)
        assert all(
            left < right for left, right in zip(result, result[1:], strict=False)
        )
        assert result[-1] < rate / 2
        if expected is not None:
            assert result == pytest.approx(expected)


@pytest.mark.parametrize(
    "options,error",
    [
        *[
            (dict(decimate_factors=factors), "integers from 2 through 7")
            for factors in (0, 1, 8, [5, 1], [2, 8], [2.0])
        ],
        (dict(backend="unknown"), "Unknown backend"),
    ],
)
def test_decimation_policy(tmp_path, options, error):
    source = tmp_path / "source"
    source.mkdir()
    with pytest.raises(ValueError, match=error):
        remove_response.deconvolve_waveforms(
            source, tmp_path / "stations.xml", output_dir=tmp_path / "output", **options
        )
    with pytest.raises(ValueError, match="decimation leaves Nyquist"):
        remove_response._final_sampling_rate(
            1.0, (5, 5, 4), remove_response.DEFAULT_PRE_FILTER
        )
