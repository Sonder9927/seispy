"""Geometry checks against fixed SAC Community 103.0 results, without SAC."""

import math

import pytest

from seispy._sac_geometry import sac_geometry


@pytest.mark.parametrize(
    "coordinates,expected",
    [
        ((30, 110, 35, 115), (726.560791, 38.967049, 221.644516, 6.5330634)),
        ((30, 110, 40, -120), (10699.728516, 36.301136, 318.043457, 96.304665)),
        ((10, 179, 10, -179), (219.279175, 89.827461, 270.172546, 1.97000885)),
        ((89, 0, 89, 90), (157.955551, 45.004421, 314.995575, 1.42370641)),
        ((0, 0, 0, 90), (10018.790039, 90, 270, 90)),
        ((30, 110, -30.001, -69.999), (20003.820517, 221.036, 138.964, None)),
    ],
)
def test_angles_match_sac_and_distance_stays_close(coordinates, expected):
    result = sac_geometry(*coordinates)
    near_antipodal = expected[3] is None
    # SAC's legacy distance differs slightly from the WGS84 result.
    assert result["dist"] == pytest.approx(
        expected[0],
        rel=0 if near_antipodal else 1e-5,
        abs=0.001 if near_antipodal else 0,
    )
    for key, value in zip(("az", "baz"), expected[1:3], strict=True):
        assert result[key] == pytest.approx(value, abs=0.1 if near_antipodal else 3e-5)
    if near_antipodal:
        assert result["gcarc"] > 179.99
    else:
        assert result["gcarc"] == pytest.approx(expected[3], abs=3e-5)
    assert result["lcalda"] is True


@pytest.mark.parametrize(
    "coordinates,arc",
    [
        ((30, 110, 30, 110), 0),
        ((30, 110, -30, -70), 180),
        ((90, 0, 90, 150), 0),
    ],
)
def test_undefined_directions_are_not_invented(coordinates, arc):
    result = sac_geometry(*coordinates)
    assert result["az"] == result["baz"] == -12345
    assert result["gcarc"] == pytest.approx(arc, abs=1e-10)
    assert math.isfinite(result["dist"])


@pytest.mark.parametrize("value", [None, -12345, float("nan"), float("inf"), 91])
def test_invalid_coordinates_clear_stale_geometry(value):
    result = sac_geometry(value, 110, 30, 115)
    assert result == dict(
        dist=-12345, az=-12345, baz=-12345, gcarc=-12345, lcalda=False
    )
