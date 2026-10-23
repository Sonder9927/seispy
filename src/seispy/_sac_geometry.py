"""SAC geometry: WGS84 distance and geocentric great-circle angles."""

import math

from geographiclib.geodesic import Geodesic


def sac_geometry(evla, evlo, stla, stlo):
    """Return geometry headers; incomplete/invalid coordinates clear geometry.

    Distance is the WGS84 ellipsoidal geodesic in km. Angles use geocentric
    latitudes on a sphere, matching SAC's convention rather than converting
    ellipsoidal distance to degrees. Coincident/antipodal azimuths are undefined
    and represented by SAC's undefined value. Heights/depths are not used.
    """
    empty = dict.fromkeys(("dist", "az", "baz", "gcarc"), -12345.0)
    empty["lcalda"] = False
    try:
        coords = tuple(float(x) for x in (evla, evlo, stla, stlo))
    except (TypeError, ValueError):
        return empty
    if not all(math.isfinite(x) and x != -12345 for x in coords):
        return empty
    evla, evlo, stla, stlo = coords
    if abs(evla) > 90 or abs(stla) > 90 or abs(evlo) > 180 or abs(stlo) > 180:
        return empty

    f = Geodesic.WGS84.f

    def geocentric(latitude):
        phi = math.radians(latitude)
        return math.atan2((1 - f) ** 2 * math.sin(phi), math.cos(phi))

    a, b = geocentric(evla), geocentric(stla)
    dl = math.radians(stlo - evlo)
    y = math.sin(dl) * math.cos(b)
    x = math.cos(a) * math.sin(b) - math.sin(a) * math.cos(b) * math.cos(dl)
    dot = math.sin(a) * math.sin(b) + math.cos(a) * math.cos(b) * math.cos(dl)
    cross = math.hypot(x, y)
    arc = math.degrees(math.atan2(cross, dot))
    az = math.degrees(math.atan2(y, x)) % 360
    baz = (
        math.degrees(
            math.atan2(
                -math.sin(dl) * math.cos(a),
                math.cos(b) * math.sin(a) - math.sin(b) * math.cos(a) * math.cos(dl),
            )
        )
        % 360
    )
    if cross < 1e-14:
        az = baz = -12345.0
    distance = Geodesic.WGS84.Inverse(evla, evlo, stla, stlo)["s12"] / 1000
    return dict(dist=distance, az=az, baz=baz, gcarc=arc, lcalda=True)
