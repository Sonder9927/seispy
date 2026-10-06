"""Per-sample seismic LAB candidates, in positive-downward kilometres.

This is an operational velocity-gradient definition, not a thermal LAB.
Thresholds are configurable analysis choices, not universal physical constants.
"""

from dataclasses import dataclass

import numpy as np
from scipy.ndimage import gaussian_filter1d
from scipy.signal import find_peaks, peak_widths


@dataclass(frozen=True)
class LABSettings:
    min_depth_km: float = 50.0
    max_depth_km: float = 250.0
    moho_buffer_km: float = 15.0
    step_km: float = 1.0
    smoothing_km: float = 5.0
    min_drop_km_s: float = 0.05
    min_width_km: float = 10.0

    def __post_init__(self):
        values = np.array(list(vars(self).values()), dtype=float)
        if not np.isfinite(values).all() or np.any(values <= 0):
            raise ValueError("LAB settings must be finite and positive")
        if self.max_depth_km <= self.min_depth_km:
            raise ValueError("LAB maximum depth must exceed minimum depth")


def identify_lab(depth, velocity, moho, settings: LABSettings) -> dict:
    """Identify all resolved negative-gradient peaks; never force one LAB.

    Width is the half-prominence gradient width. Drop is measured across that
    width in the smoothed profile. The search boundary has a three-sigma guard.
    Multiple qualifying peaks are ambiguous and have no single depth estimate.
    """
    z, v = np.asarray(depth), np.asarray(velocity)
    empty = dict(
        lab_depth_km=np.nan,
        lab_width_km=np.nan,
        lab_drop_km_s=np.nan,
        lab_candidates_km=[],
        lab_status="not_detected",
    )
    if z.size < 5 or z.shape != v.shape or not np.isfinite(v).all():
        return {**empty, "lab_status": "insufficient_coverage"}
    dz = np.diff(z)
    if np.any(dz <= 0) or not np.allclose(dz, dz[0]):
        raise ValueError("LAB evaluation requires a uniform increasing depth grid")
    lo = max(settings.min_depth_km, moho + settings.moho_buffer_km)
    hi = min(settings.max_depth_km, z[-1])
    guard = 3 * settings.smoothing_km
    search = (z >= lo) & (z <= hi)
    if hi - lo <= 2 * guard or search.sum() < 5:
        return {**empty, "lab_status": "insufficient_coverage"}
    smooth = gaussian_filter1d(v, settings.smoothing_km / dz[0], mode="nearest")
    descent = -np.gradient(smooth, z)
    peaks, _ = find_peaks(descent, height=0, prominence=0)
    candidates = []
    boundary = False
    for peak in peaks:
        if not lo <= z[peak] <= hi:
            continue
        width, _, left, right = peak_widths(descent, [peak], rel_height=0.5)
        left_z = np.interp(left[0], np.arange(z.size), z)
        right_z = np.interp(right[0], np.arange(z.size), z)
        drop = float(np.interp(left_z, z, smooth) - np.interp(right_z, z, smooth))
        if width[0] * dz[0] < settings.min_width_km or drop < settings.min_drop_km_s:
            continue
        if left_z < lo or right_z > hi or not lo + guard < z[peak] < hi - guard:
            boundary = True
            continue
        candidates.append((float(z[peak]), float(width[0] * dz[0]), drop))
    # A monotonically strengthening descent at a search edge is also censored.
    indices = np.flatnonzero(search)
    strongest = indices[np.argmax(descent[search])]
    if descent[strongest] > 0 and strongest in (indices[0], indices[-1]):
        boundary = True
    empty["lab_candidates_km"] = [c[0] for c in candidates]
    if len(candidates) > 1:
        return {**empty, "lab_status": "ambiguous"}
    if boundary:
        return {**empty, "lab_status": "boundary_limited"}
    if not candidates:
        return empty
    depth, width, drop = candidates[0]
    return dict(
        lab_depth_km=depth,
        lab_width_km=width,
        lab_drop_km_s=drop,
        lab_candidates_km=[depth],
        lab_status="detected",
    )
