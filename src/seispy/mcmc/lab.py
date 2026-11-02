"""Per-sample seismic LAB candidates, in positive-downward kilometres.

This is an operational velocity-gradient definition, not a thermal LAB.
Thresholds are configurable analysis choices, not universal physical constants.
"""

from dataclasses import dataclass

import numpy as np
from scipy.ndimage import gaussian_filter1d
from scipy.signal import find_peaks


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
    """Return the center of a resolved velocity-decrease band.

    Band shoulders are where descent falls below 10% of its peak. Small peaks
    sharing that band are merged. A censored shoulder does not invalidate an
    interior center. Comparable distinct bands (>=80% of the largest drop)
    remain ambiguous. Boundary candidates never veto a valid interior band.
    These are operational signal rules, not a thermal definition of LAB.
    """
    z, v = np.asarray(depth, dtype=float), np.asarray(velocity, dtype=float)
    empty = dict(
        lab_depth_km=np.nan,
        lab_width_km=np.nan,
        lab_drop_km_s=np.nan,
        lab_candidates_km=[],
        lab_top_km=np.nan,
        lab_bottom_km=np.nan,
        lab_width_limited=False,
        lab_status="not_detected",
    )
    if (
        z.ndim != 1
        or z.size < 5
        or z.shape != v.shape
        or not np.isfinite(z).all()
        or not np.isfinite(v).all()
    ):
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
    peaks, props = find_peaks(descent, height=1e-8, prominence=1e-8)
    # Reject interpolation ripples with negligible prominence relative to slope.
    peaks = peaks[props["prominences"] >= 0.2 * descent[peaks]]
    bands = []
    for peak in sorted(peaks, key=lambda i: descent[i], reverse=True):
        if any(left <= peak <= right for _, left, right in bands):
            continue
        threshold = 0.1 * descent[peak]
        left, right = int(peak), int(peak)
        while left > 0 and descent[left - 1] > threshold:
            left -= 1
        while right < z.size - 1 and descent[right + 1] > threshold:
            right += 1
        bands.append((peak, left, right))
    candidates = []
    boundary = False
    for peak, left, right in bands:
        width = float(z[right] - z[left])
        drop = float(smooth[left] - smooth[right])
        if width < settings.min_width_km or drop < settings.min_drop_km_s:
            continue
        if not lo <= z[peak] <= hi:
            continue
        # Both shoulders censored: a uniform downward ramp has no resolved center.
        if (
            left == 0
            and right == z.size - 1
            or not lo + settings.smoothing_km < z[peak] < hi - settings.smoothing_km
            or not z[0] + guard < z[peak] < z[-1] - guard
        ):
            boundary = True
            continue
        limited = bool(
            z[left] < lo or z[right] > hi or left == 0 or right == z.size - 1
        )
        candidates.append(
            dict(
                lab_depth_km=float(z[peak]),
                lab_width_km=width,
                lab_drop_km_s=drop,
                lab_top_km=float(z[left]),
                lab_bottom_km=float(z[right]),
                lab_width_limited=limited,
            )
        )
    if not candidates:
        indices = np.flatnonzero(search)
        strongest = indices[np.argmax(descent[search])]
        if descent[strongest] > 1e-8 and strongest in (indices[0], indices[-1]):
            boundary = True
        return {
            **empty,
            "lab_status": "boundary_limited" if boundary else "not_detected",
        }
    candidates.sort(key=lambda c: c["lab_drop_km_s"], reverse=True)
    depths = sorted(c["lab_depth_km"] for c in candidates)
    if (
        len(candidates) > 1
        and candidates[1]["lab_drop_km_s"] >= 0.8 * candidates[0]["lab_drop_km_s"]
    ):
        return {**empty, "lab_candidates_km": depths, "lab_status": "ambiguous"}
    return {**candidates[0], "lab_candidates_km": depths, "lab_status": "detected"}


def summarize_lab(depths, multiplicities) -> dict:
    """Select one user-facing estimate, leaving conditional statistics separate.

    Require at least half the retained mass to have a resolved center. Reject
    clearly separated comparable modes using 5-km bins smoothed by 7.5 km:
    peaks must be >=20 km apart, have at least half the dominant peak's height,
    and an intervening valley below half the smaller peak. This conservative
    signal check is not a formal multimodality or convergence test.
    """
    values, weights = np.asarray(depths), np.asarray(multiplicities)
    valid = np.isfinite(values)
    total = weights.sum()
    if total <= 0 or weights[valid].sum() < 0.5 * total:
        return dict(lab_estimate_km=np.nan, lab_estimate_status="low_support")
    expanded = np.repeat(values[valid], weights[valid])
    edges = np.arange(
        np.floor(expanded.min() / 5) * 5 - 20, np.ceil(expanded.max() / 5) * 5 + 25, 5.0
    )
    density = gaussian_filter1d(np.histogram(expanded, edges)[0].astype(float), 1.5)
    peaks, _ = find_peaks(density, height=0.5 * density.max())
    for i, first in enumerate(peaks):
        for second in peaks[i + 1 :]:
            if 5 * (second - first) >= 20 and density[
                first : second + 1
            ].min() < 0.5 * min(density[first], density[second]):
                return dict(lab_estimate_km=np.nan, lab_estimate_status="multimodal")
    return dict(
        lab_estimate_km=float(np.median(expanded)), lab_estimate_status="resolved"
    )
