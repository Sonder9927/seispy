"""A single LAB estimate from the mean shear-velocity profile."""

from dataclasses import dataclass

import numpy as np
from scipy.ndimage import gaussian_filter1d
from scipy.signal import find_peaks


@dataclass(frozen=True)
class LABSettings:
    """Mean-profile LAB controls; depths are positive downward in kilometres."""

    min_depth_km: float = 50.0
    max_depth_km: float = 200.0
    moho_buffer_km: float = 15.0
    smoothing_km: float = 5.0
    min_drop_km_s: float = 0.015

    def __post_init__(self):
        if not all(np.isfinite(v) and v > 0 for v in vars(self).values()):
            raise ValueError("LAB settings must be finite and positive")
        if self.max_depth_km <= self.min_depth_km:
            raise ValueError("LAB maximum depth must exceed minimum depth")


def estimate_lab(depth, velocity, moho: float, settings: LABSettings) -> float:
    """Locate the dominant localized mantle descent, or return NaN.

    No posterior states or inversion inputs are required. The depth is the
    maximum negative-gradient center, not a velocity minimum or thermal LAB.
    """
    z, v = np.asarray(depth, dtype=float), np.asarray(velocity, dtype=float)
    if (
        z.ndim != 1
        or z.shape != v.shape
        or z.size < 5
        or not np.isfinite(z).all()
        or not np.isfinite(v).all()
        or not np.isfinite(moho)
        or np.any(np.diff(z) <= 0)
    ):
        return float("nan")
    mantle = z >= moho
    z, v = z[mantle], v[mantle]
    if z.size < 5:
        return float("nan")
    step = max(1.0, float(np.median(np.diff(z))))
    grid = np.arange(z[0], z[-1] + 1e-6, step)
    if grid.size < 5:
        return float("nan")
    smooth = gaussian_filter1d(
        np.interp(grid, z, v), settings.smoothing_km / step, mode="nearest"
    )
    descent = -np.gradient(smooth, grid)
    regional = max(0.0, float(np.median(descent)))
    peaks, props = find_peaks(descent, height=1e-8, prominence=1e-8)
    lo = max(settings.min_depth_km, moho + settings.moho_buffer_km)
    hi = min(settings.max_depth_km, grid[-1])
    candidates, bands = [], []
    for peak, prominence in sorted(
        zip(peaks, props["prominences"], strict=True),
        key=lambda pair: descent[pair[0]],
        reverse=True,
    ):
        background = max(regional, float(descent[peak] - prominence))
        excess = descent[peak] - background
        if excess < max(1e-8, 0.2 * descent[peak]):
            continue
        if any(left <= peak <= right for left, right in bands):
            continue
        left = right = int(peak)
        shoulder = background + 0.1 * excess
        while left > 0 and descent[left - 1] > shoulder:
            left -= 1
        while right < len(grid) - 1 and descent[right + 1] > shoulder:
            right += 1
        bands.append((left, right))
        center = float(grid[peak])
        drop = float(
            smooth[left] - smooth[right] - background * (grid[right] - grid[left])
        )
        if (
            not lo <= center <= hi
            or not grid[0] + 3 * settings.smoothing_km
            < center
            < grid[-1] - 3 * settings.smoothing_km
            or left == 0
            and right == len(grid) - 1
            or grid[right] - grid[left] < 2 * step
            or drop < settings.min_drop_km_s
        ):
            continue
        candidates.append((drop, center))
    candidates.sort(reverse=True)
    if (
        not candidates
        or len(candidates) > 1
        and candidates[1][0] >= 0.8 * candidates[0][0]
    ):
        return float("nan")
    return candidates[0][1]
