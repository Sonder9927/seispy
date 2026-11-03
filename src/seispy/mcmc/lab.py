"""Resolution-aware, multi-scale seismic LAB detection at positive depths.

The criteria are operational signal rules, not a thermal LAB definition or a
calibrated probability of a physical interface.
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
    smoothing_km: float = 3.0
    min_drop_km_s: float = 0.01
    min_relative_drop: float = 0.005
    noise_multiplier: float = 4.0
    min_width_km: float = 5.0
    boundary_buffer_km: float = 5.0
    min_detection_fraction: float = 0.5

    def __post_init__(self):
        values = np.array(list(vars(self).values()), dtype=float)
        if not np.isfinite(values).all() or np.any(values <= 0):
            raise ValueError("LAB settings must be finite and positive")
        if self.max_depth_km <= self.min_depth_km:
            raise ValueError("LAB maximum depth must exceed minimum depth")
        if self.min_detection_fraction > 1:
            raise ValueError("LAB minimum detection fraction must be in (0, 1]")


def _bands(z, velocity, sigma):
    """Describe distinct positive-descent bands at one smoothing scale."""
    dz = z[1] - z[0]
    smooth = gaussian_filter1d(velocity, sigma / dz, mode="nearest")
    descent = -np.gradient(smooth, z)
    regional = max(0.0, float(np.median(descent)))
    peaks, props = find_peaks(descent, height=1e-8, prominence=1e-8)
    effective = np.minimum(props["prominences"], descent[peaks] - regional)
    keep = (props["prominences"] >= 0.2 * descent[peaks]) & (effective > 1e-8)
    peaks = peaks[keep]
    prominences = effective[keep]
    if not peaks.size:
        return []
    # Measure width above the local gradient background. A regional downward
    # trend should not make every localized band extend to the profile ends.
    widths = (
        peak_widths(
            descent,
            peaks,
            rel_height=0.5,
            prominence_data=(
                prominences,
                props["left_bases"][keep],
                props["right_bases"][keep],
            ),
        )[0]
        * dz
    )
    fwhm = dict(zip(peaks, widths, strict=True))
    backgrounds = dict(zip(peaks, descent[peaks] - prominences, strict=True))
    bands = []
    for peak in sorted(peaks, key=lambda k: descent[k], reverse=True):
        if any(b["left"] <= peak <= b["right"] for b in bands):
            continue
        left = right = int(peak)
        background = max(0.0, float(backgrounds[peak]))
        threshold = background + 0.1 * (descent[peak] - background)
        while left > 0 and descent[left - 1] > threshold:
            left -= 1
        while right < z.size - 1 and descent[right + 1] > threshold:
            right += 1
        bands.append(
            dict(
                left=left,
                right=right,
                lab_depth_km=float(z[peak]),
                lab_width_km=float(z[right] - z[left]),
                lab_fwhm_km=float(fwhm[peak]),
                lab_drop_km_s=float(
                    smooth[left] - smooth[right] - background * (z[right] - z[left])
                ),
                lab_total_drop_km_s=float(smooth[left] - smooth[right]),
                lab_top_km=float(z[left]),
                lab_bottom_km=float(z[right]),
                baseline=float(0.5 * (smooth[left] + smooth[right])),
            )
        )
    return bands


def identify_lab(
    depth, velocity, moho, settings: LABSettings, *, resolution_km=None
) -> dict:
    """Identify a stable descent center; retain rejected candidate diagnostics.

    ``resolution_km`` is the spacing of the original forward mantle nodes, not
    the interpolated evaluation grid. When unknown, only the evaluation spacing
    is available. Roughness estimated from second differences is a signal proxy,
    not an estimate of observational or posterior uncertainty.
    """
    z, v = np.asarray(depth, dtype=float), np.asarray(velocity, dtype=float)
    empty = dict(
        lab_depth_km=np.nan,
        lab_width_km=np.nan,
        lab_fwhm_km=np.nan,
        lab_drop_km_s=np.nan,
        lab_total_drop_km_s=np.nan,
        lab_candidates_km=[],
        lab_top_km=np.nan,
        lab_bottom_km=np.nan,
        lab_width_limited=False,
        lab_width_resolved=False,
        lab_candidate_depth_km=np.nan,
        lab_candidate_drop_km_s=np.nan,
        lab_drop_threshold_km_s=np.nan,
        lab_smoothing_km=np.nan,
        lab_noise_km_s=np.nan,
        lab_center_shift_km=np.nan,
        lab_min_width_km=np.nan,
        lab_status="not_detected",
        lab_rejection_reason="insufficient_coverage",
    )
    if (
        z.ndim != 1
        or z.size < 5
        or z.shape != v.shape
        or not np.isfinite(z).all()
        or not np.isfinite(v).all()
        or not np.isfinite(moho)
    ):
        return {**empty, "lab_status": "insufficient_coverage"}
    dzs = np.diff(z)
    if np.any(dzs <= 0) or not np.allclose(dzs, dzs[0]):
        raise ValueError("LAB evaluation requires a uniform increasing depth grid")
    dz = float(dzs[0])
    resolution = dz if resolution_km is None else float(resolution_km)
    if not np.isfinite(resolution) or resolution <= 0:
        raise ValueError("Native mantle spacing must be finite and positive")
    sigma = max(settings.smoothing_km, resolution / 2, dz)
    minimum_width = max(settings.min_width_km, resolution, 2 * dz)
    margin = max(settings.boundary_buffer_km, sigma, resolution)
    lo = max(settings.min_depth_km, moho + settings.moho_buffer_km)
    hi = min(settings.max_depth_km, z[-1])
    if hi - lo <= 2 * margin or ((z >= lo) & (z <= hi)).sum() < 5:
        return {**empty, "lab_status": "insufficient_coverage"}
    native_z = np.arange(z[0], z[-1] + 0.1 * dz, max(resolution, dz))
    curvature = np.diff(np.interp(native_z, z, v), n=2)
    noise = (
        float(1.4826 * np.median(np.abs(curvature - np.median(curvature))) / np.sqrt(6))
        if curvature.size
        else 0.0
    )
    diagnostic = {
        **empty,
        "lab_smoothing_km": sigma,
        "lab_noise_km_s": noise,
        "lab_min_width_km": minimum_width,
    }
    bands = [b for b in _bands(z, v, sigma) if lo <= b["lab_depth_km"] <= hi]
    if not bands:
        return {**diagnostic, "lab_rejection_reason": "no_descent_peak"}
    broad = _bands(z, v, 2 * sigma)
    candidates, rejected = [], []
    for b in bands:
        center = b["lab_depth_km"]
        threshold = max(
            settings.min_drop_km_s,
            settings.min_relative_drop * abs(b["baseline"]),
            settings.noise_multiplier * noise,
        )
        details = dict(
            lab_candidate_depth_km=center,
            lab_candidate_drop_km_s=b["lab_drop_km_s"],
            lab_drop_threshold_km_s=threshold,
        )
        if b["lab_drop_km_s"] < threshold:
            reason = "weak_drop"
        elif b["lab_fwhm_km"] < minimum_width:
            reason = "too_narrow"
        elif b["lab_fwhm_km"] > 0.8 * (z[-1] - z[0]):
            reason = "unlocalized_descent"
        elif (
            b["left"] == 0
            and b["right"] == len(z) - 1
            or not lo + margin < center < hi - margin
            or not z[0] + 3 * sigma < center < z[-1] - 3 * sigma
        ):
            reason = "boundary_center"
        else:
            # A peak must survive doubled smoothing in the same descent band.
            matches = [
                c
                for c in broad
                if b["lab_top_km"] <= c["lab_depth_km"] <= b["lab_bottom_km"]
            ]
            match = (
                min(matches, key=lambda c: abs(c["lab_depth_km"] - center))
                if matches
                else None
            )
            shift = abs(match["lab_depth_km"] - center) if match else np.inf
            details["lab_center_shift_km"] = shift
            if (
                match is None
                or shift > max(sigma, resolution)
                or match["lab_drop_km_s"] < threshold
            ):
                reason = "scale_unstable"
            else:
                limited = bool(
                    b["lab_top_km"] < lo
                    or b["lab_bottom_km"] > hi
                    or b["left"] == 0
                    or b["right"] == len(z) - 1
                )
                fields = {k: value for k, value in b.items() if k.startswith("lab_")}
                candidates.append(
                    {
                        **fields,
                        **details,
                        "lab_width_limited": limited,
                        "lab_width_resolved": bool(
                            b["lab_fwhm_km"] >= max(2 * resolution, 3 * sigma)
                        ),
                    }
                )
                continue
        rejected.append((b["lab_drop_km_s"], reason, details))
    if not candidates:
        _, reason, details = max(rejected, key=lambda item: item[0])
        return {
            **diagnostic,
            **details,
            "lab_rejection_reason": reason,
            "lab_status": "boundary_limited"
            if reason == "boundary_center"
            else "not_detected",
        }
    candidates.sort(key=lambda c: c["lab_drop_km_s"], reverse=True)
    depths = sorted(c["lab_depth_km"] for c in candidates)
    if (
        len(candidates) > 1
        and candidates[1]["lab_drop_km_s"] >= 0.8 * candidates[0]["lab_drop_km_s"]
    ):
        return {
            **diagnostic,
            "lab_candidates_km": depths,
            "lab_status": "ambiguous",
            "lab_rejection_reason": "competing_bands",
        }
    return {
        **diagnostic,
        **candidates[0],
        "lab_candidates_km": depths,
        "lab_status": "detected",
        "lab_rejection_reason": "none",
    }


def summarize_lab(depths, multiplicities, *, min_detection_fraction=0.5) -> dict:
    """Select one user-facing estimate, leaving conditional statistics separate.

    Require the configured retained mass fraction to have a resolved center. Reject
    clearly separated comparable modes using 5-km bins smoothed by 7.5 km:
    peaks must be >=20 km apart, have at least half the dominant peak's height,
    and an intervening valley below half the smaller peak. This conservative
    signal check is not a formal multimodality or convergence test.
    """
    values, weights = np.asarray(depths), np.asarray(multiplicities)
    if not np.isfinite(min_detection_fraction) or not 0 < min_detection_fraction <= 1:
        raise ValueError("LAB minimum detection fraction must be in (0, 1]")
    valid = np.isfinite(values)
    total = weights.sum()
    if total <= 0 or weights[valid].sum() < min_detection_fraction * total:
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
