"""Regular-grid geometry and resampling for MCMC spatial inputs.

Every spatial product consumed by the MCMC module (topography, sediment
thickness, Moho depth, phase dispersion and reference Vs) is a regular
longitude/latitude grid. This module defines the authoritative inversion grid
(:class:`TargetGrid`) and aligns any regular source grid to it with one of
three strategies chosen from the source/target spacing relationship:

* equal spacing and aligned origin -> direct selection, no resampling;
* source coarser than the target    -> linear interpolation;
* source finer than the target      -> conservative area average.

Only numpy, scipy and xarray are used. No external gridding tools (GMT/PyGMT)
are required.

Alignment is separable per horizontal axis, so the same routine handles cubes
that carry extra leading dimensions such as ``period`` (phase dispersion) or
``z`` (reference Vs).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Literal

import numpy as np
import xarray as xr
from scipy import sparse
from scipy.interpolate import interp1d

MissingPolicy = Literal["raise", "nan"]

_COORD_ATOL = 1e-9
_SPACING_RTOL = 1e-6
_ALIGN_ATOL = 1e-6


# =========================
# TARGET GRID
# =========================


def target_shape(
    region: Iterable[float],
    spacing: float,
    *,
    tolerance: float = 1e-10,
) -> tuple[int, int]:
    """Return the target ``(ny, nx)`` implied by region and spacing.

    The MCMC grid is a user-defined contract: both regional extents must be
    divisible by ``spacing``, and both gridline endpoints are included.
    """

    values = np.asarray(region, dtype=float)
    if values.shape != (4,) or not np.isfinite(values).all():
        raise ValueError(
            "region must contain four finite values: xmin, xmax, ymin, ymax"
        )
    if not np.isfinite(spacing) or spacing <= 0:
        raise ValueError(f"grid_spacing must be finite and > 0, got {spacing}")

    xmin, xmax, ymin, ymax = values
    if xmax <= xmin or ymax <= ymin:
        raise ValueError(f"region must have positive extents, got {list(values)}")

    x_intervals = (xmax - xmin) / spacing
    y_intervals = (ymax - ymin) / spacing
    x_count = int(round(x_intervals))
    y_count = int(round(y_intervals))
    if not np.isclose(
        x_intervals, x_count, rtol=tolerance, atol=tolerance
    ) or not np.isclose(y_intervals, y_count, rtol=tolerance, atol=tolerance):
        raise ValueError(
            "region extents must be divisible by grid_spacing: "
            f"x extent/spacing={x_intervals:.12g}, "
            f"y extent/spacing={y_intervals:.12g}"
        )

    return y_count + 1, x_count + 1


@dataclass(frozen=True, eq=False)
class TargetGrid:
    """Authoritative inversion grid as ascending longitude/latitude axes.

    The grid is gridline-registered: both regional endpoints are nodes. Values
    are compared by identity, not by element-wise equality, because the axes are
    arrays.
    """

    x: np.ndarray
    y: np.ndarray

    def __post_init__(self) -> None:
        object.__setattr__(self, "x", np.asarray(self.x, dtype=float))
        object.__setattr__(self, "y", np.asarray(self.y, dtype=float))
        validate_regular_axis(self.x, "target longitude")
        validate_regular_axis(self.y, "target latitude")

    @classmethod
    def from_region(cls, region: Iterable[float], spacing: float) -> "TargetGrid":
        """Build the exact lon/lat axes used by every MCMC grid point."""

        ny, nx = target_shape(region, spacing)
        xmin, xmax, ymin, ymax = np.asarray(region, dtype=float)
        return cls(x=np.linspace(xmin, xmax, nx), y=np.linspace(ymin, ymax, ny))

    @property
    def shape(self) -> tuple[int, int]:
        return self.y.size, self.x.size

    @property
    def size(self) -> int:
        return self.x.size * self.y.size

    def meshgrid(self) -> tuple[np.ndarray, np.ndarray]:
        """Return ``(lon, lat)`` two-dimensional arrays of shape ``(ny, nx)``."""

        return np.meshgrid(self.x, self.y)

    def flat_lonlat(self) -> tuple[np.ndarray, np.ndarray]:
        """Return row-major flattened longitude/latitude coordinate arrays.

        This is the canonical point order shared by :meth:`meshgrid` consumers
        and flattened cubes; ``k = iy * nx + ix``.
        """

        lon, lat = self.meshgrid()
        return lon.ravel(), lat.ravel()


# =========================
# REGULAR SOURCE GRIDS
# =========================


def validate_regular_axis(axis: np.ndarray, name: str) -> None:
    """Require a finite, strictly increasing, evenly spaced source axis."""

    axis = np.asarray(axis, dtype=float)
    if axis.ndim != 1 or not np.isfinite(axis).all():
        raise ValueError(f"Regular grid {name} axis requires a finite 1-D coordinate")
    if axis.size < 2:
        raise ValueError(f"Regular grid {name} axis requires at least two nodes")
    steps = np.diff(axis)
    if np.any(steps <= 0) or not np.allclose(
        steps, steps[0], rtol=_SPACING_RTOL, atol=1e-9
    ):
        raise ValueError(
            f"Regular grid {name} axis must have uniform spacing and unique coordinates"
        )


def regular_grid_from_xyz(xyz: np.ndarray, name: str = "spatial field") -> xr.DataArray:
    """Recover a complete regular grid, independent of input row order."""

    data = np.asarray(xyz, dtype=float)
    if data.ndim != 2 or data.shape[1] != 3 or not np.isfinite(data).all():
        raise ValueError("Interpolation requires a finite N x 3 x-y-value array")

    x, ix = np.unique(data[:, 0], return_inverse=True)
    y, iy = np.unique(data[:, 1], return_inverse=True)
    ix = np.asarray(ix).reshape(-1)
    iy = np.asarray(iy).reshape(-1)
    validate_regular_axis(x, "longitude")
    validate_regular_axis(y, "latitude")
    if x.size * y.size != len(data):
        raise ValueError(
            "Regular source must be a complete rectangular grid without "
            "missing or duplicate nodes"
        )

    indices = iy * x.size + ix
    if np.unique(indices).size != len(data):
        raise ValueError(
            "Rectangular source contains duplicate coordinates and missing cells"
        )
    values = np.empty(len(data), dtype=float)
    values[indices] = data[:, 2]
    return xr.DataArray(
        values.reshape(y.size, x.size),
        coords={"y": y, "x": x},
        dims=("y", "x"),
        name=name,
    )


def long_to_grid(
    frame,
    value_column: str,
    extra_column: str,
    *,
    lon_column: str = "lon",
    lat_column: str = "lat",
    extra_name: str | None = None,
) -> xr.DataArray:
    """Pivot a long lon/lat/extra table into an ``(extra, y, x)`` cube.

    The lon/lat coordinates must form a strictly regular grid; off-grid
    coordinates are rejected rather than snapped. Duplicate rows are averaged,
    missing combinations remain NaN and all axes are ascending. The extra axis
    keeps its own sampled values.
    """

    extras = np.sort(np.asarray(frame[extra_column].unique(), dtype=float))
    lons = np.sort(np.asarray(frame[lon_column].unique(), dtype=float))
    lats = np.sort(np.asarray(frame[lat_column].unique(), dtype=float))
    for axis, name in ((lons, lon_column), (lats, lat_column)):
        if axis.size >= 2:
            validate_regular_axis(axis, name)

    work = frame[[lon_column, lat_column, extra_column, value_column]].copy()
    work["_ix"] = np.searchsorted(lons, work[lon_column].to_numpy(dtype=float))
    work["_iy"] = np.searchsorted(lats, work[lat_column].to_numpy(dtype=float))
    work["_ie"] = np.searchsorted(extras, work[extra_column].to_numpy(dtype=float))
    work = work.groupby(["_ie", "_iy", "_ix"], as_index=False)[value_column].mean()

    values = np.full((extras.size, lats.size, lons.size), np.nan, dtype=float)
    values[work["_ie"], work["_iy"], work["_ix"]] = work[value_column].to_numpy(
        dtype=float
    )
    dim = extra_name or extra_column
    return xr.DataArray(
        values,
        dims=(dim, "y", "x"),
        coords={dim: extras, "y": lats, "x": lons},
    )


# =========================
# ALIGNMENT
# =========================


def align_to_grid(
    source: xr.DataArray,
    target: TargetGrid,
    *,
    on_missing: MissingPolicy = "raise",
) -> xr.DataArray:
    """Align a regular source grid to the authoritative MCMC target grid.

    Parameters
    ----------
    source
        DataArray with ascending x (longitude) and y (latitude) coordinates.
        Any extra leading dimensions (period, z) are preserved.
    target
        Authoritative inversion grid.
    on_missing
        ``"raise"`` rejects an uncovered target region (used for fixed fields
        such as topography/Moho); ``"nan"`` keeps uncovered cells as NaN (used
        for partially covering ANT/Vs products).

    Returns
    -------
    xarray.DataArray
        Same dimensions as source with x/y replaced by the target axes. The
        resulting attributes record the per-axis strategy that was applied.
    """

    if "x" not in source.dims or "y" not in source.dims:
        raise ValueError("align_to_grid requires 'x' and 'y' dimensions")
    if on_missing not in ("raise", "nan"):
        raise ValueError(f"on_missing must be 'raise' or 'nan', got {on_missing!r}")

    xs = np.asarray(source["x"].values, dtype=float)
    ys = np.asarray(source["y"].values, dtype=float)
    _require_ascending(xs, "x")
    _require_ascending(ys, "y")

    x_mode = _classify_axis(xs, target.x, "longitude", on_missing)
    y_mode = _classify_axis(ys, target.y, "latitude", on_missing)

    values = np.asarray(source.values, dtype=float)
    values = _apply_axis(values, source.get_axis_num("x"), xs, target.x, x_mode)
    values = _apply_axis(values, source.get_axis_num("y"), ys, target.y, y_mode)

    coords = {
        dim: np.asarray(source[dim].values)
        for dim in source.dims
        if dim not in ("x", "y")
    }
    coords["x"] = target.x
    coords["y"] = target.y
    attrs = dict(source.attrs)
    attrs.update(source_align_x=x_mode, source_align_y=y_mode)
    return xr.DataArray(
        values,
        dims=tuple(source.dims),
        coords=coords,
        name=source.name,
        attrs=attrs,
    )


# =========================
# ALIGNMENT INTERNALS
# =========================


def _require_ascending(axis: np.ndarray, name: str) -> None:
    if axis.ndim != 1 or axis.size == 0 or not np.isfinite(axis).all():
        raise ValueError(f"{name} axis must be a finite 1-D coordinate")
    if axis.size > 1 and np.any(np.diff(axis) <= 0):
        raise ValueError(f"{name} axis must be strictly increasing")


def _source_spacing(nodes: np.ndarray, name: str) -> float:
    steps = np.diff(nodes)
    if steps.size == 0:
        raise ValueError(f"source {name} axis requires at least two nodes")
    if np.any(steps <= 0) or not np.allclose(
        steps, steps[0], rtol=_SPACING_RTOL, atol=1e-12
    ):
        raise ValueError(
            f"source {name} axis must be uniformly spaced; "
            "all MCMC inputs are declared as regular grids"
        )
    return float(steps[0])


def _covers(src: np.ndarray, dst: np.ndarray) -> bool:
    return bool(dst.size) and bool(
        dst.min() >= src.min() - _COORD_ATOL and dst.max() <= src.max() + _COORD_ATOL
    )


def _aligned(src: np.ndarray, dst: np.ndarray, spacing: float) -> bool:
    offset = (src[0] - dst[0]) / spacing
    return abs(offset - round(offset)) < _ALIGN_ATOL


def _classify_axis(
    src: np.ndarray,
    dst: np.ndarray,
    name: str,
    on_missing: MissingPolicy,
) -> str:
    """Choose ``identity``, ``single``, ``interp`` or ``aggregate`` for one axis."""

    if src.size < 2:
        if on_missing == "raise" and not (
            dst.size == 1 and np.isclose(src[0], dst[0], atol=_COORD_ATOL)
        ):
            raise ValueError(_coverage_error(src, dst, name))
        return "single"

    source_spacing = _source_spacing(src, name)
    target_spacing = float(np.diff(dst)[0])
    if not _covers(src, dst):
        if on_missing == "raise":
            raise ValueError(_coverage_error(src, dst, name))
        return "interp" if source_spacing >= target_spacing else "aggregate"
    if np.isclose(
        source_spacing, target_spacing, rtol=_SPACING_RTOL, atol=1e-12
    ) and _aligned(src, dst, target_spacing):
        return "identity"
    return "interp" if source_spacing >= target_spacing else "aggregate"


def _coverage_error(src: np.ndarray, dst: np.ndarray, name: str) -> str:
    return (
        f"source {name} extent [{src.min():g}, {src.max():g}] does not cover "
        f"target [{dst.min():g}, {dst.max():g}]; extrapolation is disabled"
    )


def _apply_axis(
    values: np.ndarray,
    axis: int,
    src: np.ndarray,
    dst: np.ndarray,
    mode: str,
) -> np.ndarray:
    if mode == "identity":
        return _select_axis(values, axis, src, dst)
    if mode == "single":
        return _select_single(values, axis, src, dst)
    if mode == "interp":
        return _interp_axis(values, axis, src, dst)
    return _aggregate_axis(values, axis, src, dst)


def _select_axis(
    values: np.ndarray, axis: int, src: np.ndarray, dst: np.ndarray
) -> np.ndarray:
    moved = np.moveaxis(values, axis, 0)
    out = np.full((dst.size,) + moved.shape[1:], np.nan, dtype=float)
    idx = np.searchsorted(src, dst)
    inside = idx < src.size
    valid = np.zeros(dst.size, dtype=bool)
    valid[inside] = np.isclose(src[idx[inside]], dst[inside], atol=_COORD_ATOL)
    out[valid] = moved[idx[valid]]
    return np.moveaxis(out, 0, axis)


def _select_single(
    values: np.ndarray, axis: int, src: np.ndarray, dst: np.ndarray
) -> np.ndarray:
    moved = np.moveaxis(values, axis, 0)
    out = np.full((dst.size,) + moved.shape[1:], np.nan, dtype=float)
    for target_index in np.where(np.isclose(dst, src[0], atol=_COORD_ATOL))[0]:
        out[target_index] = moved[0]
    return np.moveaxis(out, 0, axis)


def _interp_axis(
    values: np.ndarray, axis: int, src: np.ndarray, dst: np.ndarray
) -> np.ndarray:
    sampler = interp1d(
        src,
        values,
        axis=axis,
        kind="linear",
        bounds_error=False,
        fill_value=np.nan,
        assume_sorted=True,
    )
    return np.asarray(sampler(dst), dtype=float)


def _aggregate_axis(
    values: np.ndarray, axis: int, src: np.ndarray, dst: np.ndarray
) -> np.ndarray:
    src_spacing = _source_spacing(src, "source")
    dst_spacing = float(np.diff(dst)[0])
    weights = _conservative_weights(src, dst, src_spacing, dst_spacing)
    return _weighted_mean_axis(values, axis, weights)


def _edges(nodes: np.ndarray, spacing: float) -> np.ndarray:
    edges = np.empty(nodes.size + 1, dtype=float)
    edges[1:-1] = 0.5 * (nodes[:-1] + nodes[1:])
    edges[0] = nodes[0] - 0.5 * spacing
    edges[-1] = nodes[-1] + 0.5 * spacing
    return edges


def _conservative_weights(
    src: np.ndarray,
    dst: np.ndarray,
    src_spacing: float,
    dst_spacing: float,
) -> sparse.csr_matrix:
    """Area weights of a one-dimensional conservative remap (rows sum to 1)."""

    src_edges = _edges(src, src_spacing)
    dst_edges = _edges(dst, dst_spacing)
    rows: list[int] = []
    cols: list[int] = []
    data: list[float] = []
    for i in range(dst.size):
        j0 = int(np.searchsorted(src_edges, dst_edges[i], side="right")) - 1
        j1 = int(np.searchsorted(src_edges, dst_edges[i + 1], side="left"))
        j0 = max(j0, 0)
        j1 = min(j1, src.size)
        if j1 <= j0:
            continue
        js = np.arange(j0, j1)
        left = np.maximum(dst_edges[i], src_edges[js])
        right = np.minimum(dst_edges[i + 1], src_edges[js + 1])
        overlap = np.clip(right - left, 0.0, None)
        total = overlap.sum()
        if total <= 0:
            continue
        overlap = overlap / total
        nonzero = overlap > 0
        rows.extend([i] * int(nonzero.sum()))
        cols.extend(js[nonzero].tolist())
        data.extend(overlap[nonzero].tolist())
    return sparse.csr_matrix((data, (rows, cols)), shape=(dst.size, src.size))


def _weighted_mean_axis(
    values: np.ndarray, axis: int, weights: sparse.csr_matrix
) -> np.ndarray:
    """Conservative mean that ignores NaN source cells.

    Numerator and denominator are accumulated separately so a target cell is
    only NaN when every contributing source cell is NaN. This keeps partially
    sampled ANT/Vs products usable after downsampling.
    """

    moved = np.moveaxis(values, axis, -1)
    shape = moved.shape
    flat = moved.reshape(-1, shape[-1]).astype(float)
    finite = np.isfinite(flat)
    filled = np.where(finite, flat, 0.0)
    numerator = np.asarray((weights @ filled.T).T)
    denominator = np.asarray((weights @ finite.astype(float).T).T)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = numerator / denominator
    out[denominator == 0] = np.nan
    out = out.reshape(shape[:-1] + (weights.shape[0],))
    return np.moveaxis(out, -1, axis)
