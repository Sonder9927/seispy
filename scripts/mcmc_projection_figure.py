"""Regenerate the Greville-versus-projection figures used by the MCMC docs.

Each figure is a depth-versus-Vs panel in the style of the per-point
``point.png`` model panel: depth increases downward, the crust and mantle
splines are drawn as one continuous section split by the Moho line, and the raw
reference profile is overlaid with the reconstructed spline.

The reference is a real model. Both figures use point ``122.00_33.50`` of the
reference Vs model, sediment thickness and Moho depth that the project
configuration (``data/config-lyb.json``) points at, aligned with the same
``TargetGrid`` the preparation workflow uses. Layer bounds, coefficient counts,
``factor`` and the shallow-extrapolation policy are read from that
configuration too, so the figures track it automatically.

The input grids are not committed to the repository, so this script needs a
checkout that also has ``data/config-lyb.json`` and ``data/models``. The
generated PNGs are committed.

Run from the repository root:

    uv run python scripts/mcmc_projection_figure.py
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402

from seispy.mcmc.bspline import (  # noqa: E402
    basis_geometry,
    basis_matrix,
    greville_depths,
    projection_coefficients,
)
from seispy.mcmc.config import load_config  # noqa: E402
from seispy.mcmc.gridding import TargetGrid  # noqa: E402
from seispy.mcmc.inputs import (  # noqa: E402
    DEPTH_NAMES,
    LAT_NAMES,
    LON_NAMES,
    VS_NAMES,
    pick_name,
    read_table,
)
from seispy.mcmc.priors import PROJECTION_SAMPLES  # noqa: E402
from seispy.mcmc.spatial import (  # noqa: E402
    ScalarFieldSpec,
    interpolate_regular_grid,
    read_spatial_scalar,
)
from seispy.mcmc.velocity import VsProfile, velocity_at_depths  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "data" / "config-lyb.json"
ASSET_DIR = ROOT / "docs" / "assets"

POINT = (122.0, 33.5)

LAYER_ORDER = ("crust", "mantle")
LAYER_STYLE = {
    "crust": {"color": "#0072B2", "marker": "o"},
    "mantle": {"color": "#D55E00", "marker": "s"},
}

REFERENCE_COLOR = "0.55"
SEDIMENT_COLOR = "#009E73"
X_LIMITS = (2.4, 4.95)
Y_LIMITS = (305.0, 0.0)

METHODS = {
    "greville": r"Greville sampling: $c_j = V_{s,\mathrm{ref}}(\xi_j)$",
    "projection": (
        r"Least-squares projection: "
        r"$c^*=\arg\min_c\|\sum_j c_j B_j - V_{s,\mathrm{ref}}\|$"
    ),
}


@dataclass(frozen=True)
class ReferenceModel:
    """One real reference profile with its configured layer geometry."""

    profile: VsProfile
    z_sediment: float
    z_moho: float
    z_max: float
    factor: float
    n_basis: dict[str, int]
    allow_shallow: bool
    max_shallow: float | None

    def reference(self, depths: np.ndarray) -> np.ndarray:
        """Return the reference Vs at ``depths`` (km).

        Uses the same shallow-extrapolation policy as the preparation code, so
        a layer that starts above the profile coverage is sampled identically.
        """

        return velocity_at_depths(
            self.profile,
            np.asarray(depths, dtype=float),
            allow_shallow_extrapolation=self.allow_shallow,
            max_shallow_extrapolation_km=self.max_shallow,
        )

    def layer(self, name: str) -> tuple[float, float, int]:
        """Return ``(z_top, z_bottom, n_basis)`` for one layer."""

        if name == "crust":
            return self.z_sediment, self.z_moho, self.n_basis["crust"]
        return self.z_moho, self.z_max, self.n_basis["mantle"]


def _grid_value(
    path: Path,
    spec: ScalarFieldSpec,
    target: TargetGrid,
    region: list[float],
) -> float:
    """Read one aligned grid value at the example point."""

    lon, lat = target.flat_lonlat()
    index = int(np.argmin((lon - POINT[0]) ** 2 + (lat - POINT[1]) ** 2))
    xyz = read_spatial_scalar(path, spec, region=region)
    return float(interpolate_regular_grid(xyz, target).values.ravel()[index])


def load_reference_model() -> ReferenceModel:
    """Load the configured profile and geometry for the example point."""

    cfg = load_config(CONFIG_PATH)
    target = TargetGrid.from_region(cfg.region, cfg.grid_spacing)

    frame = read_table(cfg.paths.vs_model_file)
    lon_col = pick_name(frame.columns, LON_NAMES)
    lat_col = pick_name(frame.columns, LAT_NAMES)
    depth_col = pick_name(frame.columns, DEPTH_NAMES)
    vs_col = pick_name(frame.columns, VS_NAMES)
    if lon_col is None or lat_col is None or depth_col is None or vs_col is None:
        raise ValueError(
            f"{cfg.paths.vs_model_file.name} is missing lon/lat/depth/vs columns"
        )

    selected = frame[(frame[lon_col] == POINT[0]) & (frame[lat_col] == POINT[1])]
    if selected.empty:
        raise ValueError(f"{cfg.paths.vs_model_file.name} has no profile at {POINT}")
    selected = selected.sort_values(depth_col)

    sediment = _grid_value(
        cfg.paths.sediment_file,
        ScalarFieldSpec(
            "sediment thickness",
            convention="positive_thickness",
            value_columns=("sediment", "sediment_thickness", "thickness", "sed", "z"),
        ),
        target,
        list(cfg.region),
    )
    moho = _grid_value(
        cfg.paths.moho_file,
        ScalarFieldSpec(
            "Moho depth",
            convention="positive_depth",
            value_columns=("moho", "moho_depth", "depth", "z"),
            allow_zero=False,
        ),
        target,
        list(cfg.region),
    )
    profile = VsProfile(
        lon=POINT[0],
        lat=POINT[1],
        depth=selected[depth_col].to_numpy(dtype=float),
        vs=selected[vs_col].to_numpy(dtype=float),
    )
    return ReferenceModel(
        profile=profile,
        z_sediment=sediment,
        z_moho=moho,
        z_max=float(cfg.zmax_Bs),
        factor=float(cfg.factor),
        n_basis={"crust": cfg.n_coeff_crust, "mantle": cfg.n_coeff_mantle},
        allow_shallow=bool(cfg.vs_constraints.allow_shallow_extrapolation),
        max_shallow=cfg.vs_constraints.max_shallow_extrapolation_km,
    )


def sample_depths(z_top: float, z_bottom: float) -> np.ndarray:
    """Return the interior depth samples used by the projection and the audit.

    These are the same interior samples the preparation code projects onto, so
    the errors annotated here match the residual reported in ``point.png`` and
    ``prior_bounds.csv``.
    """

    return np.linspace(z_top, z_bottom, PROJECTION_SAMPLES + 2)[1:-1]


def greville_centers(
    model: ReferenceModel, layer: str
) -> tuple[np.ndarray, np.ndarray]:
    """Return the Greville depth and sampled coefficient of each coefficient."""

    z_top, z_bottom, n_basis = model.layer(layer)
    depths = greville_depths(n_basis, z_top, z_bottom, model.factor)
    return depths, model.reference(depths)


def projection_centers(
    model: ReferenceModel, layer: str
) -> tuple[np.ndarray, np.ndarray]:
    """Return the basis centroid and projected coefficient of each coefficient."""

    z_top, z_bottom, n_basis = model.layer(layer)
    sample = sample_depths(z_top, z_bottom)
    coefficients, _ = projection_coefficients(
        n_basis, z_top, z_bottom, model.factor, sample, model.reference(sample)
    )
    _, centroid = basis_geometry(n_basis, z_top, z_bottom, model.factor)
    return centroid, coefficients


def evaluate(
    model: ReferenceModel,
    layer: str,
    coefficients: np.ndarray,
    *,
    samples: int = 2001,
) -> tuple[np.ndarray, np.ndarray]:
    """Return depths and the reconstructed Vs, with the clamped endpoints."""

    z_top, z_bottom, n_basis = model.layer(layer)
    depths = np.linspace(z_top, z_bottom, samples)
    interior = depths[1:-1]
    basis = basis_matrix(n_basis, z_top, z_bottom, model.factor, interior)
    values = np.concatenate(
        ([coefficients[0]], basis @ coefficients, [coefficients[-1]])
    )
    return depths, values


def layer_errors(
    model: ReferenceModel,
    layer: str,
    coefficients: np.ndarray,
) -> tuple[float, float]:
    """Return the maximum and RMS reconstruction error of one layer, in km/s.

    Both are measured on :func:`sample_depths`, the same interior samples the
    projection uses, so the projection maximum equals the per-point audit
    residual.
    """

    z_top, z_bottom, n_basis = model.layer(layer)
    sample = sample_depths(z_top, z_bottom)
    basis = basis_matrix(n_basis, z_top, z_bottom, model.factor, sample)
    residual = basis @ coefficients - model.reference(sample)
    return float(np.max(np.abs(residual))), float(np.sqrt((residual**2).mean()))


def draw(
    model: ReferenceModel,
    method: str,
    output: Path,
) -> dict[str, tuple[float, float]]:
    """Draw one method (``greville`` or ``projection``) into ``output``."""

    if method not in METHODS:
        raise ValueError(f"unknown method: {method!r}")
    centers = greville_centers if method == "greville" else projection_centers

    figure, ax = plt.subplots(figsize=(5.8, 6.6), constrained_layout=True)
    errors: dict[str, tuple[float, float]] = {}

    for layer in LAYER_ORDER:
        style = LAYER_STYLE[layer]
        reading_depth, coefficients = centers(model, layer)
        depths, fit = evaluate(model, layer, coefficients)
        truth = model.reference(depths)
        errors[layer] = layer_errors(model, layer, coefficients)

        ax.fill_betweenx(depths, truth, fit, color=style["color"], alpha=0.16, zorder=1)
        # The reference is drawn thick and underneath so it stays visible where
        # a fit lies exactly on top of it.
        ax.plot(
            truth,
            depths,
            color=REFERENCE_COLOR,
            linewidth=3.4,
            alpha=0.9,
            label="reference" if layer == LAYER_ORDER[0] else None,
            zorder=2,
        )
        ax.plot(
            fit,
            depths,
            color=style["color"],
            linewidth=1.8,
            label=f"{layer} fit",
            zorder=3,
        )
        ax.plot(
            coefficients,
            reading_depth,
            style["marker"],
            color=style["color"],
            markersize=6,
            markeredgecolor="white",
            markeredgewidth=0.8,
            label=f"{layer} coefficients",
            zorder=4,
        )

    ax.axhspan(0.0, model.z_sediment, color=SEDIMENT_COLOR, alpha=0.12, zorder=0)
    ax.axhline(model.z_sediment, color=SEDIMENT_COLOR, linewidth=1.0, linestyle="--")
    ax.axhline(model.z_moho, color="black", linewidth=1.2)
    ax.axhline(model.z_max, color="0.3", linewidth=1.0, linestyle=":")
    for depth, text in (
        (model.z_sediment, "sediment bottom"),
        (model.z_moho, "Moho"),
        (model.z_max, "model bottom"),
    ):
        ax.text(
            0.01,
            depth,
            text,
            transform=ax.get_yaxis_transform(),
            ha="left",
            va="bottom",
            fontsize=8,
            color="0.2",
        )

    for layer, level in (("crust", 0.62), ("mantle", 0.565)):
        style = LAYER_STYLE[layer]
        maximum, rms = errors[layer]
        ax.text(
            0.02,
            level,
            f"{layer}  max |ΔVs| = {maximum:.3f}   rms = {rms:.3f} km/s",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=8.5,
            color=style["color"],
            bbox={
                "boxstyle": "round,pad=0.25",
                "fc": "white",
                "ec": style["color"],
                "alpha": 0.9,
            },
        )

    ax.set_xlim(*X_LIMITS)
    ax.set_ylim(*Y_LIMITS)
    ax.set_xlabel("Vs (km/s)")
    ax.set_ylabel("Depth (km)")
    ax.set_title(METHODS[method], fontsize=10)
    ax.grid(alpha=0.2)
    ax.legend(loc="lower left", fontsize=8, framealpha=0.9)

    figure.savefig(output, dpi=200)
    plt.close(figure)
    print(f"wrote {output}")
    for layer in LAYER_ORDER:
        maximum, rms = errors[layer]
        _, coefficients = centers(model, layer)
        print(
            f"  {layer:6s} max {maximum:.4f}  rms {rms:.4f} km/s  "
            f"coefficients {np.array2string(coefficients, precision=3)}"
        )
    return errors


def main() -> None:
    model = load_reference_model()
    print(
        f"point {POINT[0]:.2f}_{POINT[1]:.2f}: sediment {model.z_sediment:.4f} km, "
        f"Moho {model.z_moho:.4f} km, {model.profile.depth.size} reference samples"
    )
    ASSET_DIR.mkdir(parents=True, exist_ok=True)
    greville = draw(model, "greville", ASSET_DIR / "mcmc-greville-centers.png")
    projection = draw(model, "projection", ASSET_DIR / "mcmc-projection-centers.png")
    print("\nRMS improvement (Greville / projection):")
    for layer in LAYER_ORDER:
        print(f"  {layer:6s} {greville[layer][1] / projection[layer][1]:.2f}x")


if __name__ == "__main__":
    main()
