"""Read retained Fortran states and reconstruct the forward layer geometry.

No accepted-event export files are used. Iteration multiplicities are retained.
The geometry follows generate_s, including its distinct basis-node and layer
spacing conventions; interpolation for posterior profiles is layer-constant.
"""

from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd

from seispy.mcmc.bspline import node_matrix
from seispy.mcmc.lab import LABSettings, identify_lab


@lru_cache(maxsize=64)
def _node_basis(n_basis, factor, n_nodes):
    # The basis on equally spaced nodes is invariant under affine depth changes.
    return node_matrix(n_basis, 0.0, 1.0, factor, n_nodes)


@dataclass(frozen=True)
class ModelLayout:
    factor: float
    maximum: float
    crust_nodes: int
    mantle_nodes: int
    shallow: float
    ice_velocity: float
    ice: bool
    water: bool
    sediment: bool
    keys: tuple[tuple[int, int], ...]

    @classmethod
    def read(cls, path: Path):
        lines = iter(
            line.strip() for line in path.read_text().splitlines() if line.strip()
        )

        def number():
            return float(next(lines).split()[0])

        smooth = int(number())
        if smooth:
            next(lines)
        ice = bool(number())
        shallow, ice_velocity = 0.0, 0.0
        if ice:
            shallow, ice_velocity = map(float, next(lines).split())
        water = bool(number())
        if water:
            shallow = number()
        sediment = bool(number())
        factor, maximum = number(), number()
        nc, nm = int(number()), int(number())
        next(lines)  # reference model path
        records = [line.split() for line in lines]
        if any(len(row) != 4 for row in records):
            raise ValueError("Invalid para.inp parameter records")
        keys = tuple((int(row[0]), int(row[1])) for row in records)
        if len(set(keys)) != len(keys) or (0, 1) not in keys:
            raise ValueError("Duplicate parameters or missing Moho parameter")
        if nc < 2 or nm < 2 or factor <= 0 or maximum <= 0:
            raise ValueError("Invalid Fortran model geometry")
        if ice and water or sediment and (ice or water):
            raise ValueError("Combined shallow-layer modes are unsupported")
        return cls(
            factor, maximum, nc, nm, shallow, ice_velocity, ice, water, sediment, keys
        )

    def reconstruct(self, parameters):
        p = dict(zip(self.keys, parameters, strict=True))
        moho = p[0, 1]
        shallow = p[0, 0] if self.sediment else self.shallow
        if not 0 <= shallow < moho < self.maximum:
            raise ValueError("Invalid sampled interface order")
        nc, nm = self.crust_nodes, self.mantle_nodes
        crust_z = shallow + np.arange(nc) * (moho - shallow) / nc
        mantle_z = moho + np.arange(nm) * (self.maximum - moho) / nm
        if self.sediment:
            # generate_s initializes prof(3)=sedi_Bs+dz_cBs.
            crust_z += (moho - shallow) / nc
            mantle_z += (moho - shallow) / nc
        mantle_z[-1] = self.maximum
        prefix = (
            [0.0, shallow]
            if self.sediment
            else ([0.0] if self.water or self.ice else [])
        )
        z = np.r_[prefix, crust_z, mantle_z]

        if np.any(np.diff(z) < 0):
            raise ValueError("Fortran layer depths are not monotonic")

        def coefficients(group):
            keys = sorted(k for k in p if k[0] == group)
            if [k[1] for k in keys] != list(range(1, len(keys) + 1)) or len(keys) < 3:
                raise ValueError(f"Invalid B-spline coefficient group {group}")
            return np.array([p[k] for k in keys])

        def field(cgroup, mgroup):
            c, m = coefficients(cgroup), coefficients(mgroup)
            cv = _node_basis(len(c), self.factor, nc) @ c
            mv = _node_basis(len(m), self.factor, nm) @ m
            return np.r_[np.zeros(len(prefix)), cv, mv]

        groups = {k[0] for k in p}
        first, second = {1, 2} <= groups, {3, 4} <= groups
        if first and second:
            v, a = field(1, 2), field(3, 4)
            if prefix:
                a[: len(prefix)] = 0
            fields = {"v": v, "a": a}
        elif first:
            fields = {"vsv": field(1, 2)}
        elif second:
            fields = {"vsh": field(3, 4)}
        else:
            raise ValueError("No complete velocity parameter groups")
        for name, values in fields.items():
            if name == "a":
                continue
            if self.sediment:
                values[:2] = [p[10, 1], p[10, 2]]
            elif self.water or self.ice:
                values[0] = self.ice_velocity if self.ice else 0
        if first and second:
            fields["vsv"] = fields["v"] * (1 - fields["a"] / 3)
            fields["vsh"] = fields["v"] * (1 + 2 * fields["a"] / 3)
        if first and second:
            fields["xi"] = np.divide(
                fields["vsh"] ** 2,
                fields["vsv"] ** 2,
                out=np.full(z.size, np.nan),
                where=fields["vsv"] != 0,
            )
        return z, fields, moho


def stats(values, weights=None):
    """Exact empirical quantiles with iteration multiplicities, not jump weights."""
    values = np.asarray(values, dtype=float)
    if weights is not None:
        values = np.repeat(values, weights)
    values = values[np.isfinite(values)]
    if not values.size:
        return {
            key: np.nan
            for key in ("mean", "std", "q025", "q16", "median", "q84", "q975")
        }
    q = np.quantile(values, [0.025, 0.16, 0.5, 0.84, 0.975])
    return dict(
        mean=float(values.mean()),
        std=float(values.std(ddof=1)) if values.size > 1 else np.nan,
        **dict(zip(("q025", "q16", "median", "q84", "q975"), q, strict=True)),
    )


def read_chain(directory, layout):
    control = (directory / "input_DRAM_T.dat").read_text().splitlines()
    names = [s.strip() for s in control[0].split("|")]
    vals = control[1].split()
    settings = dict(
        zip(
            names,
            (float(v.replace("D", "e").replace("d", "e")) for v in vals),
            strict=True,
        )
    )
    if settings["biasfac"] < 1:
        raise ValueError(
            "biasfac < 1 modifies acceptance; not a standard posterior chain"
        )
    burn = int(settings["burn_in"])
    if burn < 1:
        raise ValueError("Fortran retained-chain indexing requires burn_in >= 1")
    lines = (directory / "fort.8010").read_text().splitlines()
    if len(lines) < 3 or "misfit" not in lines[1].lower():
        raise ValueError("Expected MCMC chain headers (not --forward output)")
    chain = np.loadtxt(
        [line.replace("D", "e").replace("d", "e") for line in lines[2:]], ndmin=2
    )
    expected = int(settings["nsimu"]) - burn + 1
    if chain.shape != (expected, len(layout.keys) + 1) or not np.isfinite(chain).all():
        raise ValueError("Retained chain shape/values do not match run inputs")
    return chain, burn


def collect_posterior(directory: Path, depths, settings: LABSettings):
    layout = ModelLayout.read(directory / "para.inp")
    chain, burn = read_chain(directory, layout)
    parameters, inverse, counts = np.unique(
        chain[:, 1:], axis=0, return_inverse=True, return_counts=True
    )
    profiles = {}
    sample_rows = []
    for sample in parameters:
        z, fields, moho = layout.reconstruct(sample)
        indices = np.clip(np.searchsorted(z, depths, side="right") - 1, 0, len(z) - 1)
        if np.any(depths < 0) or np.any(depths > layout.maximum):
            raise ValueError("Requested profile depths exceed model coverage")
        for name, values in fields.items():
            profiles.setdefault(name, []).append(values[indices])
        component = "vsv" if "vsv" in fields else "vsh"
        mantle = np.arange(len(z)) >= len(z) - layout.mantle_nodes
        eval_z = np.arange(z[mantle][0], layout.maximum, settings.step_km)
        # Interpolate forward mantle nodes, then smooth at an explicit physical scale.
        eval_v = np.interp(eval_z, z[mantle], fields[component][mantle])
        lab = identify_lab(eval_z, eval_v, moho, settings)
        sample_rows.append({"moho_km": moho, "lab_component": component, **lab})
    samples = pd.DataFrame(sample_rows)
    samples["multiplicity"] = counts
    samples["sample_id"] = np.arange(len(samples))
    samples["mantle_lithosphere_km"] = samples.lab_depth_km - samples.moho_km
    summary = {
        "retained_states": len(chain),
        "unique_states": len(samples),
        "chain_objective_min": float(chain[:, 0].min()),
        "chain_objective_min_iteration": int(chain[:, 0].argmin() + burn),
        "lab_component": sample_rows[0]["lab_component"],
    }
    for field in (
        "moho_km",
        "lab_depth_km",
        "lab_width_km",
        "lab_drop_km_s",
        "mantle_lithosphere_km",
    ):
        summary.update(
            {
                f"{field}_{key}": value
                for key, value in stats(samples[field], counts).items()
            }
        )
    for status in (
        "detected",
        "ambiguous",
        "boundary_limited",
        "not_detected",
        "insufficient_coverage",
    ):
        summary[f"lab_{status}_fraction"] = float(
            counts[samples.lab_status == status].sum() / len(chain)
        )
    profile_summary = {}
    for name, values in profiles.items():
        arr = np.asarray(values)
        per_depth = [stats(arr[:, i], counts) for i in range(len(depths))]
        for key in per_depth[0]:
            profile_summary[f"{name}_{key}"] = [row[key] for row in per_depth]
    labels = [f"{group}:{index}" for group, index in layout.keys]
    ess = [effective_sample_size(chain[:, i + 1]) for i in range(len(labels))]
    finite_ess = np.asarray(ess)[np.isfinite(ess)]
    summary["parameter_ess_min"] = (
        float(finite_ess.min()) if finite_ess.size else np.nan
    )
    summary["parameter_ess_unavailable_count"] = int(len(ess) - finite_ess.size)
    summary["convergence_status"] = "single_chain_not_certified"
    summary["rhat"] = np.nan
    cov = np.atleast_2d(np.cov(chain[:, 1:], rowvar=False))
    covariance = pd.DataFrame(
        [(a, b, cov[i, j]) for i, a in enumerate(labels) for j, b in enumerate(labels)],
        columns=["parameter_i", "parameter_j", "covariance"],
    )
    # Keep the chain order as well as compressed derived samples for later diagnostics.
    retained = pd.DataFrame(chain, columns=["objective", *labels])
    retained.insert(0, "iteration", np.arange(burn, burn + len(chain)))
    retained["sample_id"] = inverse
    return pd.DataFrame(profile_summary), summary, samples, covariance, retained


def effective_sample_size(values):
    """Single-chain initial-positive monotone-pair autocorrelation estimate.

    Returns NaN for short or constant series. This is a mixing diagnostic,
    not a rank-normalized multi-chain convergence test.
    """
    x = np.asarray(values, dtype=float)
    n = x.size
    if n < 20 or not np.isfinite(x).all():
        return np.nan
    x = x - x.mean()
    if np.dot(x, x) <= np.finfo(float).tiny:
        return np.nan
    spectrum = np.fft.rfft(x, n=2 * n)
    acf = np.fft.irfft(spectrum * spectrum.conjugate())[:n]
    acf /= acf[0]
    pairs = acf[: n - n % 2].reshape(-1, 2).sum(axis=1)
    stop = np.flatnonzero(pairs <= 0)
    pairs = pairs[: stop[0]] if stop.size else pairs
    if not pairs.size:
        return float(n)
    tau = max(1.0, -1.0 + 2.0 * np.minimum.accumulate(pairs).sum())
    return float(min(n, n / tau))
