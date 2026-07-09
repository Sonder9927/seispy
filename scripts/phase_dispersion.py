"""
Build merged phase-dispersion CSV files for MCMC inversion.

This script provides two public entry points:

1. generate_lyb_phase_dispersion(...)
   - Reads gridded ANT and TPWT files from data/lyb_data.
   - ANT files:  ant_grids/ant_vel_<period>.grid
   - TPWT files: tpwt_grids/tpwt_vel_<period>.grid and tpwt_std_<period>.grid
   - Each grid file is whitespace-separated with columns: longitude latitude value.
   - If ANT and TPWT both exist at the same lon-lat-period point, the phase
     velocity is averaged and TPWT std is retained.

2. generate_nz_phase_dispersion(...)
   - Reads point-wise ANT dispersion curves and a TPWT CSV.
   - ANT files: <longitude>_<latitude>_dispersion.dat, with columns period phv.
   - TPWT CSV columns: period, longitude, latitude, phv, std.
   - Merging rule:
       period < 20 s           -> ANT only
       period in 20,25,30,35 s -> mean of ANT and TPWT if both exist
       period > 35 s           -> TPWT only
       other periods           -> ignored

The output CSV always uses the common format:
    period, longitude, latitude, phv, std, source

Missing std values are intentionally preserved as NaN.  The downstream MCMC
input generator should assign its default uncertainty only when writing
phase.input.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd


PHASE_COLUMNS = ["period", "longitude", "latitude", "phv", "std", "source"]
DEFAULT_NZ_MERGE_PERIODS = (20.0, 25.0, 30.0, 35.0)


# =============================================================================
# Generic helpers
# =============================================================================


def _as_path(path: str | Path) -> Path:
    return path if isinstance(path, Path) else Path(path)


def _parse_period_from_filename(path: Path, prefix: str, suffix: str = ".grid") -> float:
    """Parse period from names such as ant_vel_10.grid or tpwt_vel_20.grid."""

    pattern = rf"^{re.escape(prefix)}_(\d+(?:\.\d+)?){re.escape(suffix)}$"
    match = re.match(pattern, path.name)
    if match is None:
        raise ValueError(f"Cannot parse period from filename: {path.name}")
    return float(match.group(1))


def _parse_nz_ant_lon_lat(path: Path) -> tuple[float, float]:
    """Parse lon/lat from names such as 172.00_-41.50_dispersion.dat."""

    pattern = r"^([+-]?\d+(?:\.\d+)?)_([+-]?\d+(?:\.\d+)?)_dispersion\.dat$"
    match = re.match(pattern, path.name)
    if match is None:
        raise ValueError(f"Cannot parse lon/lat from filename: {path.name}")
    return float(match.group(1)), float(match.group(2))


def _read_whitespace_grid(path: str | Path, value_name: str) -> pd.DataFrame:
    """Read a whitespace-separated lon-lat-value grid file.

    The input file may contain multiple spaces or leading spaces.  Invalid rows
    are removed after numeric conversion.
    """

    path = _as_path(path)
    df = pd.read_csv(
        path,
        sep=r"\s+",
        header=None,
        names=["longitude", "latitude", value_name],
        usecols=[0, 1, 2],
        engine="python",
    )

    for column in ["longitude", "latitude", value_name]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df.dropna(subset=["longitude", "latitude", value_name]).reset_index(
        drop=True
    )


def _read_period_phv_curve(path: str | Path) -> pd.DataFrame:
    """Read a whitespace-separated period-phv ANT dispersion curve."""

    path = _as_path(path)
    df = pd.read_csv(
        path,
        sep=r"\s+",
        header=None,
        names=["period", "phv"],
        usecols=[0, 1],
        engine="python",
    )
    df["period"] = pd.to_numeric(df["period"], errors="coerce")
    df["phv"] = pd.to_numeric(df["phv"], errors="coerce")
    return df.dropna(subset=["period", "phv"]).reset_index(drop=True)


def _read_tpwt_csv(path: str | Path) -> pd.DataFrame:
    """Read a TPWT CSV and return the standard phase-dispersion columns."""

    path = _as_path(path)
    df = pd.read_csv(path)

    required = {"period", "longitude", "latitude", "phv", "std"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"TPWT file missing columns: {sorted(missing)}")

    df = df[["period", "longitude", "latitude", "phv", "std"]].copy()
    for column in ["period", "longitude", "latitude", "phv", "std"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df.dropna(subset=["period", "longitude", "latitude", "phv"]).reset_index(
        drop=True
    )


def _period_isin(values: pd.Series, periods: Iterable[float], atol: float = 1e-6) -> pd.Series:
    """Float-safe replacement for Series.isin when periods are decimals."""

    values_array = values.to_numpy(dtype=float)
    mask = np.zeros(len(values_array), dtype=bool)
    for period in periods:
        mask |= np.isclose(values_array, float(period), atol=atol, rtol=0.0)
    return pd.Series(mask, index=values.index)


def _sort_phase_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Return a consistently ordered phase-dispersion DataFrame."""

    if df.empty:
        return pd.DataFrame(columns=PHASE_COLUMNS)

    return (
        df[PHASE_COLUMNS]
        .sort_values(["longitude", "latitude", "period"])
        .reset_index(drop=True)
    )


def _write_phase_csv(df: pd.DataFrame, out_file: str | Path | None) -> pd.DataFrame:
    """Write output CSV when requested and return the DataFrame unchanged."""

    if out_file is not None:
        out_path = _as_path(out_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_path, index=False)
        print(f"Saved: {out_path}")
        print(f"Rows: {len(df)}")
        if "source" in df.columns and len(df):
            print(df["source"].value_counts())
    return df


# =============================================================================
# Lyb data workflow
# =============================================================================


def _read_lyb_ant_grids(ant_dir: str | Path) -> pd.DataFrame:
    """Read Lyb ANT grids into period-longitude-latitude-phv format."""

    ant_dir = _as_path(ant_dir)
    rows: list[pd.DataFrame] = []

    for file in sorted(ant_dir.glob("ant_vel_*.grid")):
        period = _parse_period_from_filename(file, prefix="ant_vel")
        df = _read_whitespace_grid(file, value_name="phv")
        df["period"] = period
        rows.append(df)

    if not rows:
        raise FileNotFoundError(f"No Lyb ANT grid files found in {ant_dir}")

    ant = pd.concat(rows, ignore_index=True)
    return ant[["period", "longitude", "latitude", "phv"]]


def _read_lyb_tpwt_grids(tpwt_dir: str | Path) -> pd.DataFrame:
    """Read Lyb TPWT velocity grids and optional std grids."""

    tpwt_dir = _as_path(tpwt_dir)
    rows: list[pd.DataFrame] = []

    for vel_file in sorted(tpwt_dir.glob("tpwt_vel_*.grid")):
        period = _parse_period_from_filename(vel_file, prefix="tpwt_vel")
        vel = _read_whitespace_grid(vel_file, value_name="phv")
        vel["period"] = period

        std_file = tpwt_dir / f"tpwt_std_{period:g}.grid"
        if std_file.exists():
            std = _read_whitespace_grid(std_file, value_name="std")
            std["period"] = period
            df = pd.merge(
                vel,
                std,
                on=["period", "longitude", "latitude"],
                how="left",
            )
        else:
            df = vel.copy()
            df["std"] = np.nan
            print(f"[WARN] Missing TPWT std grid for period={period:g}: {std_file}")

        rows.append(df)

    if not rows:
        raise FileNotFoundError(f"No Lyb TPWT velocity grid files found in {tpwt_dir}")

    tpwt = pd.concat(rows, ignore_index=True)
    return tpwt[["period", "longitude", "latitude", "phv", "std"]]


def _merge_lyb_ant_tpwt(ant: pd.DataFrame, tpwt: pd.DataFrame) -> pd.DataFrame:
    """Merge Lyb ANT and TPWT data.

    Lyb uses a simple source rule:
    - ANT + TPWT at the same lon-lat-period: average phv and keep TPWT std.
    - ANT only: keep ANT phv and std=NaN.
    - TPWT only: keep TPWT phv and std.
    """

    merged = pd.merge(
        ant.rename(columns={"phv": "phv_ant"}),
        tpwt.rename(columns={"phv": "phv_tpwt", "std": "std_tpwt"}),
        on=["period", "longitude", "latitude"],
        how="outer",
    )

    has_ant = np.isfinite(merged["phv_ant"])
    has_tpwt = np.isfinite(merged["phv_tpwt"])
    keep = has_ant | has_tpwt
    merged = merged.loc[keep].copy()
    has_ant = has_ant.loc[keep]
    has_tpwt = has_tpwt.loc[keep]

    merged["phv"] = np.nan
    merged.loc[has_ant & has_tpwt, "phv"] = merged.loc[
        has_ant & has_tpwt, ["phv_ant", "phv_tpwt"]
    ].mean(axis=1)
    merged.loc[has_ant & ~has_tpwt, "phv"] = merged.loc[has_ant & ~has_tpwt, "phv_ant"]
    merged.loc[~has_ant & has_tpwt, "phv"] = merged.loc[~has_ant & has_tpwt, "phv_tpwt"]

    merged["std"] = merged["std_tpwt"]
    merged["source"] = np.select(
        [has_ant & has_tpwt, has_ant & ~has_tpwt, ~has_ant & has_tpwt],
        ["ANT_TPWT_MEAN", "ANT", "TPWT"],
        default="UNKNOWN",
    )

    return _sort_phase_dataframe(merged)


def generate_lyb_phase_dispersion(
    base_dir: str | Path = "data/lyb_data",
    out_file: str | Path | None = "data/mcmc/phase_dispersion_lyb.csv",
) -> pd.DataFrame:
    """Generate phase_dispersion.csv from Lyb gridded ANT and TPWT data."""

    base_dir = _as_path(base_dir)
    ant = _read_lyb_ant_grids(base_dir / "ant_grids")
    tpwt = _read_lyb_tpwt_grids(base_dir / "tpwt_grids")
    phase = _merge_lyb_ant_tpwt(ant, tpwt)
    return _write_phase_csv(phase, out_file)


# =============================================================================
# New Zealand data workflow
# =============================================================================


def _read_nz_ant_dispersion(ant_dir: str | Path) -> pd.DataFrame:
    """Read NZ ANT point-wise dispersion curves from lon_lat filenames."""

    ant_dir = _as_path(ant_dir)
    rows: list[pd.DataFrame] = []

    for file in sorted(ant_dir.glob("*_dispersion.dat")):
        lon, lat = _parse_nz_ant_lon_lat(file)
        df = _read_period_phv_curve(file)
        df["longitude"] = lon
        df["latitude"] = lat
        rows.append(df)

    if not rows:
        raise FileNotFoundError(f"No NZ ANT dispersion files found in {ant_dir}")

    ant = pd.concat(rows, ignore_index=True)
    return ant[["period", "longitude", "latitude", "phv"]]


def _merge_nz_ant_tpwt(
    ant: pd.DataFrame,
    tpwt: pd.DataFrame,
    merge_periods: Sequence[float] = DEFAULT_NZ_MERGE_PERIODS,
) -> pd.DataFrame:
    """Merge NZ ANT and TPWT data using the project-specific period rule."""

    merged = pd.merge(
        ant.rename(columns={"phv": "phv_ant"}),
        tpwt.rename(columns={"phv": "phv_tpwt", "std": "std_tpwt"}),
        on=["period", "longitude", "latitude"],
        how="outer",
    )

    has_ant = np.isfinite(merged["phv_ant"])
    has_tpwt = np.isfinite(merged["phv_tpwt"])
    is_merge_period = _period_isin(merged["period"], merge_periods)

    use_ant_only = (merged["period"] < 20.0) & has_ant
    use_mean = is_merge_period & (has_ant | has_tpwt)
    use_tpwt_only = (merged["period"] > 35.0) & has_tpwt
    keep = use_ant_only | use_mean | use_tpwt_only

    out = merged.loc[keep].copy()
    has_ant = has_ant.loc[keep]
    has_tpwt = has_tpwt.loc[keep]
    use_ant_only = use_ant_only.loc[keep]
    use_mean = use_mean.loc[keep]
    use_tpwt_only = use_tpwt_only.loc[keep]

    out["phv"] = np.nan
    out.loc[use_ant_only, "phv"] = out.loc[use_ant_only, "phv_ant"]
    out.loc[use_tpwt_only, "phv"] = out.loc[use_tpwt_only, "phv_tpwt"]

    mean_values = out.loc[use_mean, ["phv_ant", "phv_tpwt"]].mean(axis=1)
    out.loc[use_mean, "phv"] = mean_values

    out["std"] = out["std_tpwt"]
    out["source"] = "UNKNOWN"
    out.loc[use_ant_only, "source"] = "ANT"
    out.loc[use_tpwt_only, "source"] = "TPWT"
    out.loc[use_mean & has_ant & has_tpwt, "source"] = "ANT_TPWT_MEAN"
    out.loc[use_mean & has_ant & ~has_tpwt, "source"] = "ANT_ONLY"
    out.loc[use_mean & ~has_ant & has_tpwt, "source"] = "TPWT_ONLY"

    return _sort_phase_dataframe(out)


def generate_nz_phase_dispersion(
    ant_dir: str | Path = "data/ant-dispersions",
    tpwt_file: str | Path = "data/mcmc/tpwt_phv-snr10-sm100.csv",
    out_file: str | Path | None = "data/mcmc/phase_dispersion_nz.csv",
    merge_periods: Sequence[float] = DEFAULT_NZ_MERGE_PERIODS,
) -> pd.DataFrame:
    """Generate phase_dispersion.csv from NZ ANT curves and TPWT CSV data."""

    ant = _read_nz_ant_dispersion(ant_dir)
    tpwt = _read_tpwt_csv(tpwt_file)
    phase = _merge_nz_ant_tpwt(ant, tpwt, merge_periods=merge_periods)
    return _write_phase_csv(phase, out_file)


# =============================================================================
# Optional command-line interface
# =============================================================================


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate merged phase-dispersion CSV files for MCMC inversion."
    )
    parser.add_argument(
        "dataset",
        choices=["lyb", "nz", "both"],
        nargs="?",
        default="nz",
        help="Dataset to process. Default: nz.",
    )
    parser.add_argument(
        "--lyb-base-dir",
        default="data/lyb_data",
        help="Base directory containing Lyb ant_grids and tpwt_grids.",
    )
    parser.add_argument(
        "--lyb-out",
        default="data/mcmc/phase_dispersion_lyb.csv",
        help="Output CSV path for Lyb phase dispersion.",
    )
    parser.add_argument(
        "--nz-ant-dir",
        default="data/ant-dispersions",
        help="Directory containing NZ ANT *_dispersion.dat files.",
    )
    parser.add_argument(
        "--nz-tpwt-file",
        default="data/mcmc/tpwt_phv-snr10-sm100.csv",
        help="NZ TPWT CSV file.",
    )
    parser.add_argument(
        "--nz-out",
        default="data/mcmc/phase_dispersion_nz.csv",
        help="Output CSV path for NZ phase dispersion.",
    )
    args = parser.parse_args()

    if args.dataset in {"lyb", "both"}:
        generate_lyb_phase_dispersion(
            base_dir=args.lyb_base_dir,
            out_file=args.lyb_out,
        )

    if args.dataset in {"nz", "both"}:
        generate_nz_phase_dispersion(
            ant_dir=args.nz_ant_dir,
            tpwt_file=args.nz_tpwt_file,
            out_file=args.nz_out,
        )


if __name__ == "__main__":
    main()
