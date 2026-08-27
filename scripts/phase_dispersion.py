"""Build phase-dispersion tables for MCMC inversion.

This module provides three public workflows for different input formats:

1. ``generate_phase_dispersion_from_grids``
   ANT and TPWT are stored as gridded ``*.grid`` files.

2. ``generate_phase_dispersion_from_ant_curves``
   ANT is stored as point-wise ``lon_lat_dispersion.dat`` curves, while TPWT
   is stored in a CSV file.

3. ``generate_phase_dispersion_from_csvs``
   ANT and TPWT are both stored in CSV files.

All public functions return a DataFrame with columns:
``period, longitude, latitude, phv, std, source``.

Missing uncertainty values are preserved as NaN. Any default uncertainty
required by the inversion should be assigned later when writing phase.input.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Sequence

import numpy as np
import pandas as pd
import pygmt

PHASE_COLUMNS = ["period", "longitude", "latitude", "phv", "std", "source"]
REQUIRED_PHV_COLUMNS = ["period", "longitude", "latitude", "phv"]
DEFAULT_MERGE_PERIODS = (20.0, 25.0, 30.0, 35.0)


def _read_phase_csv(path: str | Path) -> pd.DataFrame:
    """Read a phase-velocity CSV and normalize its columns.

    The CSV must contain at least:
    period, longitude, latitude, phv

    If ``std`` is absent, it is added and filled with NaN.
    """
    path = Path(path)
    df = pd.read_csv(path)

    missing = set(REQUIRED_PHV_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(f"{path} missing columns: {sorted(missing)}")

    if "std" not in df.columns:
        df["std"] = np.nan

    columns = REQUIRED_PHV_COLUMNS + ["std"]
    df = df[columns].copy()

    for column in columns:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    return df.dropna(subset=REQUIRED_PHV_COLUMNS).reset_index(drop=True)


def _merge_ant_tpwt_by_period(
    ant: pd.DataFrame,
    tpwt: pd.DataFrame,
    merge_periods: Sequence[float] = DEFAULT_MERGE_PERIODS,
) -> pd.DataFrame:
    """Merge ANT and TPWT data using the project period-selection rule.

    Rules
    -----
    - period < 20 s:
        use ANT only.
    - periods listed in ``merge_periods``:
        average ANT and TPWT if both exist; otherwise use whichever exists.
    - period > 35 s:
        use TPWT only.
    - other periods between 20 and 35 s:
        ignore.

    TPWT uncertainty is retained whenever available.
    """
    ant = ant[REQUIRED_PHV_COLUMNS].copy()

    if "std" not in tpwt.columns:
        tpwt = tpwt.copy()
        tpwt["std"] = np.nan

    tpwt = tpwt[REQUIRED_PHV_COLUMNS + ["std"]].copy()

    merged = ant.rename(columns={"phv": "phv_ant"}).merge(
        tpwt.rename(columns={"phv": "phv_tpwt", "std": "std_tpwt"}),
        on=["period", "longitude", "latitude"],
        how="outer",
    )

    has_ant = merged["phv_ant"].notna()
    has_tpwt = merged["phv_tpwt"].notna()

    period_values = merged["period"].to_numpy(dtype=float)
    is_merge_period = np.zeros(len(merged), dtype=bool)
    for period in merge_periods:
        is_merge_period |= np.isclose(
            period_values,
            float(period),
            atol=1e-6,
            rtol=0.0,
        )

    is_merge_period = pd.Series(is_merge_period, index=merged.index)

    use_ant = (merged["period"] < 20.0) & has_ant
    use_merge = is_merge_period & (has_ant | has_tpwt)
    use_tpwt = (merged["period"] > 35.0) & has_tpwt

    keep = use_ant | use_merge | use_tpwt
    result = merged.loc[keep].copy()

    has_ant = has_ant.loc[result.index]
    has_tpwt = has_tpwt.loc[result.index]
    use_ant = use_ant.loc[result.index]
    use_merge = use_merge.loc[result.index]
    use_tpwt = use_tpwt.loc[result.index]

    result["phv"] = np.nan
    result.loc[use_ant, "phv"] = result.loc[use_ant, "phv_ant"]
    result.loc[use_tpwt, "phv"] = result.loc[use_tpwt, "phv_tpwt"]
    result.loc[use_merge, "phv"] = result.loc[use_merge, ["phv_ant", "phv_tpwt"]].mean(
        axis=1
    )

    result["std"] = result["std_tpwt"]

    result["source"] = "UNKNOWN"
    result.loc[use_ant, "source"] = "ANT"
    result.loc[use_tpwt, "source"] = "TPWT"
    result.loc[use_merge & has_ant & has_tpwt, "source"] = "ANT_TPWT_MEAN"
    result.loc[use_merge & has_ant & ~has_tpwt, "source"] = "ANT_ONLY"
    result.loc[use_merge & ~has_ant & has_tpwt, "source"] = "TPWT_ONLY"

    return (
        result[PHASE_COLUMNS]
        .dropna(subset=["phv"])
        .sort_values(["longitude", "latitude", "period"])
        .reset_index(drop=True)
    )


def _save_phase_dispersion(
    df: pd.DataFrame,
    out_file: str | Path | None,
) -> pd.DataFrame:
    """Optionally save phase-dispersion data and return the DataFrame."""
    if out_file is not None:
        out_file = Path(out_file)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out_file, index=False)

    return df


def generate_phase_dispersion_from_grids(
    ant_dir: str | Path,
    tpwt_dir: str | Path,
    out_file: str | Path | None = None,
) -> pd.DataFrame:
    """Generate phase dispersion from gridded ANT and TPWT files.

    Expected files
    --------------
    ANT:
        ``ant_vel_<period>.grid``

    TPWT velocity:
        ``tpwt_vel_<period>.grid``

    TPWT uncertainty:
        ``tpwt_std_<period>.grid``

    Each grid contains three whitespace-separated columns:
    longitude, latitude, value.

    Merge rule
    ----------
    - ANT + TPWT at the same period/location:
        average phase velocity and retain TPWT std.
    - ANT only:
        retain ANT phase velocity and set std to NaN.
    - TPWT only:
        retain TPWT phase velocity and TPWT std.
    """
    ant_dir = Path(ant_dir)
    tpwt_dir = Path(tpwt_dir)

    ant_frames: list[pd.DataFrame] = []

    for file in sorted(ant_dir.glob("ant_vel_*.grid")):
        match = re.fullmatch(r"ant_vel_(\d+(?:\.\d+)?)\.grid", file.name)
        if match is None:
            continue

        period = float(match.group(1))

        df = pd.read_csv(
            file,
            sep=r"\s+",
            header=None,
            names=["longitude", "latitude", "phv"],
            usecols=[0, 1, 2],
        )

        for column in ["longitude", "latitude", "phv"]:
            df[column] = pd.to_numeric(df[column], errors="coerce")

        df = df.dropna(subset=["longitude", "latitude", "phv"])
        df["period"] = period
        ant_frames.append(df[REQUIRED_PHV_COLUMNS])

    if not ant_frames:
        raise FileNotFoundError(f"No ANT grid files found in {ant_dir}")

    ant = pd.concat(ant_frames, ignore_index=True)

    tpwt_frames: list[pd.DataFrame] = []

    for vel_file in sorted(tpwt_dir.glob("tpwt_vel_*.grid")):
        match = re.fullmatch(r"tpwt_vel_(\d+(?:\.\d+)?)\.grid", vel_file.name)
        if match is None:
            continue

        period = float(match.group(1))

        vel = pd.read_csv(
            vel_file,
            sep=r"\s+",
            header=None,
            names=["longitude", "latitude", "phv"],
            usecols=[0, 1, 2],
        )

        for column in ["longitude", "latitude", "phv"]:
            vel[column] = pd.to_numeric(vel[column], errors="coerce")

        vel = vel.dropna(subset=["longitude", "latitude", "phv"])
        vel["period"] = period

        std_file = tpwt_dir / f"tpwt_std_{period:g}.grid"

        if std_file.exists():
            std = pd.read_csv(
                std_file,
                sep=r"\s+",
                header=None,
                names=["longitude", "latitude", "std"],
                usecols=[0, 1, 2],
            )

            for column in ["longitude", "latitude", "std"]:
                std[column] = pd.to_numeric(std[column], errors="coerce")

            std = std.dropna(subset=["longitude", "latitude"])
            std["period"] = period

            vel = vel.merge(
                std,
                on=["period", "longitude", "latitude"],
                how="left",
            )
        else:
            vel["std"] = np.nan

        tpwt_frames.append(vel[REQUIRED_PHV_COLUMNS + ["std"]])

    if not tpwt_frames:
        raise FileNotFoundError(f"No TPWT grid files found in {tpwt_dir}")

    tpwt = pd.concat(tpwt_frames, ignore_index=True)

    merged = ant.rename(columns={"phv": "phv_ant"}).merge(
        tpwt.rename(columns={"phv": "phv_tpwt", "std": "std_tpwt"}),
        on=["period", "longitude", "latitude"],
        how="outer",
    )

    has_ant = merged["phv_ant"].notna()
    has_tpwt = merged["phv_tpwt"].notna()

    merged["phv"] = np.nan
    merged.loc[has_ant & has_tpwt, "phv"] = merged.loc[
        has_ant & has_tpwt, ["phv_ant", "phv_tpwt"]
    ].mean(axis=1)
    merged.loc[has_ant & ~has_tpwt, "phv"] = merged.loc[has_ant & ~has_tpwt, "phv_ant"]
    merged.loc[~has_ant & has_tpwt, "phv"] = merged.loc[~has_ant & has_tpwt, "phv_tpwt"]

    merged["std"] = merged["std_tpwt"]

    merged["source"] = np.select(
        [
            has_ant & has_tpwt,
            has_ant & ~has_tpwt,
            ~has_ant & has_tpwt,
        ],
        ["ANT_TPWT_MEAN", "ANT", "TPWT"],
        default="UNKNOWN",
    )

    result = (
        merged[PHASE_COLUMNS]
        .dropna(subset=["phv"])
        .sort_values(["longitude", "latitude", "period"])
        .reset_index(drop=True)
    )

    return _save_phase_dispersion(result, out_file)


def generate_phase_dispersion_from_ant_curves(
    ant_dir: str | Path,
    tpwt_phv_csv: str | Path,
    out_file: str | Path | None = None,
    merge_periods: Sequence[float] = DEFAULT_MERGE_PERIODS,
) -> pd.DataFrame:
    """Generate phase dispersion from ANT curve files and a TPWT CSV.

    ANT files must be named:
        ``<longitude>_<latitude>_dispersion.dat``

    Each ANT file contains two whitespace-separated columns:
        period, phv

    The TPWT CSV must contain at least:
        period, longitude, latitude, phv

    `std` is optional.
    """
    ant_dir = Path(ant_dir)

    pattern = re.compile(r"^([+-]?\d+(?:\.\d+)?)_([+-]?\d+(?:\.\d+)?)_dispersion\.dat$")

    ant_frames: list[pd.DataFrame] = []

    for file in sorted(ant_dir.glob("*_dispersion.dat")):
        match = pattern.fullmatch(file.name)
        if match is None:
            continue

        longitude = float(match.group(1))
        latitude = float(match.group(2))

        df = pd.read_csv(
            file,
            sep=r"\s+",
            header=None,
            names=["period", "phv"],
            usecols=[0, 1],
        )

        df["period"] = pd.to_numeric(df["period"], errors="coerce")
        df["phv"] = pd.to_numeric(df["phv"], errors="coerce")
        df = df.dropna(subset=["period", "phv"])

        df["longitude"] = longitude
        df["latitude"] = latitude

        ant_frames.append(df[REQUIRED_PHV_COLUMNS])

    if not ant_frames:
        raise FileNotFoundError(f"No ANT dispersion files found in {ant_dir}")

    ant = pd.concat(ant_frames, ignore_index=True)
    tpwt = _read_phase_csv(tpwt_phv_csv)

    result = _merge_ant_tpwt_by_period(
        ant=ant,
        tpwt=tpwt,
        merge_periods=merge_periods,
    )

    return _save_phase_dispersion(result, out_file)


def generate_phase_dispersion_from_csvs(
    ant_phv_csv: str | Path,
    tpwt_phv_csv: str | Path,
    out_file: str | Path | None = None,
    merge_periods: Sequence[float] = DEFAULT_MERGE_PERIODS,
) -> pd.DataFrame:
    """Generate phase dispersion from ANT and TPWT CSV files.

    Both CSV files must contain at least:
        period, longitude, latitude, phv

    `std` is optional. Only TPWT `std` is used in the output.
    """
    ant = _read_phase_csv(ant_phv_csv)
    tpwt = _read_phase_csv(tpwt_phv_csv)

    result = _merge_ant_tpwt_by_period(
        ant=ant,
        tpwt=tpwt,
        merge_periods=merge_periods,
    )

    return _save_phase_dispersion(result, out_file)


def resample_ant_with_blockmean(
    ant_phv_csv: str | Path,
    out_file: str | Path,
    spacing: float = 0.5,
    region: list[float] | None = None,
) -> pd.DataFrame:
    """Resample ANT phase-velocity data using GMT blockmean.

    Parameters
    ----------
    ant_phv_csv
        Input CSV file containing at least:
        period, longitude, latitude, phv.
    out_file
        Output CSV file.
    spacing
        Target block spacing in degrees, e.g. 0.25.
    region
        GMT region as [west, east, south, north].
        If None, the region is inferred from the input data.

    Returns
    -------
    pandas.DataFrame
        Resampled phase-velocity data with columns:
        period, longitude, latitude, phv.
    """
    ant_phv_csv = Path(ant_phv_csv)
    out_file = Path(out_file)

    df = pd.read_csv(ant_phv_csv)

    required = {"period", "longitude", "latitude", "phv"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{ant_phv_csv} missing columns: {sorted(missing)}")

    df = df[["period", "longitude", "latitude", "phv"]].copy()

    for column in ["period", "longitude", "latitude", "phv"]:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=["period", "longitude", "latitude", "phv"])

    if region is None:
        region = [
            df["longitude"].min(),
            df["longitude"].max(),
            df["latitude"].min(),
            df["latitude"].max(),
        ]

    results = []

    for period, period_df in df.groupby("period", sort=True):
        block = pygmt.blockmean(
            x=period_df["longitude"],
            y=period_df["latitude"],
            z=period_df["phv"],
            region=region,
            spacing=spacing,
        )

        block.columns = ["longitude", "latitude", "phv"]
        block["period"] = period

        results.append(block)

    resampled = pd.concat(results, ignore_index=True)

    # Round grid coordinates to 2 decimal places
    resampled["longitude"] = resampled["longitude"].round(2)
    resampled["latitude"] = resampled["latitude"].round(2)

    resampled = (
        resampled[["period", "longitude", "latitude", "phv"]]
        .sort_values(["period", "longitude", "latitude"])
        .reset_index(drop=True)
    )

    out_file.parent.mkdir(parents=True, exist_ok=True)
    resampled.to_csv(out_file, index=False)

    return resampled


if __name__ == "__main__":
    # # lyb
    # generate_phase_dispersion_from_grids(
    #     "data/lyb_data/ant_grids",
    #     "data/lyb_data/tpwt_grids",
    #     "data/mcmc/phase_dispersion_lyb.csv",
    # )
    # # nz
    # generate_phase_dispersion_from_ant_curves(
    #     "data/ant-dispersions",
    #     "data/mcmc/tpwt_phv-snr10-sm100.csv",
    #     "data/mcmc/phase_dispersion_nz.csv",
    #     merge_periods=DEFAULT_MERGE_PERIODS,
    # )
    ant_csv_d25 = "data/mcmc/fmst_202608_phv-d25.csv"
    ant_csv_d5 = "data/mcmc/fmst_202608_phv-d5.csv"
    ant_resampled_df = resample_ant_with_blockmean(
        ant_phv_csv="data/mcmc/fmst_202608_phv.csv",
        out_file=ant_csv_d5,
        spacing=0.5,
        region=[171.5, 179.5, -42.5, -34],
    )
    ant_resampled_df = resample_ant_with_blockmean(
        ant_phv_csv="data/mcmc/fmst_202608_phv.csv",
        out_file=ant_csv_d25,
        spacing=0.25,
        region=[171.5, 179.5, -42.5, -34],
    )
    generate_phase_dispersion_from_csvs(
        ant_csv_d25,
        "data/mcmc/tpwt_phv-d25.260810.csv",
        "data/mcmc/phase_dispersion_2608-d25.csv",
        merge_periods=DEFAULT_MERGE_PERIODS,
    )
    generate_phase_dispersion_from_csvs(
        ant_csv_d5,
        "data/mcmc/tpwt_phv.260817.csv",
        "data/mcmc/phase_dispersion_2608-d5.csv",
        merge_periods=DEFAULT_MERGE_PERIODS,
    )
