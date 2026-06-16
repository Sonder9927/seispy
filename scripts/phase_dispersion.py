import re
from pathlib import Path

import numpy as np
import pandas as pd

# df = pd.read_csv("data/mcmc/vs_rjmcmc.dat", sep=r"\s+", header=None, names=["x", "y","z","vs"])
# print(df.head())
# df.to_csv("data/mcmc/vs_rjmcmc.csv", index=False)

ANT_DIR = Path("data/ant-dispersions")
TPWT_FILE = Path("data/mcmc/tpwt_phv-snr10-sm100.csv")
OUT_FILE = Path("data/mcmc/phase_dispersion.csv")

DEFAULT_STD = 20.0

MERGE_PERIODS = {20.0, 25.0, 30.0, 35.0}


def parse_ant_filename(path: Path):
    m = re.match(
        r"([+-]?\d+(?:\.\d+)?)_([+-]?\d+(?:\.\d+)?)_dispersion\.dat",
        path.name,
    )
    if m is None:
        raise ValueError(f"Cannot parse lon/lat from filename: {path.name}")

    return float(m.group(1)), float(m.group(2))


def read_ant_dispersion(ant_dir: Path) -> pd.DataFrame:
    rows = []

    for file in sorted(ant_dir.glob("*_dispersion.dat")):
        lon, lat = parse_ant_filename(file)

        df = pd.read_csv(
            file, sep=r"\s+", header=None, names=["period", "phv"], usecols=[0, 1]
        )

        df["longitude"] = lon
        df["latitude"] = lat
        rows.append(df)

    if not rows:
        raise FileNotFoundError(f"No ANT dispersion files found in {ant_dir}")

    ant = pd.concat(rows, ignore_index=True)
    ant = ant[["period", "longitude", "latitude", "phv"]].copy()

    return ant.astype(
        {
            "period": float,
            "longitude": float,
            "latitude": float,
            "phv": float,
        }
    )


def read_tpwt_dispersion(tpwt_file: Path) -> pd.DataFrame:
    tpwt = pd.read_csv(tpwt_file)

    required = {"period", "longitude", "latitude", "phv", "std"}
    missing = required - set(tpwt.columns)
    if missing:
        raise ValueError(f"TPWT file missing columns: {sorted(missing)}")

    tpwt = tpwt[["period", "longitude", "latitude", "phv", "std"]].copy()

    return tpwt.astype(
        {
            "period": float,
            "longitude": float,
            "latitude": float,
            "phv": float,
            "std": float,
        }
    )


def merge_phase_dispersion(ant: pd.DataFrame, tpwt: pd.DataFrame) -> pd.DataFrame:
    ant = ant.rename(columns={"phv": "phv_ant"})
    tpwt = tpwt.rename(columns={"phv": "phv_tpwt", "std": "std_tpwt"})

    merged = pd.merge(
        ant,
        tpwt,
        on=["longitude", "latitude", "period"],
        how="outer",
    )

    rows = []

    for _, row in merged.iterrows():
        period = float(row["period"])
        lon = float(row["longitude"])
        lat = float(row["latitude"])

        phv_ant = row.get("phv_ant", np.nan)
        phv_tpwt = row.get("phv_tpwt", np.nan)
        std_tpwt = row.get("std_tpwt", np.nan)

        if period < 20.0:
            if not np.isfinite(phv_ant):
                continue

            phv = phv_ant
            std = np.nan
            source = "ANT"

        elif period in MERGE_PERIODS:
            values = [v for v in (phv_ant, phv_tpwt) if np.isfinite(v)]
            if not values:
                continue

            phv = float(np.mean(values))
            std = std_tpwt if np.isfinite(std_tpwt) else np.nan

            if np.isfinite(phv_ant) and np.isfinite(phv_tpwt):
                source = "ANT_TPWT_MEAN"
            elif np.isfinite(phv_ant):
                source = "ANT_ONLY"
            else:
                source = "TPWT_ONLY"

        elif period > 35.0:
            if not np.isfinite(phv_tpwt):
                continue

            phv = phv_tpwt
            std = std_tpwt if np.isfinite(std_tpwt) else np.nan
            source = "TPWT"

        else:
            # 这里会自动忽略 ANT 中的 22、24、38 等不符合规则的周期
            continue

        rows.append(
            {
                "period": period,
                "longitude": lon,
                "latitude": lat,
                "phv": phv,
                "std": std,
                "source": source,
            }
        )

    out = pd.DataFrame(rows)
    out = out.sort_values(["longitude", "latitude", "period"]).reset_index(drop=True)

    return out


def main():
    ant = read_ant_dispersion(ANT_DIR)
    tpwt = read_tpwt_dispersion(TPWT_FILE)

    phase = merge_phase_dispersion(ant, tpwt)

    # phase["std"] = phase["std"].fillna(DEFAULT_STD)

    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    phase.to_csv(OUT_FILE, index=False)

    print(f"Saved: {OUT_FILE}")
    print(f"Rows: {len(phase)}")
    print(phase["source"].value_counts())


if __name__ == "__main__":
    main()
