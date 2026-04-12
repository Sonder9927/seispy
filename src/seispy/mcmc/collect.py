from pathlib import Path
import pandas as pd
import shutil


def collect_prob_figs(grids_path: Path, out_path: Path) -> None:
    """Copy all probalCr.png files to output directory"""
    out_path.mkdir(parents=True, exist_ok=True)
    for ffig in grids_path.rglob("probalCr.png"):
        shutil.copy(ffig, out_path / f"prob_fig_{ffig.parent.name}.png")


def collect_prob_mean(grids_path: Path, vs_path: Path, mml_path: Path) -> None:
    """Extract vs profiles, misfit, moho, and lab depth"""
    vs_list, mml_list = [], []

    for grid_path in grids_path.glob("*0_*0"):
        lon, lat = map(float, grid_path.name.split("_"))

        # Read vs profile
        df = pd.read_csv(
            grid_path / "mean_prob.lst",
            sep=r"\s+",
            usecols=[0, 1],
            header=None,
            names=["z", "vs"],
        )
        df["z"] = -df["z"]
        df["x"], df["y"] = lon, lat
        vs_list.append(df)

        # Find lab: depth of maximum vs decrease below 50km
        mask = df["z"] <= -50
        lab = df.loc[df[mask]["vs"].diff().idxmin(), "z"] if mask.any() else -999

        # Extract misfit from last line, 9th field
        with open(grid_path / "Litmod_output.log") as f:
            misfit = float(f.readlines()[-1].split()[7])

        # Average moho depth
        moho = (
            pd.read_csv(grid_path / "moho.lst", sep=r"\s+", header=None)
            .iloc[:, 0]
            .mean()
        )

        mml_list.append([lon, lat, misfit, moho, lab])

    pd.concat(vs_list, ignore_index=True).to_csv(vs_path, index=False)
    pd.DataFrame(mml_list, columns=["x", "y", "misfit", "moho", "lab"]).to_csv(
        mml_path, index=False
    )


def collect_results(grids_dir: str | Path, out_dir: str | Path) -> None:
    """Main function: collect all MCMC results"""
    grids_path, out_path = Path(grids_dir), Path(out_dir)
    collect_prob_figs(grids_path, out_path / "mcmc_prob_figs")
    collect_prob_mean(grids_path, out_path / "vs.csv", out_path / "misfit_moho_lab.csv")
