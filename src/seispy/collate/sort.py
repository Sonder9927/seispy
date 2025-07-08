import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

from icecream import ic
from rose import batch_generator, pather
from tqdm import tqdm


def sort_to(src: str | Path, dest: str | Path, pattern: str = "*.SAC"):
    """copy source and sort structure of destnation

    Sort files from `mseed2sac` to the structure like `/net/sta/year/day`.
    Target file named like `net.sta..channel.D.year.day.hourminsec.SAC`.

    Parameters:
        src_dir: source directory
        dest_dir: destination directory
        pattern: search pattern of target files

    Examples:
        >>>import seispy
        >>>seispy.sort_to('/path/src', '/path/dest', '*.SAC')
    """
    src_path = Path(src)
    dest_path = Path(dest)
    targets = pather.glob(src_path, "rglob", [pattern])
    batch_size = 2000
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_copy_targets, batch, dest_path)
            for batch in batch_generator(targets, batch_size)
        }
        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()
    ic(f"All done. Sorted `{src}` to `{dest}`.")


def _copy_targets(targets: list[Path], dest_path: Path):
    for target in targets:
        # change these parts to parse filename.
        net, sta, _, _, _, year, day, _ = target.stem.split(".")
        dest_file = dest_path / net / sta / year / day / target.name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(target, dest_file)
    time.sleep(0.1)
