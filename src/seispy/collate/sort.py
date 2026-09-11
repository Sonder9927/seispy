import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import batched
from pathlib import Path

from tqdm import tqdm


def sort_to(src: str | Path, dest: str | Path, pattern: str = "*.SAC"):
    """Copy SAC files into a station/year/Julian-day directory tree.

    Files produced by :func:`seispy.collate.mseed2sac` are copied into a
    ``network/station/year/day`` hierarchy.

    Args:
        src: Source directory searched recursively.
        dest: Destination root for the sorted copy.
        pattern: Glob pattern used to select source files.

    Examples:
        ```python
        sort_to("data/raw", "data/sorted", pattern="*.sac")
        ```
    """
    src_path = Path(src)
    dest_path = Path(dest)
    targets = list(src_path.rglob(pattern))
    batch_size = 2000
    with ProcessPoolExecutor(max_workers=5) as executor:
        futures = {
            executor.submit(_copy_targets, batch, dest_path)
            for batch in batched(targets, batch_size)
        }
        for future in tqdm(as_completed(futures), total=len(futures)):
            future.result()
    print(f"All done. Sorted `{src}` to `{dest}`.")


def _copy_targets(targets: list[Path], dest_path: Path):
    for target in targets:
        # change these parts to parse filename.
        net, sta, _, _, _, year, day, _ = target.stem.split(".")
        dest_file = dest_path / net / sta / year / day / target.name
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(target, dest_file)
    time.sleep(0.1)
