"""Organize waveform files into canonical archive paths."""

import shutil
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from itertools import batched
from pathlib import Path

import obspy
from tqdm import tqdm

from seispy.archive import WaveformIdentity


def sort_waveforms(src: str | Path, dest: str | Path, pattern: str = "*.SAC"):
    """Copy SAC files into a network/station/year directory tree.

    Each destination is derived from the SAC header, never the source filename.

    Args:
        src: Source directory searched recursively.
        dest: Destination root for the sorted copy.
        pattern: Glob pattern used to select source files.

    Examples:
        ```python
        sort_waveforms("data/raw", "data/sorted", pattern="*.sac")
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
        stream = obspy.read(target, headonly=True)
        if len(stream) != 1:
            raise ValueError(f"SAC file must contain exactly one trace: {target}")
        identity = WaveformIdentity.from_trace(stream[0])
        dest_file = identity.sac_path(dest_path)
        if dest_file.exists():
            raise FileExistsError(dest_file)
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(target, dest_file)
    time.sleep(0.1)
