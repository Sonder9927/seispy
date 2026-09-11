"""Path discovery shared by filesystem workflows."""

from pathlib import Path


def bundled_command(command, bin_path: str = "bin") -> Path:
    """Return the path to a bundled command."""
    path = Path(bin_path) / command
    if not path.exists():
        raise FileNotFoundError(f"{command=} not found in {bin_path}.")
    return path


def find_leaf_directories(path: Path) -> list[Path]:
    """Return leaf directories beneath path."""
    if not path.is_dir():
        return []
    subdirectories = [child for child in path.iterdir() if child.is_dir()]
    if not subdirectories:
        return [path]
    return [leaf for child in subdirectories for leaf in find_leaf_directories(child)]
