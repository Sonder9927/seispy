from pathlib import Path


def binuse(command, bin_path: str = "bin") -> Path:
    """get bin command

    Parameters:
        command: target command
        bin_path: bin path

    Returns:
        bin command path
    """
    command_path = Path(bin_path) / command
    if command_path.exists():
        return command_path
    else:
        raise FileNotFoundError(f"{command=} not found in {bin_path}.")


def find_last_subdirs(path: Path) -> list[Path]:
    """Find the last subdirectory in a path."""
    if not path.is_dir():
        return []

    subdirs = [child for child in path.iterdir() if child.is_dir()]
    if not subdirs:
        return [path]

    last_subdirs = []
    for subdir in subdirs:
        last_subdirs.extend(find_last_subdirs(subdir))

    return last_subdirs


def glob(dir, method, patterns, exclude_parts=None, include_parts=None) -> list[Path]:
    """Glob files in a directory with given patterns and exclude patterns."""
    targets = []
    dir = Path(dir)
    if method == "glob":
        for pattern in patterns:
            targets += list(dir.glob(pattern))
    elif method == "rglob":
        for pattern in patterns:
            targets += list(dir.rglob(pattern))

    # filter out files
    if exclude_parts:
        for part in exclude_parts:
            targets = [target for target in targets if part not in target.parts]
    if include_parts:
        for part in include_parts:
            targets = [target for target in targets if part in target.parts]

    return targets


def copy_structure(src, dest):
    """Copy directory structure from src to dest."""
    src_path = Path(src)
    dest_path = Path(dest)

    dest_path.mkdir(parents=True, exist_ok=True)
    for item in src_path.glob("**/*"):
        if item.is_dir():
            target_dir = path_relative(src_path, item, dest_path)
            target_dir.mkdir(parents=True, exist_ok=True)


def path_relative(src, target: Path, dest: Path | None = None):
    """Get relative path of target from base."""
    relative_path = target.relative_to(src)
    if dest is None:
        return relative_path
    return dest / relative_path
