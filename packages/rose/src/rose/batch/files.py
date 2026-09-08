import os
import tempfile
from collections.abc import Iterable
from pathlib import Path


def temporary_output_path(destination: str | Path) -> Path:
    """Reserve a unique temporary name beside its final destination."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, name = tempfile.mkstemp(
        prefix=f".{destination.stem}.",
        suffix=destination.suffix,
        dir=destination.parent,
    )
    os.close(descriptor)
    path = Path(name)
    path.unlink()
    return path


def validate_output(path: str | Path, description: str = "output") -> Path:
    path = Path(path)
    if not path.is_file() or path.stat().st_size == 0:
        raise OSError(f"{description} was not written")
    return path


def commit_output(
    temporary: str | Path, destination: str | Path, *, overwrite: bool = False
) -> Path:
    """Atomically commit a validated output, optionally replacing its target."""
    temporary = validate_output(temporary)
    destination = Path(destination)
    if overwrite:
        os.replace(temporary, destination)
    else:
        os.link(temporary, destination)
        temporary.unlink()
    return destination


def cleanup_outputs(paths: Iterable[str | Path]) -> None:
    for path in paths:
        Path(path).unlink(missing_ok=True)
