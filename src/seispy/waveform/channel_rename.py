"""Rename SAC channels in place without re-encoding waveform samples."""

import csv
import os
import re
import shutil
from collections import Counter
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import obspy
from obspy.io.sac import header as sac_header

from seispy.workflow import commit_output, temporary_output_path

# Binary SAC: 70 float32 fields, 40 int32 fields, then 24 eight-byte strings.
_CHANNEL_OFFSET = 4 * (
    len(sac_header.FLOATHDRS) + len(sac_header.INTHDRS)
) + 8 * sac_header.STRHDRS.index("kcmpnm")


@dataclass(frozen=True)
class ChannelRenameIssue:
    """One source file that could not be renamed completely."""

    source: Path
    error: str


@dataclass(frozen=True)
class ChannelRenameSummary:
    """File counts; deletion failures are included in failed."""

    total: int
    renamed: int
    skipped: int
    failed: int
    deletion_failed: int
    issue_samples: tuple[ChannelRenameIssue, ...]
    errors_csv: Path | None

    @property
    def has_issues(self) -> bool:
        return self.failed > 0


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _read_header(path):
    # ObsPy's strict size check assumes v6 and rejects valid v7 footers.
    stream = obspy.read(str(path), format="SAC", headonly=True, fsize=False)
    if len(stream) != 1:
        raise ValueError("Expected one trace per binary SAC file")
    trace = stream[0]
    if trace.stats.sac.get("iftype") != 1 or not trace.stats.sac.get("leven"):
        raise ValueError("Only evenly sampled SAC time series are supported")
    footer_size = 176 if trace.stats.sac.nvhdr == 7 else 0
    if path.stat().st_size != 632 + trace.stats.npts * 4 + footer_size:
        raise ValueError("SAC file size does not match header sample count/version")
    return trace


def _verify_bytes(source, staged, code):
    """Require exactly the expected string substitution; compare in chunks."""
    with source.open("rb") as original, staged.open("rb") as modified:
        prefix = original.read(_CHANNEL_OFFSET)
        if len(prefix) != _CHANNEL_OFFSET or modified.read(_CHANNEL_OFFSET) != prefix:
            raise ValueError("SAC bytes before kcmpnm changed")
        original.read(8)
        if modified.read(8) != code.encode("ascii").ljust(8, b" "):
            raise ValueError("SAC kcmpnm validation failed")
        while True:
            chunk = original.read(1024 * 1024)
            if modified.read(len(chunk) or 1) != chunk:
                raise ValueError("SAC bytes after kcmpnm changed")
            if not chunk:
                break


def _rename_one(task):
    source, destination, code, error = task
    temporary = None
    committed = deleting = False
    try:
        if error is not None:
            raise ValueError(error)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"Target already exists: {destination}")
        temporary = temporary_output_path(destination)
        shutil.copyfile(source, temporary)
        with temporary.open("r+b") as handle:
            handle.seek(_CHANNEL_OFFSET)
            handle.write(code.encode("ascii").ljust(8, b" "))
            handle.flush()
            os.fsync(handle.fileno())
        _verify_bytes(source, temporary, code)
        check = _read_header(temporary)
        if check.stats.channel != code:
            raise ValueError("Renamed SAC channel validation failed")
        commit_output(temporary, destination, overwrite=False)
        committed = True
        _sync_directory(source.parent)
        deleting = True
        source.unlink()
        _sync_directory(source.parent)
        return None, False
    except Exception as exc:
        if committed and not deleting:
            destination.unlink(missing_ok=True)
        detail = f"{type(exc).__name__}: {exc}"
        if deleting:
            detail += (
                "; renamed output retained; original deletion/directory sync incomplete"
            )
        return ChannelRenameIssue(source, detail), deleting
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def rename_channels(
    src: str | Path,
    mapping: Mapping[str, str],
    *,
    pattern: str = "*.sac",
    max_workers: int = 5,
    errors_csv: str | Path | None = None,
) -> ChannelRenameSummary:
    """Rename binary SAC channels and filename tokens in place.

    Recursively search src using a basename glob. Match mapping keys against
    header channel names exactly (three uppercase ASCII letters/digits). Read
    only headers for discovery; unmapped and identity mappings are skipped.
    Apply the mapping once, never recursively. Filenames must have exactly
    one dot-delimited token equal to the original channel.

    A staged byte copy changes only the eight-byte kcmpnm field. All other
    bytes, including waveform data, direction headers and SAC v7 footers,
    remain identical. No rotation, resampling or Inventory modification occurs.
    Both SAC endiannesses are supported for evenly sampled time series;
    SAC text files are not supported.

    Existing destinations (including other source files) and multiple sources
    mapping to one destination fail without replacing any files. Mapping swaps
    and occupied chains are therefore unsupported. Verify and sync outputs
    before removing originals. A crash or deletion failure may leave both
    names present; completed outputs are retained and errors reported. Do not
    run concurrently with other writers in src.

    Print a summary and up to five errors. Optional errors_csv must be outside
    src; it is replaced with all failures in source,error columns.
    """
    source = Path(src).expanduser().resolve()
    if not source.is_dir():
        raise NotADirectoryError(source)
    if not isinstance(mapping, Mapping) or not mapping:
        raise ValueError("mapping must be a non-empty channel mapping")
    mapping = dict(mapping)
    if any(
        not isinstance(code, str) or re.fullmatch(r"[A-Z0-9]{3}", code) is None
        for item in mapping.items()
        for code in item
    ):
        raise ValueError(
            "Channel codes must contain three uppercase ASCII letters/digits"
        )
    if (
        isinstance(max_workers, bool)
        or not isinstance(max_workers, int)
        or max_workers < 1
    ):
        raise ValueError("max_workers must be a positive integer")
    if not pattern or "/" in pattern or "\\" in pattern:
        raise ValueError("pattern must be a non-empty basename glob")
    report = Path(errors_csv).expanduser().resolve() if errors_csv is not None else None
    if report is not None and report.is_relative_to(source):
        raise ValueError("errors_csv must be outside src")
    tasks, issues = [], []
    total = skipped = 0
    for path in sorted(source.rglob(pattern)):
        if not path.is_file() and not path.is_symlink():
            continue
        total += 1
        try:
            if path.is_symlink() or path.resolve() != path.absolute():
                raise ValueError("Symlinked inputs are not supported")
            old = _read_header(path).stats.channel
            code = mapping.get(old, old)
            if code == old:
                skipped += 1
                continue
            tokens = path.name.split(".")
            if tokens.count(old) != 1:
                raise ValueError(
                    f"Filename must contain exactly one {old} channel token"
                )
            tokens[tokens.index(old)] = code
            destination = path.with_name(".".join(tokens))
            tasks.append((path, destination, code))
        except Exception as exc:
            issues.append(ChannelRenameIssue(path, f"{type(exc).__name__}: {exc}"))
    counts = Counter(destination for _, destination, _ in tasks)
    jobs = [
        (
            path,
            dest,
            code,
            "Multiple sources map to the same target"
            if counts[dest] > 1
            else "Target already exists"
            if dest.exists() or dest.is_symlink()
            else None,
        )
        for path, dest, code in tasks
    ]
    deletion_failed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        for issue, deletion in pool.map(_rename_one, jobs):
            if issue is not None:
                issues.append(issue)
                deletion_failed += int(deletion)
    if report is not None:
        temp = temporary_output_path(report)
        try:
            with temp.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.writer(handle)
                writer.writerow(("source", "error"))
                writer.writerows((issue.source, issue.error) for issue in issues)
            commit_output(temp, report, overwrite=True)
        finally:
            temp.unlink(missing_ok=True)
    failed = len(issues)
    summary = ChannelRenameSummary(
        total,
        total - skipped - failed,
        skipped,
        failed,
        deletion_failed,
        tuple(issues[:5]),
        report,
    )
    print(
        f"Channel rename: {summary.total} files, {summary.renamed} renamed, {summary.skipped} skipped, {summary.failed} failed, {summary.deletion_failed} deletion failures"
    )
    for issue in summary.issue_samples:
        print(f"{issue.source}: {issue.error}")
    return summary
