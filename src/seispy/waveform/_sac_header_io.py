"""Edit binary SAC metadata without reading or serializing waveform samples."""

from dataclasses import dataclass
import os
from pathlib import Path
import shutil
import stat
import struct

from obspy.io.sac.header import FLOATHDRS, INTHDRS, STRHDRS

from seispy.workflow import commit_output, temporary_output_path

_FLOAT = {name: i * 4 for i, name in enumerate(FLOATHDRS)}
_INT = {name: 280 + i * 4 for i, name in enumerate(INTHDRS)}
_STRING = {name: 440 + i * 8 for i, name in enumerate(STRHDRS)}
# SAC v7 footer order, including values that must remain byte-for-byte intact.
_DOUBLE = {
    name: i * 8
    for i, name in enumerate(
        (
            "delta",
            "b",
            "e",
            "o",
            "a",
            *(f"t{i}" for i in range(10)),
            "f",
            "evlo",
            "evla",
            "stlo",
            "stla",
            "sb",
            "sdelta",
        )
    )
}


@dataclass(frozen=True)
class Header:
    """A small immutable snapshot of header/footer bytes and their byte order."""

    raw: bytes
    footer: bytes
    endian: str
    footer_offset: int

    def number(self, name):
        if self.footer and name in _DOUBLE:
            value = struct.unpack_from(self.endian + "d", self.footer, _DOUBLE[name])[0]
        else:
            value = struct.unpack_from(self.endian + "f", self.raw, _FLOAT[name])[0]
        return None if value == -12345 else value

    def text(self, name):
        offset = _STRING[name]
        value = self.raw[offset : offset + 8].decode("ascii").strip(" \x00")
        return "" if value == "-12345" else value

    def regions(self):
        return [(0, self.raw)] + (
            [(self.footer_offset, self.footer)] if self.footer else []
        )

    def updated_regions(self, values):
        raw, footer = bytearray(self.raw), bytearray(self.footer)
        for name, value in values.items():
            if name == "lcalda":
                struct.pack_into(self.endian + "i", raw, _INT[name], int(value))
            else:
                struct.pack_into(self.endian + "f", raw, _FLOAT[name], value)
                if footer and name in _DOUBLE:
                    struct.pack_into(self.endian + "d", footer, _DOUBLE[name], value)
        return [(0, bytes(raw))] + (
            [(self.footer_offset, bytes(footer))] if footer else []
        )


def _read_header(handle):
    handle.seek(0)
    raw = handle.read(632)
    if len(raw) != 632:
        raise ValueError("Incomplete binary SAC header")
    endian = next(
        (
            order
            for order in ("<", ">")
            if struct.unpack_from(order + "i", raw, _INT["nvhdr"])[0] in (6, 7)
        ),
        None,
    )
    if endian is None:
        raise ValueError("Only binary SAC v6/v7 files are supported")
    ints = struct.unpack_from(endian + "40i", raw, 280)
    if ints[INTHDRS.index("iftype")] != 1 or ints[INTHDRS.index("leven")] != 1:
        raise ValueError("Only evenly spaced SAC time series are supported")
    npts = ints[INTHDRS.index("npts")]
    if npts < 1:
        raise ValueError("SAC npts must be positive")
    offset = 632 + 4 * npts
    version = ints[INTHDRS.index("nvhdr")]
    footer_size = 176 if version == 7 else 0
    if os.fstat(handle.fileno()).st_size != offset + footer_size:
        raise ValueError("SAC size does not match npts and header version")
    footer = b""
    if footer_size:
        handle.seek(offset)
        footer = handle.read(footer_size)
        if len(footer) != footer_size:
            raise ValueError("Incomplete SAC v7 footer")
    return Header(raw, footer, endian, offset)


def _write_regions(handle, regions):
    for offset, data in regions:
        handle.seek(offset)
        if handle.write(data) != len(data):
            raise OSError("Incomplete SAC header write")
    handle.flush()


def _edit(path, updates, *, recover):
    with path.open("r+b") as handle:
        header = _read_header(handle)
        if not header.text("kstnm") or not header.text("kcmpnm"):
            raise ValueError("SAC station and channel headers must be defined")
        old = header.regions()
        new = header.updated_regions(updates(header))
        if new == old:
            return True
        try:
            _write_regions(handle, new)
        except BaseException:
            if recover:
                # Do not call the failed writer again: restore the saved bytes
                # directly, including the v7 footer, before propagating failure.
                try:
                    for offset, data in old:
                        handle.seek(offset)
                        if handle.write(data) != len(data):
                            raise OSError("Incomplete SAC header restoration")
                    handle.flush()
                except BaseException as exc:
                    raise OSError(
                        f"SAC header update and restoration failed: {path}"
                    ) from exc
            raise
    return False


def edit_headers(source, destination, updates, *, overwrite=False):
    """Copy then patch, or patch in place with best-effort exception rollback.

    No sample arrays are loaded. Rollback covers catchable failures, not power
    loss or uncatchable process termination. In-place edits require a regular,
    non-symlink file with one link to avoid mutating aliases concurrently.
    """
    source = Path(source)
    if destination is None:
        info = source.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError(
                "In-place SAC updates require a regular file without aliases"
            )
        return _edit(source, updates, recover=True)
    temporary = temporary_output_path(destination)
    try:
        shutil.copyfile(source, temporary)
        unchanged = _edit(temporary, updates, recover=False)
        commit_output(temporary, destination, overwrite=overwrite)
        return unchanged
    finally:
        temporary.unlink(missing_ok=True)
