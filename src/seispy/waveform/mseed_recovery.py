"""Recover trustworthy records from a partially corrupt miniSEED file."""

from __future__ import annotations

import io
import fnmatch
import warnings
from dataclasses import dataclass
from pathlib import Path

import numpy as np
from obspy import read
from obspy.io.mseed import InternalMSEEDWarning
from obspy.io.mseed.util import get_record_information


@dataclass(frozen=True)
class MSeedRecovery:
    """Summary of a record-level miniSEED recovery."""

    total_records: int
    valid_records: int
    discarded_records: int


def filter_valid_mseed_records(
    source: str | Path,
    destination: str | Path,
    *,
    network: str,
    station: str,
    location: str,
    channel: str,
    sample_rate: float | None = None,
) -> MSeedRecovery:
    """Copy independently verified miniSEED records, dropping corrupt records.

    Record framing must remain readable. Each record is decompressed separately
    with miniSEED integrity warnings promoted to errors. Valid records are copied
    byte-for-byte; no interpolation, merging, or recompression is performed.
    """
    source_path = Path(source)
    destination_path = Path(destination)
    filesize = source_path.stat().st_size
    offset = 0
    total = 0
    valid = 0
    destination_path.parent.mkdir(parents=True, exist_ok=True)
    with source_path.open("rb") as reader, destination_path.open("wb") as writer:
        while offset < filesize:
            try:
                reader.seek(0)
                info = get_record_information(reader, offset=offset)
                record_length = int(info["record_length"])
            except Exception as exc:
                raise ValueError(
                    f"cannot parse miniSEED record at byte offset {offset}"
                ) from exc
            if record_length <= 0 or offset + record_length > filesize:
                raise ValueError(
                    f"invalid miniSEED record length {record_length} at byte "
                    f"offset {offset}"
                )
            reader.seek(offset)
            record = reader.read(record_length)
            total += 1
            if _record_is_valid(
                record,
                network=network,
                station=station,
                location=location,
                channel=channel,
                sample_rate=sample_rate,
            ):
                writer.write(record)
                valid += 1
            offset += record_length
    if not valid:
        destination_path.unlink(missing_ok=True)
        raise ValueError("all miniSEED records failed integrity validation")
    return MSeedRecovery(total, valid, total - valid)


def _record_is_valid(
    record: bytes,
    *,
    network: str,
    station: str,
    location: str,
    channel: str,
    sample_rate: float | None,
) -> bool:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("error", InternalMSEEDWarning)
            stream = read(io.BytesIO(record), format="MSEED")
    except Exception:
        return False
    if not stream:
        return False
    for trace in stream:
        stats = trace.stats
        actual_location = getattr(stats, "location", "") or ""
        if not all(
            fnmatch.fnmatchcase(actual, expected)
            for actual, expected in (
                (stats.network, network),
                (stats.station, station),
                (actual_location, location),
                (stats.channel, channel),
            )
        ):
            return False
        if sample_rate is not None and not np.isclose(
            float(stats.sampling_rate), sample_rate, rtol=1e-7, atol=1e-9
        ):
            return False
    return True
