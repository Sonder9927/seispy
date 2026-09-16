from pathlib import Path

import pandas as pd
from obspy import UTCDateTime


def load_events(catalog: str | Path, time_window: float) -> list[dict]:
    """Load event metadata without discarding sub-second origin times."""
    frame = pd.read_csv(
        catalog,
        parse_dates=["time"],
        dtype={"latitude": float, "longitude": float, "depth": float, "mag": float},
    )
    events = []
    for _, row in frame.iterrows():
        start = UTCDateTime(row["time"].to_pydatetime())
        events.append(
            {
                "start": start,
                "end": start + time_window,
                "latitude": float(row["latitude"]),
                "longitude": float(row["longitude"]),
                "depth": float(row["depth"]),
                "mag": float(row["mag"]),
            }
        )
    return events


def load_stations(
    station_keys: set[tuple[str, str]], station_csv: str | Path | None
) -> list[dict]:
    """Load metadata for indexed network/station identities."""
    target_stations = set(station_keys)
    if not target_stations:
        return []
    if not station_csv:
        return [
            {"network": network, "station": station}
            for network, station in sorted(target_stations)
        ]
    frame = pd.read_csv(station_csv, dtype={"network": str, "station": str})
    if "station" not in frame.columns:
        raise ValueError("station_csv is missing column: station")
    networks = {network for network, _ in target_stations}
    if "network" not in frame.columns:
        if len(networks) != 1:
            raise ValueError(
                "station_csv must include network when source_dir contains "
                "multiple networks"
            )
        frame["network"] = next(iter(networks), "")
    available = set(
        zip(
            frame["network"].astype(str),
            frame["station"].astype(str),
            strict=True,
        )
    )
    duplicated = frame.duplicated(subset=["network", "station"], keep=False)
    if duplicated.any():
        duplicate_keys = set(
            zip(
                frame.loc[duplicated, "network"].astype(str),
                frame.loc[duplicated, "station"].astype(str),
                strict=True,
            )
        )
        duplicates = sorted(duplicate_keys)[:5]
        raise ValueError(
            f"station_csv contains duplicate network/station rows: {duplicates}"
        )
    missing = target_stations - available
    if missing:
        preview = sorted(missing)[:5]
        raise ValueError(
            f"{len(missing)} network/station identities missing from CSV: "
            f"{preview}{'...' if len(missing) > 5 else ''}"
        )
    wanted = frame.apply(
        lambda row: (str(row["network"]), str(row["station"])) in target_stations,
        axis=1,
    )
    return frame.loc[wanted].to_dict("records")


def filter_events(
    event_file: str | Path | None = None,
    event_files: list[str | Path] | None = None,
    output_file: str | Path | None = None,
    time_window: float = 0,
    required_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Combine event tables and retain temporally isolated events.

    Args:
        event_file: One input CSV file.
        event_files: Multiple input CSV files, used when ``event_file`` is absent.
        output_file: Optional destination for the filtered CSV.
        time_window: Minimum separation from adjacent events in seconds.
        required_columns: Columns to read; defaults to the SeisPy event schema.

    Returns:
        A time-sorted pandas data frame with normalized UTC timestamps.

    Raises:
        ValueError: If no input or no valid events are available.

    Examples:
        ```python
        events = filter_events(
            event_file="events.csv", output_file="events-filtered.csv",
            time_window=10_800,
        )
        ```
    """
    columns = required_columns or ["time", "longitude", "latitude", "depth", "mag"]
    if event_file is not None:
        frame = pd.read_csv(event_file, usecols=columns)
    elif event_files is not None:
        frame = pd.concat(
            [pd.read_csv(path, usecols=columns) for path in event_files],
            ignore_index=True,
        ).drop_duplicates(keep="first")
    else:
        raise ValueError("No event file provided")
    if frame.empty:
        raise ValueError("No events found")
    frame["time"] = pd.to_datetime(frame["time"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
    if frame.empty:
        raise ValueError("No valid events found after parsing time column")
    if time_window > 0:
        previous = frame["time"].diff().dt.total_seconds()
        following = frame["time"].diff(-1).abs().dt.total_seconds()
        frame = frame.loc[
            ~((previous < time_window) | (following < time_window))
        ].copy()
    frame = frame.reset_index(drop=True)
    frame["time"] = frame["time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:23] + "Z"
    if output_file is not None:
        frame.to_csv(output_file, index=False, encoding="utf-8")
    return frame


def write_event_catalog(frame: pd.DataFrame, output_file: str | Path) -> None:
    """Write event times for the external cutting tools.

    Args:
        frame: Data frame containing a UTC-compatible ``time`` column.
        output_file: Destination text file.

    Examples:
        ```python
        write_event_catalog(events, "events.cat")
        ```
    """
    times = pd.to_datetime(frame["time"], utc=True).dt.strftime("%Y/%m/%d,%H:%M:%S")
    Path(output_file).write_text(
        "".join(f"{value}\n" for value in times), encoding="utf-8"
    )
