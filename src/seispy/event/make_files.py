from pathlib import Path

import pandas as pd


def filter_events(
    evtf: str | Path | None = None,
    evtfs: list[str | Path] | None = None,
    outfile: str | Path | None = None,
    time_window: float = 0,
    required_columns: list[str] | None = None,
) -> pd.DataFrame:
    """
    Filter earthquake events by time separation.

    If more than one event occurs within `time_window` seconds, all events
    in that close-time group are removed. Only isolated events are kept.

    Parameters
    ----------
    evtf : str | Path | None
        Single event CSV file.
    evtfs : list[str | Path] | None
        Multiple event CSV files.
    outfile : str | Path | None
        Output CSV file.
    time_window : float
        Time window in seconds. Events closer than this window are removed.
    required_columns : list[str] | None
        Columns to read from event files.

    Returns
    -------
    pd.DataFrame
        Filtered event dataframe.

    Notes
    -----
    - The time column is parsed as UTC-aware datetime.
    - The depth column is assumed to be in kilometers.
    """

    if required_columns is None:
        required_columns = ["time", "longitude", "latitude", "depth", "mag"]

    if evtf is not None:
        evt_df = pd.read_csv(evtf, usecols=required_columns)

    elif evtfs is not None:
        evt_df = pd.concat(
            [pd.read_csv(ievtf, usecols=required_columns) for ievtf in evtfs],
            ignore_index=True,
        ).drop_duplicates(keep="first")

    else:
        raise ValueError("No event file provided.")

    if evt_df.empty:
        raise ValueError("No events found.")

    # Parse time column as UTC-aware datetime.
    evt_df["time"] = pd.to_datetime(
        evt_df["time"],
        utc=True,
        errors="coerce",
    )

    evt_df = evt_df.dropna(subset=["time"]).copy()

    if evt_df.empty:
        raise ValueError("No valid events found after parsing time column.")

    # Sort by origin time.
    evt_df = evt_df.sort_values("time").reset_index(drop=True)

    if time_window > 0:
        # Time difference to the previous and next event.
        dt_prev = evt_df["time"].diff().dt.total_seconds()
        dt_next = evt_df["time"].diff(-1).abs().dt.total_seconds()

        # Remove events that are too close to either neighbor.
        too_close = (dt_prev < time_window) | (dt_next < time_window)

        filtered_df = evt_df.loc[~too_close].copy()
    else:
        filtered_df = evt_df.copy()

    filtered_df = filtered_df.reset_index(drop=True)

    if outfile is not None:
        filtered_df.to_csv(outfile, index=False)
        print(f"Save {len(filtered_df)} events to {outfile}.")

    return filtered_df


def write_event_cat(df, outfile):
    time_str = pd.to_datetime(df["time"], utc=True).dt.strftime("%Y/%m/%d,%H:%M:%S")

    with open(outfile, "w", encoding="utf-8") as f:
        for t in time_str:
            f.write(t + "\n")


if __name__ == "__main__":
    filter_events(
        evtfs=["data/evt_25.csv", "data/evt_125.csv"],
        outfile="data/events.csv",
        time_window=10800,
    )
