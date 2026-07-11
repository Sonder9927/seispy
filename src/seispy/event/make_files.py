from pathlib import Path

import pandas as pd


def filter_events(
    evtf: str | Path | None = None,
    evtfs: list[str | Path] | None = None,
    outfile: str | Path | None = None,
    time_window: float = 0,
    required_columns: list[str] | None = None,
):
    """
    Filter earthquake events by time separation.

    If more than one event occurs within `time_window` seconds, all events
    in that close-time group are removed. Only isolated events are kept.

    Notes
    -----
    - Input time is expected to be fixed UTC ISO format:
      YYYY-MM-DDTHH:MM:SS.sssZ
    - Output time keeps the same format.
    - depth is assumed to be in kilometers.
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

    # 统一解析为 UTC datetime
    evt_df["time"] = pd.to_datetime(
        evt_df["time"],
        utc=True,
        errors="coerce",
    )

    evt_df = evt_df.dropna(subset=["time"]).copy()

    if evt_df.empty:
        raise ValueError("No valid events found after parsing time column.")

    # 按时间排序
    evt_df = evt_df.sort_values("time").reset_index(drop=True)

    if time_window and time_window > 0:
        # 与前一个事件的时间差
        dt_prev = evt_df["time"].diff().dt.total_seconds()

        # 与后一个事件的时间差
        dt_next = evt_df["time"].diff(-1).abs().dt.total_seconds()

        # 只保留前后都不在 time_window 内的孤立事件
        too_close = (dt_prev < time_window) | (dt_next < time_window)

        filtered_df = evt_df.loc[~too_close].copy()

    else:
        filtered_df = evt_df.copy()

    filtered_df = filtered_df.reset_index(drop=True)

    # 保存前恢复为固定 UTC 字符串格式
    filtered_df["time"] = (
        filtered_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%S.%f").str[:23] + "Z"
    )

    if outfile is not None:
        filtered_df.to_csv(outfile, index=False, encoding="utf-8")
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
