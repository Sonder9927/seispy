import pandas as pd


def filter_events(
    evtf=None,
    evtfs=None,
    outfile=None,
    time_window=0,
    required_columns: list[str] = ["time", "longitude", "latitude", "depth", "mag"],
):
    if evtf:
        evt_df = pd.read_csv(evtf, usecols=required_columns, parse_dates=["time"])
    elif evtfs:
        evts = [
            pd.read_csv(ievtf, usecols=required_columns, parse_dates=["time"])
            for ievtf in evtfs
        ]
        evt_df = pd.concat(evts).drop_duplicates(keep=False)
    else:
        raise ValueError("No event file provided.")
    if evt_df is None or evt_df.empty:
        raise ValueError("No events found.")

    # filter by time window
    # 按时间排序
    evt_df.sort_values("time", inplace=True)

    # 计算时间差（使用向量化操作）
    evt_df["time_diff"] = evt_df.time.diff().dt.total_seconds()
    # 标记需要排除的事件
    time_filter = evt_df.time_diff < time_window
    time_filter |= time_filter.shift(-1, fill_value=False)  # 同时标记相邻的两个事件
    # 应用时间过滤
    filtered_df = evt_df[~time_filter].drop(columns=["time_diff"])

    # 还原时间格式
    filtered_df["time"] = (
        filtered_df["time"]
        .dt.tz_convert("UTC")  # 确保时区正确
        .dt.strftime("%Y-%m-%dT%H:%M:%S.%f")  # 生成带微秒的字符串
        .str[:23]
        + "Z"  # 截取前23位（三位毫秒）并添加Z
    )
    if outfile:
        filtered_df.to_csv(outfile, index=False)
        print(f"Save {len(filtered_df)} events to {outfile}.")
    return filtered_df


if __name__ == "__main__":
    filter_events(
        evtfs=["data/evt_25.csv", "data/evt_125.csv"],
        outfile="data/events.csv",
        time_window=10800,
    )
