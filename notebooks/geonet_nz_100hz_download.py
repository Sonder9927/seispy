import marimo

__generated_with = "0.24.0"
app = marimo.App(width="full")


@app.cell
def _():
    import marimo as mo

    return (mo,)


@app.cell
def _(mo):
    mo.md(r"""
    # GeoNet NZ 台网 100 Hz 波形下载

    本 notebook 完成以下工作：

    1. 从 GeoNet 下载原始 StationXML；
    2. 分析原始 XML 中的台站、通道、采样率和 location；
    3. 固定选择 `NZ` 台网与经纬度区域 `[170, 180, -43.5, -34]`；
    4. 保留 100 Hz 地震通道，排除压力通道 `HDF`；
    5. 针对同台站多 location 的情况分析有效期，并保证同一时刻只选择一套三分量传感器；
    6. 使用筛选后的 XML 生成精确的 NSLC 下载任务，按日下载 MiniSEED。

    > 时间范围为 `2023-08-01T00:00:00Z` 至 `2025-05-01T00:00:00Z`，结束时间不包含在内。
    """)
    return


@app.cell
def _():
    import copy
    from collections import defaultdict
    from pathlib import Path

    import pandas as pd
    from obspy import UTCDateTime, read_inventory

    from seispy import download

    return Path, UTCDateTime, copy, defaultdict, download, pd, read_inventory


@app.cell
def _(Path, UTCDateTime):
    GEONET = "https://service.geonet.org.nz"
    NETWORK = "NZ"
    START = UTCDateTime("2023-08-01T00:00:00Z")
    END = UTCDateTime("2025-05-01T00:00:00Z")
    WEST, EAST, SOUTH, NORTH = 170.0, 180.0, -43.5, -34.0
    TARGET_RATE = 100.0
    EXCLUDED_CHANNELS = {"HDF"}
    MAX_WORKERS = 40

    ROOT = Path("data/geonet_nz_100hz_170_180_20230801_20250501").resolve()
    METADATA_DIR = ROOT / "metadata"
    RAW_XML = METADATA_DIR / "geonet_nz_all_channels_raw.xml"
    SELECTED_XML = METADATA_DIR / "geonet_nz_100hz_selected.xml"
    MSEED_DIR = ROOT / "mseed"
    return (
        EAST,
        END,
        EXCLUDED_CHANNELS,
        GEONET,
        MAX_WORKERS,
        METADATA_DIR,
        MSEED_DIR,
        NETWORK,
        NORTH,
        RAW_XML,
        ROOT,
        SELECTED_XML,
        SOUTH,
        START,
        TARGET_RATE,
        WEST,
    )


@app.cell
def _(
    EAST,
    END,
    EXCLUDED_CHANNELS,
    MAX_WORKERS,
    NETWORK,
    NORTH,
    ROOT,
    SOUTH,
    START,
    TARGET_RATE,
    WEST,
    mo,
):
    mo.md(f"""
    ## 固定配置

    | 项目 | 设置 |
    |---|---|
    | 台网 | `{NETWORK}` |
    | 经度 | `{WEST}°–{EAST}°` |
    | 纬度 | `{SOUTH}°–{NORTH}°` |
    | 时间 | `{START}` 至 `{END}`（右端不含） |
    | 采样率 | `{TARGET_RATE:g} Hz` |
    | 排除通道 | `{", ".join(sorted(EXCLUDED_CHANNELS))}` |
    | 并发请求 | `{MAX_WORKERS}` |
    | 输出目录 | `{ROOT}` |
    """)
    return


@app.cell
def _(mo):
    mo.md("""
    ## 1. 下载原始 StationXML
    """)
    return


@app.cell
def _(mo):
    refresh_xml = mo.ui.run_button(label="下载/刷新原始 StationXML")
    refresh_xml  # noqa: B018 - marimo 将单元格末尾表达式渲染到界面
    return (refresh_xml,)


@app.cell
def _(
    END,
    GEONET,
    METADATA_DIR,
    NETWORK,
    RAW_XML,
    START,
    download,
    mo,
    refresh_xml,
):
    if refresh_xml.value:
        METADATA_DIR.mkdir(parents=True, exist_ok=True)
        download.download_inventory(
            RAW_XML,
            client=GEONET,
            network=NETWORK,
            station="*",
            location="*",
            channel="*",
            starttime=START,
            endtime=END,
            level="channel",
        )
        xml_status = mo.callout(f"原始 StationXML 已写入 `{RAW_XML}`。", kind="success")
    elif RAW_XML.is_file():
        xml_status = mo.callout(
            f"发现已有原始 StationXML：`{RAW_XML}`。点击按钮可刷新。", kind="info"
        )
    else:
        xml_status = mo.callout("尚无原始 StationXML，请点击上方按钮。", kind="warn")
    xml_status  # noqa: B018 - marimo 将单元格末尾表达式渲染到界面
    return


@app.cell
def _(
    EAST,
    END,
    EXCLUDED_CHANNELS,
    NETWORK,
    NORTH,
    SOUTH,
    START,
    TARGET_RATE,
    WEST,
    defaultdict,
):
    def in_target_box(station):
        return (
            WEST <= float(station.longitude) <= EAST
            and SOUTH <= float(station.latitude) <= NORTH
        )

    def epoch_overlaps(channel):
        return (channel.start_date is None or channel.start_date < END) and (
            channel.end_date is None or channel.end_date >= START
        )

    def preliminary_inventory(inventory):
        """区域、台网、时间和采样率筛选；尚不消解 location 重叠。"""
        selected = inventory.copy()
        networks = []
        for network in selected:
            if network.code != NETWORK:
                continue
            stations = []
            for station in network:
                if not in_target_box(station):
                    continue
                station.channels = [
                    channel
                    for channel in station.channels
                    if abs(float(channel.sample_rate) - TARGET_RATE) < 1e-6
                    and channel.code not in EXCLUDED_CHANNELS
                    and epoch_overlaps(channel)
                ]
                if station.channels:
                    stations.append(station)
            network.stations = stations
            if stations:
                networks.append(network)
        selected.networks = networks
        return selected

    def location_groups(inventory):
        groups = defaultdict(lambda: defaultdict(list))
        for network in inventory:
            for station in network:
                for channel in station:
                    key = (network.code, station.code, channel.code)
                    groups[key][channel.location_code or "--"].append(channel)
        return groups

    return in_target_box, location_groups, preliminary_inventory


@app.cell
def _(RAW_XML, in_target_box, mo, pd, read_inventory):
    mo.stop(not RAW_XML.is_file(), mo.md("下载原始 XML 后，本节将自动执行。"))

    raw_inventory = read_inventory(str(RAW_XML), format="STATIONXML")
    raw_rows = [
        {
            "network": network.code,
            "station": station.code,
            "latitude": float(station.latitude),
            "longitude": float(station.longitude),
            "location": channel.location_code or "--",
            "channel": channel.code,
            "sample_rate": float(channel.sample_rate),
            "start": channel.start_date,
            "end": channel.end_date,
            "in_region": in_target_box(station),
        }
        for network in raw_inventory
        for station in network
        for channel in station
    ]
    raw_frame = pd.DataFrame(raw_rows)

    raw_summary = pd.DataFrame(
        [
            {
                "阶段": "原始 XML",
                "台站数": raw_frame[["network", "station"]].drop_duplicates().shape[0],
                "通道历元": len(raw_frame),
                "通道代码数": raw_frame["channel"].nunique(),
            },
            {
                "阶段": "NZ + 目标区域",
                "台站数": raw_frame.loc[raw_frame["in_region"], ["network", "station"]]
                .drop_duplicates()
                .shape[0],
                "通道历元": int(raw_frame["in_region"].sum()),
                "通道代码数": raw_frame.loc[
                    raw_frame["in_region"], "channel"
                ].nunique(),
            },
        ]
    )
    rate_summary = (
        raw_frame.loc[raw_frame["in_region"]]
        .groupby("sample_rate", as_index=False)
        .agg(台站数=("station", "nunique"), 通道历元=("channel", "size"))
        .sort_values("sample_rate")
    )
    return rate_summary, raw_inventory, raw_summary


@app.cell
def _(mo, rate_summary, raw_summary):
    mo.vstack(
        [
            mo.md("## 2. 统计原始 StationXML"),
            mo.md(
                "先统计原始 XML，再查看目标区域内的采样率分布；此时尚未生成精简 XML。"
            ),
            mo.ui.table(raw_summary, pagination=False),
            mo.md("### 目标区域内采样率分布"),
            mo.ui.table(rate_summary, pagination=True),
        ]
    )
    return


@app.cell
def _(END, START, location_groups, pd, preliminary_inventory, raw_inventory):
    preliminary = preliminary_inventory(raw_inventory)
    groups_before = location_groups(preliminary)
    location_rows = []
    for (network, station, channel), locations in sorted(groups_before.items()):
        if len(locations) < 2:
            continue
        simultaneous = False
        values = sorted(locations.items())
        epoch_descriptions = []
        for location, epochs in values:
            for epoch in epochs:
                epoch_start = epoch.start_date or START
                epoch_end = epoch.end_date or END
                epoch_descriptions.append(f"{location}: {epoch_start} — {epoch_end}")
        for index, (_, left_epochs) in enumerate(values):
            for _, right_epochs in values[index + 1 :]:
                for left in left_epochs:
                    for right in right_epochs:
                        left_start = left.start_date or START
                        left_end = left.end_date or END
                        right_start = right.start_date or START
                        right_end = right.end_date or END
                        simultaneous |= max(left_start, right_start) < min(
                            left_end, right_end
                        )
        location_rows.append(
            {
                "network": network,
                "station": station,
                "channel": channel,
                "locations": ",".join(sorted(locations)),
                "同期重叠": simultaneous,
                "XML 有效期": " | ".join(epoch_descriptions),
            }
        )
    location_frame = pd.DataFrame(location_rows)
    return location_frame, preliminary


@app.cell
def _(location_frame, mo):
    mo.vstack(
        [
            mo.md(
                r"""
                ## 3. 从原始 XML 发现多 location 现象

                现在只应用台网、区域、时间、100 Hz 和 `HDF` 排除条件，尚未人为选择
                location。下表直接来自原始 XML，展示同台站同通道出现多个 location
                的情况以及它们是否同期有效。这一步是后续规则的证据来源。
                """
            ),
            mo.ui.table(location_frame, pagination=False),
        ]
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. 查阅 GeoNet 官方定义

    GeoNet 规定一条数据流由 `NETWORK.STATION.LOCATION.CHANNEL` 唯一标识：

    - [GeoNet Stream Naming Conventions](https://www.geonet.org.nz/data/supplementary/channels)
    - [GeoNet FDSN Webservice](https://www.geonet.org.nz/data/access/FDSN)

    官方说明 location 用于区分同一 station 下的不同传感器位置或采集器；`1?`
    保留给弱震动传感器。因此这里的 `10/11/12/13` 是不同 NSLC 数据流，不能当作
    可任意合并的重复副本。官方同时定义 `HDF` 为气压计压力通道，所以本任务将其排除。

    `HHN/HHE` 是北/东向分量；`HH1/HH2` 是非标准定向水平分量，后续处理必须使用
    StationXML 中的 azimuth 旋转，不能仅靠通道名称当成 N/E。
    """)
    return


@app.cell
def _(END, START, UTCDateTime, mo):
    design_explanation = mo.md(r"""
    ## 5. 设计精简 XML

    根据原始 XML 现象和官方定义，采用以下规则：

    1. 只保留 `NZ`、目标矩形内、目标时段有效的 100 Hz 通道；
    2. 排除压力通道 `HDF`；
    3. 同一台站同一时刻只选择一个完整三分量 location；
    4. 非同期的换装 location 按 XML 有效期自然接续；
    5. 同期 location 优先选择覆盖连续、三分量完整且后续稳定的一套；
    6. 绝不把一个 location 的 Z 与另一个 location 的水平分量组合。

    人工审阅得到的同期消歧规则：

    - `ABAZ`：11 使用至 2024-11-20 02:16 UTC，之后使用 12；
    - `MAVZ`：10 使用至 2024-06-21 03:55 UTC，之后使用 11；
    - `WHSZ`：使用覆盖更长且为 N/E 定向的 10；
    - `WTAZ`：使用覆盖完整目标时段的 13。
    """)

    LOCATION_WINDOWS = {
        "ABAZ": {
            "11": ((START, UTCDateTime("2024-11-20T02:16:00Z")),),
            "12": ((UTCDateTime("2024-11-20T02:16:00Z"), END),),
        },
        "MAVZ": {
            "10": ((START, UTCDateTime("2024-06-21T03:55:00Z")),),
            "11": ((UTCDateTime("2024-06-21T03:55:00Z"), END),),
        },
        "WHSZ": {"10": ((START, END),)},
        "WTAZ": {"13": ((START, END),)},
    }
    design_explanation  # noqa: B018 - marimo 渲染单元格末尾表达式
    return (LOCATION_WINDOWS,)


@app.cell
def _(END, LOCATION_WINDOWS, START, copy):
    def preferred_epochs(station_code, channel):
        windows_by_location = LOCATION_WINDOWS.get(station_code)
        if windows_by_location is None:
            return [channel]
        windows = windows_by_location.get(channel.location_code or "")
        if windows is None:
            return []
        result = []
        for window_start, window_end in windows:
            begin = max(channel.start_date or START, window_start, START)
            finish = min(channel.end_date or END, window_end, END)
            if begin < finish:
                clipped = copy.deepcopy(channel)
                clipped.start_date = begin
                clipped.end_date = finish
                result.append(clipped)
        return result

    def apply_location_policy(inventory):
        selected = inventory.copy()
        for network in selected:
            for station in network:
                station.channels = [
                    epoch
                    for channel in station.channels
                    for epoch in preferred_epochs(station.code, channel)
                ]
        return selected

    return (apply_location_policy,)


@app.cell
def _(SELECTED_XML, apply_location_policy, pd, preliminary):
    selected_inventory = apply_location_policy(preliminary)
    SELECTED_XML.parent.mkdir(parents=True, exist_ok=True)
    selected_inventory.write(str(SELECTED_XML), format="STATIONXML")

    selected_rows = [
        {
            "network": network.code,
            "station": station.code,
            "location": channel.location_code or "--",
            "channel": channel.code,
            "sample_rate": float(channel.sample_rate),
            "start": channel.start_date,
            "end": channel.end_date,
        }
        for network in selected_inventory
        for station in network
        for channel in station
    ]
    selected_frame = pd.DataFrame(selected_rows)
    selected_summary = pd.DataFrame(
        [
            {
                "台站数": selected_frame[["network", "station"]]
                .drop_duplicates()
                .shape[0],
                "NSLC 数": selected_frame[["network", "station", "location", "channel"]]
                .drop_duplicates()
                .shape[0],
                "通道历元": len(selected_frame),
                "输出 XML": str(SELECTED_XML),
            }
        ]
    )
    channel_summary = (
        selected_frame.groupby("channel", as_index=False)
        .agg(台站数=("station", "nunique"), 通道历元=("channel", "size"))
        .sort_values("channel")
    )
    channel_summary["NSLC 数"] = [
        selected_frame.loc[selected_frame["channel"] == code][
            ["network", "station", "location", "channel"]
        ]
        .drop_duplicates()
        .shape[0]
        for code in channel_summary["channel"]
    ]
    return channel_summary, selected_inventory, selected_summary


@app.cell
def _(channel_summary, mo, selected_summary):
    mo.vstack(
        [
            mo.md(
                "## 6. 生成并检查精简 XML\n\n"
                "只有完成前述分析和 location 决策后，才在这里写出精简 XML。"
            ),
            mo.ui.table(selected_summary, pagination=False),
            mo.md("### 精简 XML 中的目标通道"),
            mo.ui.table(channel_summary, pagination=False),
        ]
    )
    return


@app.cell
def _(mo):
    start_waveforms = mo.ui.run_button(
        label="开始/继续下载全部 MiniSEED", kind="danger"
    )
    mo.vstack(
        [
            mo.md(
                "## 7. 用精简 XML 安全下载波形\n\n"
                "该操作数据量很大。脚本会检查已有文件，重复运行时跳过有效文件。"
            ),
            start_waveforms,
        ]
    )
    return (start_waveforms,)


@app.cell
def _(
    END,
    GEONET,
    MAX_WORKERS,
    MSEED_DIR,
    NETWORK,
    START,
    download,
    mo,
    selected_inventory,
    start_waveforms,
):
    if start_waveforms.value:
        waveform_summary = download.download_waveforms(
            MSEED_DIR,
            network=NETWORK,
            starttime=START,
            endtime=END,
            station="*",
            location="*",
            channel="*",
            client=GEONET,
            inventory=selected_inventory,
            output_format="mseed",
            max_workers=MAX_WORKERS,
            max_retries=5,
            retry_backoff=2.0,
            overwrite=False,
            save_report=True,
        )
        download_status = mo.callout(
            f"任务数：{waveform_summary.total:,}；下载："
            f"{waveform_summary.downloaded:,}；跳过：{waveform_summary.skipped:,}；"
            f"无数据：{waveform_summary.no_data:,}；失败："
            f"{waveform_summary.failed:,}；报告：`{waveform_summary.report_path}`",
            kind="success" if waveform_summary.ok else "warn",
        )
    else:
        download_status = mo.md("点击按钮后才会开始波形下载。")
    download_status  # noqa: B018 - marimo 将单元格末尾表达式渲染到界面
    return


if __name__ == "__main__":
    app.run()
