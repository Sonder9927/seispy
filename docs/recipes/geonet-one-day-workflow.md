---
title: GeoNet 单日数据处理
description: 下载 ABAZ、AKFZ 的 100 Hz 波形，去响应并降采样为 1 Hz SAC。
---

# GeoNet 单日数据处理：100 Hz MiniSEED 到 1 Hz SAC

本例下载 GeoNet 在 **2025-01-01 00:00:00 至 2025-01-02
00:00:00（UTC）** 的公开数据，只处理 `NZ.ABAZ` 和 `NZ.AKFZ` 两个台站的
`HH?` 通道。处理链如下：

```text
GeoNet FDSN
  ├─ StationXML（响应级元数据）
  └─ 100 Hz HH? MiniSEED
           ↓ ObsPy 直接读取并去仪器响应
       100 Hz 位移 SAC（nm）
           ↓ 带抗混叠滤波的分级抽取 5 × 5 × 4
         1 Hz 位移 SAC（nm）
```

所有处理步骤都保留上一步的文件，不覆盖原始数据。

## 已核验的通道

GeoNet Station 服务在目标日期返回：

| 台站 | 位置码 | 100 Hz 通道 | 水平分量说明 |
| --- | --- | --- | --- |
| `ABAZ` | `12` | `HH1`, `HH2`, `HHZ` | 水平分量按传感器方位编号 |
| `AKFZ` | `10` | `HHE`, `HHN`, `HHZ` | 水平分量按东、北方向命名 |

因此使用 `channel="HH?"`，而不要把通道写死成 `HHE,HHN,HHZ`。FDSN
代码区分大小写，脚本中使用服务实际采用的大写台站代码。

## 准备环境

在仓库根目录安装项目及依赖：

```bash
uv sync
```

GeoNet 官方支持 ObsPy 的 `GEONET` 客户端别名；这里使用明确的 HTTPS
地址，便于识别实际数据来源。

## 完整脚本

将下面内容保存为 `geonet_2025_01_01.py`，然后从仓库根目录运行
`uv run python geonet_2025_01_01.py`。

```python
from pathlib import Path

from obspy import read, read_inventory

from seispy import decimate_files, download, response


GEONET = "https://service.geonet.org.nz"
NETWORK = "NZ"
STATIONS = ["ABAZ", "AKFZ"]
CHANNEL = "HH?"
START = "2025-01-01T00:00:00"
END = "2025-01-02T00:00:00"  # FDSN 的 endtime 不包含在结果中

ROOT = Path("data/geonet/2025-01-01")
STATIONXML = ROOT / "metadata" / "NZ_ABAZ_AKFZ_HH_2025-01-01.xml"
MSEED = ROOT / "01_mseed_raw"
SAC_DISP = ROOT / "02_sac_displacement_nm_100hz"
SAC_1HZ = ROOT / "03_sac_displacement_nm_1hz"


def require_ok(stage, summary):
    """Stop on partial failure and point to the saved diagnostic report."""
    if not summary.ok:
        raise RuntimeError(
            f"{stage} 未完整成功；"
            f"详情见 {summary.report_path or '终端输出'}"
        )


# 1. 先下载响应级 StationXML。它也会生成同名 CSV，便于人工检查。
inventory = download.download_inventory(
    STATIONXML,
    client=GEONET,
    network=NETWORK,
    station=",".join(STATIONS),
    location="*",
    channel=CHANNEL,
    starttime=START,
    endtime=END,
    level="response",
)

# 不只相信 HH? 的名字：确认元数据中的实际采样率均为 100 Hz。
channels = [cha for net in inventory for sta in net for cha in sta]
if len(channels) != 6:
    raise RuntimeError(f"预期 6 个活动通道，实际得到 {len(channels)} 个")
if any(float(cha.sample_rate) != 100.0 for cha in channels):
    rates = sorted({float(cha.sample_rate) for cha in channels})
    raise RuntimeError(f"HH? 中出现非 100 Hz 通道：{rates}")

# 2. 按台站下载一整天的 MiniSEED。传入 inventory 可避免再次查询台站服务。
waveforms = download.download_waveforms(
    MSEED,
    client=GEONET,
    network=NETWORK,
    station=STATIONS,
    location="*",
    channel=CHANNEL,
    starttime=START,
    endtime=END,
    output_format="mseed",
    inventory=inventory,
    max_workers=2,
    max_retries=3,
    retry_backoff=2.0,
    overwrite=False,
    save_report=True,
)
require_ok("MiniSEED 下载", waveforms)

# 3. 用 ObsPy 直接读取 MiniSEED 并去响应，输出每个通道独立的 SAC。
# 读取后会合并连续片段；不超过 1 秒的短间断使用插值补齐。
# 当前 SeisPy 后端输出位移，并换算为 nm。
# 默认 pre_filt=(0.004, 0.006, 4.0, 5.0) Hz，适合本例的一天连续记录。
deconvolved = response.deconvolution_by_station(
    MSEED / NETWORK,  # 此函数要求下一层目录直接是台站代码
    STATIONXML,
    method="obspy",
    pattern="*.mseed",
    output_dir=SAC_DISP / NETWORK,
    remove_original=False,
    max_workers=2,
    save_report=True,
)
require_ok("去仪器响应", deconvolved)

# 4. 从 100 Hz 降至 1 Hz。SciPy 后端按 5 × 5 × 4 顺序使用与 SAC
# 相同的零相位 FIR 抗混叠滤波器；去响应阶段已做 taper，此处不再重复。
decimated = decimate_files(
    SAC_DISP / NETWORK,  # 下一层仍直接是 ABAZ、AKFZ
    factors=[5, 5, 4],
    method="scipy",
    pattern="*.sac",
    output_dir=SAC_1HZ / NETWORK,
    remove_original=False,
    max_workers=2,
    save_report=True,
)
require_ok("降采样到 1 Hz", decimated)

# 5. 最终验收：元数据、台站、通道、采样率和数据值都必须符合预期。
saved_inventory = read_inventory(STATIONXML)
assert {sta.code for net in saved_inventory for sta in net} == set(STATIONS)

outputs = sorted(SAC_1HZ.rglob("*.sac"))
if len(outputs) != 6:
    raise RuntimeError(f"预期 6 个最终 SAC 文件，实际得到 {len(outputs)} 个")

for path in outputs:
    trace = read(path)[0]
    assert trace.stats.network == NETWORK
    assert trace.stats.station in STATIONS
    assert trace.stats.channel.startswith("HH")
    assert trace.stats.sampling_rate == 1.0
    assert trace.stats.npts > 0
    assert trace.data.size and not (trace.data == trace.data[0]).all()
    print(trace.id, trace.stats.starttime, trace.stats.endtime, path)
```

## 输出目录

```text
data/geonet/2025-01-01/
├── metadata/
│   ├── NZ_ABAZ_AKFZ_HH_2025-01-01.xml
│   └── NZ_ABAZ_AKFZ_HH_2025-01-01.csv
├── 01_mseed_raw/                   # 原始 100 Hz MiniSEED
├── 02_sac_displacement_nm_100hz/   # 去响应后的 100 Hz SAC（nm）
└── 03_sac_displacement_nm_1hz/     # 最终 1 Hz SAC（nm）
```

每个波形目录内部继续按 `网络/台站/年份/儒略日` 组织。例如最终文件位于
`03_sac_displacement_nm_1hz/NZ/ABAZ/2025/001/`。

## 关键说明

- 时间全部是 UTC；结束时间设为次日零点，表示完整覆盖 2025-01-01。
- MiniSEED 是原始波形归档，ObsPy 后端可直接读取，不需要先转换一份未去响应的
  SAC。原始 MiniSEED 和两个处理结果目录均被保留，便于复现和回退。
- 去响应不是简单除以灵敏度：ObsPy 会使用 StationXML 中完整的响应级信息，
  做去均值、去趋势、加窗和频域反卷积。本例结果是位移，单位为 nm。
- 100 Hz 到 1 Hz 的降采样比为 100，分三级 `5 × 5 × 4` 完成，并在每级应用
  抗混叠滤波。
- 一整天两站三分量的数据量较大。脚本可重复运行；已有 MiniSEED 会被跳过，
  适合在网络中断后续传。下游目录已有同名 SAC 时，转换阶段会报告冲突；若要
  全量重算，请先把对应的下游输出目录移到备份位置。
- GeoNet 为避免重编码 MiniSEED，返回的第一条或最后一条记录可能略微越过请求
  边界；这是服务的公开行为，不代表请求日期写错。若分析要求严格日界，可在
  去响应后用 ObsPy 的 `trim` 明确裁切。

GeoNet 的 FDSN 服务说明见 [GeoNet 官方文档](https://www.geonet.org.nz/data/access/FDSN)。
