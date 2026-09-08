# SeisPy

SeisPy 是一个基于 [ObsPy](https://docs.obspy.org/) 的地震数据处理工具集，面向连续波形整理、仪器响应去除、重采样、事件波形截取、台站校正和 MCMC 输入文件生成等工作流。

📖 **在线文档：[sonder9927.github.io/seispy](https://sonder9927.github.io/seispy/)**

> 项目目前处于早期开发阶段（`0.1.0`）。公开 API、输入文件格式和目录约定仍可能调整，建议在处理重要数据前保留原始文件备份。

## 功能概览

- 从 FDSN 服务下载连续波形、台站响应和 USGS 地震目录
- 对 MiniSEED 和 SAC 文件进行分类、合并及头段整理
- 使用 ObsPy 或 SAC 去除仪器响应
- 按台站批量重采样，或将结果写入新的目录
- 根据事件目录截取连续波形
- 校正台站时钟漂移和水平分量方位
- 生成和汇总 MCMC 反演所需的网格与输入文件
- 使用 Marimo 页面检查合并、响应去除和重采样结果

## 环境要求

- Python 3.12 或更高版本
- 推荐使用 [uv](https://docs.astral.sh/uv/) 管理环境
- 部分流程需要系统中额外安装：
  - [SAC](https://ds.iris.edu/ds/nodes/dmc/software/downloads/sac/)：使用 `method="sac"` 的重采样、响应去除和事件截取
  - [GMT](https://www.generic-mapping-tools.org/)：PyGMT 网格和频散相关流程

## 安装

克隆仓库并同步依赖：

```bash
git clone <repository-url>
cd seispy
uv sync
```

在项目环境中运行 Python：

```bash
uv run python
```

也可以用开发模式安装：

```bash
python -m pip install -e .
```

## 快速开始

### 整理和合并波形

```python
from seispy import collate

# 按目录规则整理 SAC 文件
collate.sort_to("data/raw", "data/sorted", pattern="*.sac")

# 按天合并连续波形
collate.merge_by_day("data/sorted", pattern="*.sac", remove_src=False)
```

### MiniSEED 转 SAC

```python
from seispy import collate

collate.mseed2sac(
    src_dir="data/mseed",
    dest_dir="data/sac",
)
```

### 重采样

```python
from seispy import resample_by_station

# ObsPy 后端：默认写入同级的 data/sac_resampled
resample_by_station(
    src_dir="data/sac",
    delta=1.0,
    method="obspy",
)

# SAC 后端：可依次应用多个 decimate 因子并写入指定目录
resample_by_station(
    src_dir="data/sac",
    delta=[2, 2, 5],
    method="sac",
    output_dir="data/resampled",
)
```

### 下载和处理仪器响应

```python
from seispy import download, response

download.download_inventory(
    output_file="data/response.xml",
    client="GEONET",
    network="NZ",
    starttime="2023-01-01",
    endtime="2025-01-01",
)

# 波形可保存为 "mseed"（默认）或 "sac"
download.download_waveforms(
    output_dir="data/waveforms",
    network="NZ",
    station=["AAA", "BBB"],
    channel="BH?",
    starttime="2024-01-01",
    endtime="2024-01-03",
    output_format="sac",
)

# 地震目录
events = download.download_earthquake_events(
    "2024-01-01",
    "2025-01-01",
    output_csv="data/events.csv",
    minmagnitude=5.5,
)

# EarthScope 波形；受限数据需要使用 EarthScope FDSNWS 凭据
import os

summary = download.download_waveforms(
    output_dir="data/mseed",
    network="XX",
    station=["AAA", "BBB"],
    channel="BH?",
    starttime="2024-01-01",
    endtime="2024-01-05",
    username=os.environ.get("EARTHSCOPE_USERNAME"),
    password=os.environ.get("EARTHSCOPE_PASSWORD"),
)

filtered = response.select_inventory(
    "data/response.xml",
    stations=["WEL", "KHZ"],
    channels=["BHZ", "BHN", "BHE"],
    output_file="data/response_filtered.xml",
)
```

批量去除响应：

```python
from seispy.response import deconvolution_by_station

deconvolution_by_station(
    src_dir="data/sac",
    resp="data/response_filtered.xml",
    method="obspy",
    output_dir="data/deconvolved",
    remove_original=False,
)
```

### 截取事件波形

```python
from seispy.event import cut_events

cut_events(
    src_dir="data/continuous",
    dest_dir="data/events",
    event_csv="data/events.csv",
    station_csv="data/stations.csv",
    time_window=10800,
)
```

### 台站校正

```python
from seispy.correct import clock_drift, orientation

clock_drift(
    src_dir="data/sac",
    dest_dir="data/drift_corrected",
    drift_csv="data/clock_drift.csv",
)

orientation(
    src_dir="data/drift_corrected",
    dest_dir="data/orientation_corrected",
    cor_csv="data/orientation.csv",
)
```

### MCMC 网格

MCMC 模块根据 JSON 配置生成网格目录及反演输入文件：

```python
from seispy.mcmc import collect_results, init_grids

init_grids("config/mcmc.json", max_workers=4)
collect_results("output/grids", "output/summary")
```

该模块会读取地形、沉积层、莫霍面、参考速度模型和面波频散数据。实际字段要求以 `seispy.mcmc.gen.Config` 和 `Paths` 的定义为准。

## 结果检查页面

项目包含一个基于 Marimo 的交互式检查页面：

```bash
uv run marimo run src/halo_seispy.py
```

也可以从项目 Release 下载导出的 `halo-seispy.html`，直接在浏览器中查看。

## 项目结构

```text
.
├── src/
│   ├── seispy/
│   │   ├── collate/       # 文件整理、合并和格式转换
│   │   ├── correct/       # 时钟漂移与方位校正
│   │   ├── download/      # 台站、地震目录和波形下载
│   │   ├── event/         # 事件目录、波形截取及外部切割工具
│   │   ├── mcmc/          # MCMC 网格生成和结果汇总
│   │   ├── response/      # 响应文件管理与仪器响应去除
│   │   └── resample.py    # 重采样
│   ├── halo/              # 数据处理结果检查工具
│   └── halo_seispy.py     # Marimo 页面
├── packages/rose/         # 批处理、报告、日志和路径等通用工具
├── tests/                 # 回归测试
├── docs/                  # MkDocs 文档源文件
├── mkdocs.yml             # 文档站点配置
├── pyproject.toml
└── uv.lock
```

## 使用注意事项

- `remove_original=True` 或 `remove_src=True` 的操作可能删除源波形文件，首次运行时建议显式设置为 `False`。
- 批处理函数通常会使用多进程；处理少量文件或调试时，可将 `max_workers` 设置为 `1`。
- SAC 后端依赖外部 `sac` 命令，不会由 Python 包管理器自动安装。
- 波形文件命名、通道名称和目录层级会影响文件匹配，请先用少量样本验证流程。
- 运行日志写入 `logs/`；需要保存的批处理摘要写入 `logs/reports/`，成功任务默认不生成摘要文件。

## 开发状态

项目当前主要用于研究数据处理。欢迎通过 Issue 报告问题或提出改进建议。

同步开发依赖：

```bash
uv sync --group dev
```

运行代码检查、测试和构建：

```bash
uv run ruff check .
uv run pytest --cov --cov-report=term-missing
uv build --all-packages
```

本地预览 API 文档：

```bash
uv run --group docs mkdocs serve
```

然后访问 `http://127.0.0.1:8000/seispy/`。

如需可打印的单页手册，可额外执行：

```bash
uv run --group docs --group pdf mkdocs build -f mkdocs-print.yml
```

根项目和 `packages/rose` 已组成同一个 uv workspace，共用根目录下的 `uv.lock`。GitHub Actions 会在推送和拉取请求中自动执行上述检查。

## License

本项目使用 [MIT License](LICENSE)。
