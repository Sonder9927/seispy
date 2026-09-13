---
title: Experimental MassDownloader
description: Bulk-download continuous MiniSEED with ObsPy MassDownloader.
---

# Experimental MassDownloader

!!! warning "Experimental interface"

    `mass_download_waveforms` may change before SeisPy reaches a stable release.
    Test it on a short interval before starting a large acquisition. The stable
    `download_waveforms` function remains available and unchanged.

## When to use it

**Default recommendation:** use `download_waveforms` for a known network and a
small or explicit station list. Choose `mass_download_waveforms` when discovery
and acquisition across many stations or providers would otherwise require you
to build substantial selection and bookkeeping logic yourself.

MassDownloader is usually the better fit when:

- the target is a geographic region rather than a prepared station list;
- data may come from multiple FDSN providers;
- the request covers many stations or a long continuous interval;
- you want ObsPy to discover availability before downloading;
- channel and location priorities should select the best available stream;
- waveform completeness, gap rejection, station spacing, and matching
  StationXML should be enforced during acquisition;
- channel-sized MiniSEED chunks are suitable for the downstream workflow.

Prefer the stable `download_waveforms` function when:

- the network and station list are already known;
- the download is small, such as a few stations over several days;
- one daily MiniSEED per station is preferable to per-channel chunks;
- SAC output is required directly;
- a local StationXML file should serve as the task manifest without broad
  provider discovery;
- you need SeisPy's station-day counters, bounded error samples, and retry
  policy;
- exact control over each station-day request is more important than automatic
  provider discovery.

As a practical threshold, consider MassDownloader when the job involves tens
to hundreds of stations, regional spatial selection, multiple providers, or
weeks to years of continuous MiniSEED. This is guidance rather than a strict
size boundary: provider behavior and channel density can matter more than the
number of days.

!!! tip "Simple decision rule"

    If you can describe the job as “download these known stations,” start with
    `download_waveforms`. If you describe it as “find and download suitable
    stations in this region,” try `mass_download_waveforms`.

| Capability | `download_waveforms` | `mass_download_waveforms` |
| --- | --- | --- |
| Status | Stable SeisPy path | Experimental |
| Providers | One FDSN provider per call | One or multiple providers |
| Availability discovery | Station lookup or local inventory | Managed by ObsPy |
| Output | Daily MiniSEED or per-trace SAC | Channel/time-chunk MiniSEED |
| Concurrency | `max_workers` station-day threads | `threads_per_client` per provider |
| Retry/reporting | SeisPy lifecycle report, log, summary, and sampled errors | SeisPy lifecycle report and log plus ObsPy acquisition report |
| Spatial selection | FDSN selectors | Global, rectangular, circular, or custom domain |
| Metadata | Optional input inventory | Downloads matching StationXML |
| Existing data | Header-validated station-day skip | ObsPy validates each storage path |

## Recommended example

```python
from seispy import download

result = download.mass_download_waveforms(
    "data/waveforms",
    "2025-01-01",
    "2025-01-08",
    providers="GEONET",
    network="NZ",
    station="WEL,KHZ",
    location="*",
    channel="BH?",
    chunklength_in_sec=86_400,
    reject_channels_with_gaps=False,
    minimum_length=0.9,
    threads_per_client=3,
    download_chunk_size_in_mb=50,
)

print(f"MiniSEED: {len(result.mseed_files)}")
print(f"StationXML: {len(result.stationxml_files)}")
print(f"Lifecycle report: {result.report_path}")
print(f"Run log: {result.log_path}")
```

SeisPy's lifecycle report and persistent log are enabled by default. They show
whether the blocking MassDownloader call completed or was interrupted; detailed
provider acquisition statistics still come from ObsPy's `print_report=True`.
See [Batch reports and logs](batch-reports.md).

`MassDownloader` uses threads, not Python worker processes.
`threads_per_client=3` means up to three download threads for each provider.
Increasing it can overload public services and may be slower when a provider
rate-limits requests. Start with 2–3.

`download_chunk_size_in_mb` controls the approximate size of bulk requests. A
larger value reduces request count but increases memory use and retry cost.

## Limit downloads with StationXML

Supplying an XML inventory is useful when you already know the allowed stations:

```python
result = download.mass_download_waveforms(
    "data/waveforms",
    "2025-01-01",
    "2025-02-01",
    providers="GEONET",
    network="NZ",
    channel="BH?",
    inventory="data/metadata/selected-stations.xml",
    threads_per_client=3,
)
```

The inventory is passed to ObsPy as `limit_stations_to_inventory`. It narrows
the eligible stations but does not completely replace MassDownloader's remote
availability and metadata workflow. MassDownloader still downloads the
StationXML required for the selected waveform channels into
`<output_dir>/stationxml` unless `stationxml_dir` is supplied.

The other selectors still apply. A station must match both the inventory and
the `network`, `station`, `location`, and `channel` restrictions.

## Spatial domains

The default is ObsPy's global domain, but SeisPy refuses a completely
unrestricted global request. Provide at least a domain, network, station, or
inventory. For a rectangular region:

```python
from obspy.clients.fdsn.mass_downloader import RectangularDomain
from seispy import download

domain = RectangularDomain(
    minlatitude=-48,
    maxlatitude=-34,
    minlongitude=165,
    maxlongitude=179,
)

result = download.mass_download_waveforms(
    "data/waveforms",
    "2025-01-01",
    "2025-01-03",
    domain=domain,
    providers=["GEONET"],
    channel="HH?,BH?",
    threads_per_client=3,
)
```

## Output layout

Waveforms use the flattened annual archive:

```text
data/waveforms/
├── NZ/WEL/2025/NZ.WEL.10.BHZ.2025.001.mseed
└── stationxml/NZ.WEL.xml
```

Unlike the stable downloader, MassDownloader writes a separate file for every
location, channel, and requested time chunk. `download_status` recognizes both
layouts and reads waveform headers when calculating station-day availability.

## Important trade-offs

Advantages:

- bulk requests and provider discovery are handled by ObsPy;
- multiple providers and geographic domains are supported;
- channel priority, gap rejection, minimum length, and station spacing are
  handled by MassDownloader;
- interrupted downloads can reuse files already accepted by ObsPy.

Limitations:

- only MiniSEED output is supported;
- the result does not currently provide SeisPy's per-station failure counters;
- filename intervals describe requested chunks, while `download_status` uses
  actual waveform headers as the authority;
- authenticated-provider behavior depends on the configured ObsPy clients and
  has not yet been standardized by this experimental interface;
- provider availability queries can dominate runtime before downloading begins.

## Safety recommendations

1. Begin with one provider, one station, and one or two days.
2. Inspect the ObsPy report and run `download_status` afterward.
3. Keep `threads_per_client` conservative.
4. Use `minimum_length=0.9` when near-complete daily coverage is required.
5. Keep `sanitize=True` when response metadata is required downstream.

[MassDownloader API →](../api/download.md#mass-download-waveforms-experimental)
