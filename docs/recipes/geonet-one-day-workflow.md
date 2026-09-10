---
title: Process one day of GeoNet data
description: Download 100 Hz ABAZ and AKFZ waveforms, remove responses, and decimate to 1 Hz SAC.
---

# Process one day of GeoNet data: 100 Hz MiniSEED to 1 Hz SAC

This recipe downloads public GeoNet data from **2025-01-01 00:00:00 to
2025-01-02 00:00:00 UTC**. It selects only the `HH?` channels at `NZ.ABAZ`
and `NZ.AKFZ`.

```text
GeoNet FDSN
  ├─ response-level StationXML
  └─ 100 Hz HH? MiniSEED
           ├─ ObsPy: read directly and remove response (recommended)
           └─ SAC: mseed2sac → raw-count SAC → remove response
                         │
                         ▼
       100 Hz displacement SAC (nm)
           ↓ filtered decimation by 5 × 5 × 4
         1 Hz displacement SAC (nm)
```

Each stage preserves its input files.

## Verified channels

The GeoNet station service reports these channels for the requested day:

| Station | Location | 100 Hz channels | Horizontal components |
| --- | --- | --- | --- |
| `ABAZ` | `12` | `HH1`, `HH2`, `HHZ` | Numbered by sensor orientation |
| `AKFZ` | `10` | `HHE`, `HHN`, `HHZ` | Named east and north |

Use `channel="HH?"` instead of hard-coding `HHE,HHN,HHZ`, because the two
stations use different horizontal-component names. FDSN codes are
case-sensitive, so the example uses the uppercase station codes returned by
GeoNet.

## Set up the environment

Install the project and its dependencies from the repository root:

```bash
uv sync
```

GeoNet officially supports ObsPy's `GEONET` client alias. This recipe uses the
explicit HTTPS URL so the data source remains visible.

## Complete workflow with the ObsPy backend

The ObsPy backend is recommended because it reads MiniSEED directly. Save the
following as `geonet_2025_01_01.py`, then run
`uv run python geonet_2025_01_01.py` from the repository root.

```python
from pathlib import Path

from obspy import read, read_inventory

from seispy import decimate_files, download, response


GEONET = "https://service.geonet.org.nz"
NETWORK = "NZ"
STATIONS = ["ABAZ", "AKFZ"]
CHANNEL = "HH?"
START = "2025-01-01T00:00:00"
END = "2025-01-02T00:00:00"  # FDSN end times are exclusive.

ROOT = Path("data/geonet/2025-01-01")
STATIONXML = ROOT / "metadata" / "NZ_ABAZ_AKFZ_HH_2025-01-01.xml"
MSEED = ROOT / "01_mseed_raw"
SAC_DISP = ROOT / "02_sac_displacement_nm_100hz"
SAC_1HZ = ROOT / "03_sac_displacement_nm_1hz"


def require_ok(stage, summary):
    """Stop on partial failure and point to the saved diagnostic report."""
    if not summary.ok:
        raise RuntimeError(
            f"{stage} did not complete successfully; "
            f"see {summary.report_path or 'the terminal output'}"
        )


# 1. Download response-level StationXML first. A matching CSV summary is also
# written for convenient inspection.
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

# Verify the actual metadata instead of assuming every HH? channel is 100 Hz.
channels = [cha for net in inventory for sta in net for cha in sta]
if len(channels) != 6:
    raise RuntimeError(f"Expected 6 active channels, found {len(channels)}")
if any(float(cha.sample_rate) != 100.0 for cha in channels):
    rates = sorted({float(cha.sample_rate) for cha in channels})
    raise RuntimeError(f"HH? includes channels that are not 100 Hz: {rates}")

# 2. Download one day of MiniSEED per station. Passing the inventory avoids a
# second station-service request and restricts requests to active channels.
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
require_ok("MiniSEED download", waveforms)

# 3. Read MiniSEED directly with ObsPy and write one response-removed SAC file
# per channel. Continuous segments are merged, and gaps up to one second are
# interpolated. The default pre_filt=(0.004, 0.006, 4.0, 5.0) Hz is suitable
# for these day-long records. SeisPy writes displacement in nanometres.
deconvolved = response.deconvolution_by_station(
    MSEED / NETWORK,  # Its immediate children must be station directories.
    STATIONXML,
    method="obspy",
    pattern="*.mseed",
    output_dir=SAC_DISP / NETWORK,
    remove_original=False,
    max_workers=2,
    save_report=True,
)
require_ok("Instrument-response removal", deconvolved)

# 4. Decimate from 100 Hz to 1 Hz. The SciPy backend applies the same
# zero-phase FIR anti-alias filters as SAC in three stages: 5 × 5 × 4.
decimated = decimate_files(
    SAC_DISP / NETWORK,
    factors=[5, 5, 4],
    method="scipy",
    pattern="*.sac",
    output_dir=SAC_1HZ / NETWORK,
    remove_original=False,
    max_workers=2,
    save_report=True,
)
require_ok("Decimation to 1 Hz", decimated)

# 5. Validate the metadata and every final waveform.
saved_inventory = read_inventory(STATIONXML)
assert {sta.code for net in saved_inventory for sta in net} == set(STATIONS)

outputs = sorted(SAC_1HZ.rglob("*.sac"))
if len(outputs) != 6:
    raise RuntimeError(f"Expected 6 final SAC files, found {len(outputs)}")

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

## Use the SAC backend instead

The external SAC program cannot read MiniSEED directly. This branch therefore
needs `mseed2sac()` before response removal. Add the following import and
raw-count directory to the complete script:

```python
from seispy import collate

SAC_COUNTS = ROOT / "02_sac_counts_100hz"
```

Replace the ObsPy response-removal step with these two calls. The download,
decimation, and final validation steps remain unchanged.

```python
converted = collate.mseed2sac(
    MSEED,
    SAC_COUNTS,
    pattern="*.mseed",
    max_workers=2,
    remove_original=False,
    save_report=True,
)
require_ok("MiniSEED-to-SAC conversion", converted)

deconvolved = response.deconvolution_by_station(
    SAC_COUNTS / NETWORK,
    STATIONXML,
    method="sac",
    pattern="*.sac",
    output_dir=SAC_DISP / NETWORK,
    remove_original=False,
    max_workers=2,
    save_report=True,
)
require_ok("SAC instrument-response removal", deconvolved)
```

`mseed2sac()` merges continuous segments of the same channel within each
MiniSEED file and interpolates gaps up to one second. It does not merge across
multiple MiniSEED files. The downloader used here creates one MiniSEED file per
station-day, so a separate `merge_by_day()` call is unnecessary. This branch
also requires the SAC executable to be installed.

## Output layout

```text
data/geonet/2025-01-01/
├── metadata/
│   ├── NZ_ABAZ_AKFZ_HH_2025-01-01.xml
│   └── NZ_ABAZ_AKFZ_HH_2025-01-01.csv
├── 01_mseed_raw/                   # Original 100 Hz MiniSEED
├── 02_sac_counts_100hz/            # SAC-backend intermediate only
├── 02_sac_displacement_nm_100hz/   # Response-removed 100 Hz SAC
└── 03_sac_displacement_nm_1hz/     # Final 1 Hz SAC
```

Waveform directories are further organized by
`network/station/year/Julian-day`. For example, final ABAZ files are placed in
`03_sac_displacement_nm_1hz/NZ/ABAZ/2025/001/`.

## Important details

- All times are UTC. Midnight on the following day is the exclusive end time,
  so the request covers all of 2025-01-01.
- MiniSEED remains the raw waveform archive. The ObsPy backend reads it
  directly; the external SAC backend first requires `mseed2sac()`. Both paths
  preserve the original MiniSEED.
- Response removal is not simple sensitivity scaling. It uses the complete
  response metadata in StationXML, removes mean and trend, tapers the data, and
  performs frequency-domain deconvolution. The output is displacement in nm.
- The 100:1 decimation is split into `5 × 5 × 4`, with anti-alias filtering at
  every stage.
- Existing MiniSEED files are skipped, so interrupted downloads can be resumed.
  In the SAC branch, `mseed2sac()` reports a conflict if a raw-count SAC output
  already exists; move the output directory aside before a complete rerun.
- GeoNet may return the first or last MiniSEED record slightly outside the
  requested interval to avoid re-encoding records. If exact day boundaries are
  required, trim explicitly with ObsPy after response removal.

See the [official GeoNet FDSN documentation](https://www.geonet.org.nz/data/access/FDSN)
for service details.
