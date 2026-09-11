---
title: Process one day of GeoNet data
description: Process 100 Hz GeoNet MiniSEED into response-removed 1 Hz or 25 Hz SAC.
---

# Process one day of GeoNet data

This recipe uses public GeoNet data from **2025-01-01 00:00:00 to
2025-01-02 00:00:00 UTC**. It selects the 100 Hz `HH?` channels at `NZ.ABAZ`
and `NZ.AKFZ`.

It presents two workflows:

1. **Complete workflow:** download → MiniSEED to SAC → remove response →
   resample, producing response-removed 1 Hz SAC.
2. **Concise workflow:** download → remove response with the ObsPy backend and
   integrated decimation, producing response-removed 25 Hz SAC directly from
   MiniSEED.

Both workflows preserve the downloaded MiniSEED files.

## Shared configuration and download

Install the project from the repository root:

```bash
uv sync
```

Save the following configuration in a Python script. GeoNet uses numbered
horizontal channels at `ABAZ` (`HH1`, `HH2`) and named horizontal channels at
`AKFZ` (`HHE`, `HHN`), so use `channel="HH?"` rather than listing component
names explicitly.

```python
from pathlib import Path

from obspy import read

from seispy import collate, decimate_files, download, response


GEONET = "https://service.geonet.org.nz"
NETWORK = "NZ"
STATIONS = ["ABAZ", "AKFZ"]
CHANNEL = "HH?"
START = "2025-01-01T00:00:00"
END = "2025-01-02T00:00:00"  # FDSN end times are exclusive.

ROOT = Path("data/geonet/2025-01-01")
STATIONXML = ROOT / "metadata" / "NZ_ABAZ_AKFZ_HH_2025-01-01.xml"
MSEED = ROOT / "01_mseed_raw"


def require_ok(stage, summary):
    """Stop on partial failure and point to the diagnostic report."""
    if not summary.ok:
        raise RuntimeError(
            f"{stage} did not complete successfully; "
            f"see {summary.report_path or 'the terminal output'}"
        )


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

# Verify that every selected channel is 100 Hz before choosing factors.
channels = [channel for net in inventory for sta in net for channel in sta]
if len(channels) != 6:
    raise RuntimeError(f"Expected 6 active channels, found {len(channels)}")
rates = sorted({float(channel.sample_rate) for channel in channels})
if rates != [100.0]:
    raise RuntimeError(f"Expected only 100 Hz channels, found {rates}")

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
```

## Part 1: complete workflow from 100 Hz MiniSEED to 1 Hz SAC

This version keeps conversion, response removal, and resampling as separate
stages. The intermediate SAC files make each transformation easy to inspect.

```text
100 Hz MiniSEED
    ↓ mseed2sac
100 Hz raw-count SAC
    ↓ deconvolution
100 Hz displacement SAC (nm)
    ↓ decimate 5 × 5 × 4
1 Hz displacement SAC (nm)
```

Append the following code to the shared download script:

```python
SAC_COUNTS_100HZ = ROOT / "02_sac_counts_100hz"
SAC_DISP_100HZ = ROOT / "03_sac_displacement_nm_100hz"
SAC_DISP_1HZ = ROOT / "04_sac_displacement_nm_1hz"

# 1. Convert the downloaded MiniSEED files to raw-count SAC.
converted = collate.mseed2sac(
    MSEED,
    SAC_COUNTS_100HZ,
    pattern="*.mseed",
    max_workers=2,
    remove_original=False,
    save_report=True,
)
require_ok("MiniSEED-to-SAC conversion", converted)

# 2. Remove the response without changing the 100 Hz sampling rate.
deconvolved = response.deconvolution_by_station(
    SAC_COUNTS_100HZ / NETWORK,
    STATIONXML,
    backend="obspy",
    pattern="*.sac",
    output_dir=SAC_DISP_100HZ / NETWORK,
    remove_original=False,
    max_workers=2,
    pre_filt=[0.004, 0.006, 4, 5],
    save_report=True,
)
require_ok("Instrument-response removal", deconvolved)

# 3. Resample from 100 Hz to 1 Hz in SAC-compatible stages.
resampled = decimate_files(
    SAC_DISP_100HZ / NETWORK,
    factors=[5, 5, 4],
    backend="scipy",
    pattern="*.sac",
    output_dir=SAC_DISP_1HZ / NETWORK,
    remove_original=False,
    max_workers=2,
    save_report=True,
)
require_ok("Resampling to 1 Hz", resampled)

# 4. Validate every final waveform.
outputs = sorted(SAC_DISP_1HZ.rglob("*.sac"))
if len(outputs) != 6:
    raise RuntimeError(f"Expected 6 final SAC files, found {len(outputs)}")

for path in outputs:
    trace = read(path)[0]
    assert trace.stats.network == NETWORK
    assert trace.stats.station in STATIONS
    assert trace.stats.channel.startswith("HH")
    assert trace.stats.sampling_rate == 1.0
    assert trace.stats.npts > 0
    print(trace.id, trace.stats.starttime, trace.stats.endtime, path)
```

The SciPy decimation backend uses SAC FIR coefficients but does not start the
SAC executable. Set `SACAUX` or `SACHOME` if SAC is not installed under
`/usr/local/sac`.

## Part 2: concise workflow from 100 Hz MiniSEED to 25 Hz SAC

The ObsPy response-removal backend can read MiniSEED directly. Supplying
`decimate_factors=4` makes it detrend and taper each trace, apply the
SAC-compatible anti-alias filter, decimate from 100 Hz to 25 Hz, and then remove
the instrument response. No raw-count SAC intermediate is needed.

Append this code to the shared download script instead of Part 1:

```python
SAC_DISP_25HZ = ROOT / "02_sac_displacement_nm_25hz"

deconvolved = response.deconvolution_by_station(
    MSEED / NETWORK,
    STATIONXML,
    backend="obspy",
    pattern="*.mseed",
    output_dir=SAC_DISP_25HZ / NETWORK,
    remove_original=False,
    max_workers=2,
    decimate_factors=4,
    pre_filt=[0.004, 0.006, 4, 5],
    save_report=True,
)
require_ok("25 Hz response removal", deconvolved)

outputs = sorted(SAC_DISP_25HZ.rglob("*.sac"))
if len(outputs) != 6:
    raise RuntimeError(f"Expected 6 final SAC files, found {len(outputs)}")

for path in outputs:
    trace = read(path)[0]
    assert trace.stats.sampling_rate == 25.0
    assert trace.stats.npts > 0
    print(trace.id, trace.stats.starttime, trace.stats.endtime, path)
```

Here the final Nyquist frequency is 12.5 Hz, so the complete pre-filter
`(0.004, 0.006, 4.0, 5.0)` remains valid without adjustment. Every decimation
factor must be an integer from 2 through 7. If a requested factor would put the
second `pre_filt` corner at or above the final Nyquist frequency, that waveform
fails safely instead of being processed with an invalid frequency band.

## Numerical comparison of processing branches

SeisPy includes a deterministic numerical regression test covering all four
25 Hz branches:

| Response backend | Decimation order |
| --- | --- |
| ObsPy | integrated before response removal |
| ObsPy | independent after response removal |
| SAC | integrated before response removal |
| SAC | independent after response removal |

The test signal is sampled at 100 Hz and contains 0.1, 1, 4, and 20 Hz
components plus seeded noise. Every branch uses the same response model,
`pre_filt=(0.004, 0.006, 4, 5)`, factor 4, and SAC FIR coefficients. Metrics
exclude 1,000 samples at each 25 Hz output edge, where finite-record taper and
filter transients are expected.

Results from the reference macOS ARM64 environment with SAC installed:

| Comparison | Correlation | Normalized RMS difference | Peak-relative difference |
| --- | ---: | ---: | ---: |
| ObsPy, decimate before vs. after response removal | 0.99999999999 | 0.00000382 | 0.00000689 |
| SAC, decimate before vs. after response removal | 0.99999999999 | 0.00000365 | 0.00000663 |
| SAC vs. ObsPy, decimate before response removal | 0.99994861 | 0.01014335 | 0.01516561 |
| SAC vs. ObsPy, decimate after response removal | 0.99994861 | 0.01014337 | 0.01516551 |

The decimation order therefore has negligible interior impact for this
band-limited test. SAC and ObsPy remain strongly correlated, but differ by
about 1% normalized RMS because their detrending, tapering, response evaluation,
and numerical precision are not bit-for-bit identical. The test treats this as
a bounded backend difference rather than requiring exact equality.

The automated acceptance limits are stricter than a file-exists check:

- every branch must produce finite 25 Hz data with exactly 30,000 samples;
- before-versus-after comparisons must have correlation above `0.999999` and
  normalized RMS difference below `1e-5`;
- SAC-versus-ObsPy comparisons must have correlation above `0.999` and
  normalized RMS difference below `0.02`.

The cross-backend test is marked as an integration test and skips only when the
SAC executable or its FIR coefficient files are unavailable. Unit tests still
verify factor validation, Nyquist rejection, command order, failure isolation,
source preservation, and output-header validation without SAC.

## Output layout

The two alternatives produce separate, inspectable directory trees:

```text
data/geonet/2025-01-01/
├── metadata/
│   ├── NZ_ABAZ_AKFZ_HH_2025-01-01.xml
│   └── NZ_ABAZ_AKFZ_HH_2025-01-01.csv
├── 01_mseed_raw/
├── 02_sac_counts_100hz/             # Part 1
├── 03_sac_displacement_nm_100hz/    # Part 1
├── 04_sac_displacement_nm_1hz/      # Part 1 final output
└── 02_sac_displacement_nm_25hz/     # Part 2 final output
```

Waveform directories are organized by
`network/station/year/Julian-day`. Existing MiniSEED files are skipped, so the
download step can safely resume an interrupted archive.

All times are UTC. GeoNet may return a first or last MiniSEED record slightly
outside the requested interval to avoid re-encoding records. Trim explicitly
with ObsPy if exact day boundaries are required.

See the [official GeoNet FDSN documentation](https://www.geonet.org.nz/data/access/FDSN)
for service details.
