---
title: GeoNet continuous data
description: Download one day of 100 Hz GeoNet data and produce calibrated 25 Hz and 5 Hz SAC.
---

# GeoNet continuous data

Download one day of public GeoNet data, retain a 100 Hz MiniSEED archive,
and produce displacement in nanometers at 25 Hz and 5 Hz. This example is
for continuous records. To extract earthquakes from the resulting archive,
continue with [Teleseismic events from MiniSEED](teleseismic-processing.md).

## Inputs

The example requests `2025-01-01` through `2025-01-02` UTC for `NZ.ABAZ` and
`NZ.AKFZ`, selecting the 100 Hz `HH?` channels. `HH?` includes both numbered
horizontal channels (`HH1`/`HH2`) and named horizontal channels (`HHN`/`HHE`).
Response removal preserves those channel labels and does not rotate axes.
The script downloads the required StationXML as well as the waveforms.

## Processing order

**Download → archive 100 Hz MiniSEED → decimate to 25 Hz and remove response
→ decimate calibrated SAC to 5 Hz.**

The ObsPy response backend reads MiniSEED directly, avoiding a continuous
100 Hz SAC intermediate. Factor 4 runs before response removal; factors
`[5]` run afterward. Both use the bundled SAC-compatible anti-alias FIR
filters and need no SAC installation. For backend comparisons and decimation
order checks, see [numerical validation](decimate.md#numerical-validation-of-decimation-order).

## Runnable example

Run `uv sync`, save this complete script in the repository root, and run it
with `uv run python <script.py>`. All processing stages use separate directories.

```python
from pathlib import Path

from seispy import deconvolution, download, waveform

ROOT = Path("data/geonet/2025-01-01")
INVENTORY = ROOT / "metadata" / "inventory.xml"
STAGING = ROOT / "00_waveform_staging"
ARCHIVE = ROOT / "01_mseed"
DISPLACEMENT = ROOT / "02_displacement_25hz"
FINAL = ROOT / "03_displacement_5hz"
MAX_WORKERS = 4


def main():
    inventory = download.download_inventory(
        INVENTORY,
        client="https://service.geonet.org.nz",
        network="NZ", station="ABAZ,AKFZ", location="*", channel="HH?",
        starttime="2025-01-01", endtime="2025-01-02",
        level="response",
    )
    downloaded = download.download_waveforms(
        STAGING,
        client="https://service.geonet.org.nz",
        network="NZ", station=["ABAZ", "AKFZ"], location="*", channel="HH?",
        starttime="2025-01-01", endtime="2025-01-02",
        inventory=inventory, overwrite=False,
    )
    print(downloaded)

    archived = waveform.archive_waveforms(
        STAGING, ARCHIVE,
        output_format="mseed", inventory=inventory,
        trace_filter=waveform.TraceFilter(min_duration_seconds=60),
        max_workers=MAX_WORKERS,
    )
    print(archived)

    response = deconvolution.deconvolve_waveforms(
        ARCHIVE, INVENTORY,
        pattern="*.mseed", output_dir=DISPLACEMENT,
        pre_filt=(0.004, 0.006, 4.0, 5.0),
        decimate_factors=[4],
        max_workers=MAX_WORKERS,
    )
    print(response)

    final = waveform.decimate_waveforms(
        DISPLACEMENT, factors=[5], output_dir=FINAL,
        max_workers=MAX_WORKERS,
    )
    print(final)


if __name__ == "__main__":
    main()
```

## Output layout

```text
data/geonet/2025-01-01/
├── metadata/
│   ├── inventory.xml
│   └── inventory.csv
├── 00_waveform_staging/
├── 01_mseed/NET/STA/YYYY/*.mseed
├── 02_displacement_25hz/NET/STA/YYYY/*.sac
└── 03_displacement_5hz/NET/STA/YYYY/*.sac
```

`01_mseed` remains in instrument counts at 100 Hz. Both displacement products
retain the source component directions. Keep the inventory with the archive;
use the coordinate CSV exported with the inventory when cutting events.

## Checks and next steps

Inspect each printed summary and [batch report](batch-reports.md) before using
the results; these examples do not automatically stop after every partial
failure. For this selection, expect six channels when the service supplies
all requested data. Confirm the final rate and coverage in the SAC headers.
GeoNet can return MiniSEED records extending slightly beyond the request;
headers determine actual coverage. Trim explicitly if exact day boundaries
are required.

The explicit pre-filter is SeisPy's default `(0.004, 0.006, 4.0, 5.0)` Hz.
It is applied at 25 Hz before the final reduction to 5 Hz. Do not edit the
100 Hz source response to describe the derived sampling rates.

For an inspectable raw SAC conversion stage, see
[MiniSEED to SAC](convert-miniseed.md). For event products, reuse `01_mseed` in
[Teleseismic events from MiniSEED](teleseismic-processing.md), supplying the
matching inventory and station CSV. Service details are in the
[GeoNet FDSN documentation](https://www.geonet.org.nz/data/access/FDSN).
