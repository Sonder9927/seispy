---
title: OBS continuous data
description: Archive 50 Hz OBS SAC and produce daily 5 Hz displacement with clock and orientation corrections.
---

# OBS continuous data

Archive the prepared OBS records, remove instrument responses, correct clock
drift, and rotate the seismic axes. The result is daily 5 Hz Z/N/E displacement
in nanometers. For earthquake windows and final 1 Hz products, reuse the source
archive in [OBS teleseismic events](obs-teleseismic.md).

## Source processing history

These ocean-bottom seismometers free-fall to the seafloor. Their horizontal
axes need not point north and east, and autonomous clocks accumulate drift.
Ocean currents and pressure variations introduce tilt and seafloor-compliance
noise. The supplied data have already undergone:

1. Acquisition as 100 Hz MiniSEED.
2. Reduction to 50 Hz with ObsPy `resample`.
3. Transfer-function subtraction of Z noise coherent with horizontal and
   pressure observations.
4. Storage as daily SAC in `data/obs/sac_Zdenoise`.

This recipe does not repeat those steps. Transfer-function noise subtraction
is distinct from instrument-response removal and three-dimensional axis
rotation. Inputs must still retain their instrument-response amplitude scale.
For an example of the noise method, see
[Improving ambient noise cross-correlations in the noisy ocean bottom environment of the Juan de Fuca plate](https://academic.oup.com/gji/article/210/3/1787/3898835).

## Input and metadata

Inputs may be grouped by date rather than network, station and year:

```text
data/obs/
├── sac_Zdenoise/
│   └── 20240720/
│       ├── 1A.L51..BHZ.sac
│       ├── 1A.L51..BHN.sac
│       ├── 1A.L51..BHE.sac
│       ├── 1A.L51..BHH.sac
│       └── ...
└── metadata/
    └── obs_inventory.xml
```

Before running, the input and StationXML must satisfy these conditions:

- SAC header network/station/location/channel identifiers match the XML, and
  each complete record lies within one unambiguous metadata epoch. Filenames
  and directories do not replace header identity.
- The current waveform and XML sample rate is **50 Hz**. The original 100 Hz
  acquisition rate belongs to the processing history. The example preserves a
  50 Hz archive, then decimates seismic channels to 25 Hz before response removal.
- BHZ/BHN/BHE have valid displacement responses. This example represents the
  provider's PAZ in XML as the complete displacement-to-counts transfer function.
- The XML contains the fields in [Clock model and precision](correct-clock-drift.md#model-and-precision).
  The signed clock offset is observed time minus GPS time.
- XML `Azimuth` describes each raw sensor axis clockwise from true north.
  BHN/BHE are channel labels, not a claim that the recorded axes already point
  north/east. Nominal `Dip` values are permitted with `apply_tilt=False`.

BHH has no calibrated pressure response in this data set. Its XML response is
an identity placeholder, not a calibration to pascals. **Archive BHH, then
exclude it from response removal, clock correction and rotation.** It may
already have served as an auxiliary observation during upstream noise
correction; that does not require processing it again here.

## Processing order

**Archive 50 Hz counts → decimate to 25 Hz and remove response → shift clocks
→ rotate axes at 25 Hz → decimate to daily 5 Hz Z/N/E.**

Remove each raw axis response before mixing axes. Correct timestamps before
rotation, which requires a shared sample grid. Keep `01_archive` unchanged:
it is also the starting point for [OBS event processing](obs-teleseismic.md).
The continuous and event workflows are two branches from that archive; the
event workflow does not require running the remaining daily stages first.

<a id="example"></a>

## Runnable example

Run `uv sync`, save the complete script in the repository root, and run
`uv run python <script.py>`. On a server, adjust `ROOT` to your data directory.

```python
from pathlib import Path

from seispy import deconvolution, waveform


ROOT = Path("data/obs")
SRC = ROOT / "sac_Zdenoise"
INVENTORY = ROOT / "metadata" / "obs_inventory.xml"
WORK = ROOT / "processed"
MAX_WORKERS = 4

ARCHIVE = WORK / "01_archive"
DECONVOLVED = WORK / "02_displacement_nm"
CLOCK = WORK / "03_clock_shifted"
ORIENTED = WORK / "04_zne"
FINAL = WORK / "05_zne_5hz"


def main():
    # Archive all four channels, including BHH.
    archived = waveform.archive_waveforms(
        SRC,
        ARCHIVE,
        output_format="sac",
        inventory=INVENTORY,
        pattern="*.sac",
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(archived)

    # Process only seismic channels: 50 -> 25 Hz before response removal.
    # Output is displacement in nanometres at 25 Hz.
    response = deconvolution.deconvolve_waveforms(
        ARCHIVE,
        INVENTORY,
        output_dir=DECONVOLVED,
        backend="obspy",
        pattern="*BH[ZNE]*.sac",
        pre_filt=(0.004, 0.006, 4.0, 5.0),
        decimate_factors=[2],
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(response)

    clocks = waveform.correct_clock_drift(
        DECONVOLVED,
        INVENTORY,
        output_dir=CLOCK,
        layout="archive",
        channels=("BHZ", "BHN", "BHE"),
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(clocks)

    orientation = waveform.correct_orientation(
        CLOCK,
        INVENTORY,
        output_dir=ORIENTED,
        layout="archive",
        components=("BHZ", "BHN", "BHE"),
        apply_tilt=False,
        max_workers=MAX_WORKERS,
        overwrite=False,
    )
    print(orientation)
    final = waveform.decimate_waveforms(
        ORIENTED, factors=[5], output_dir=FINAL,
        max_workers=MAX_WORKERS,
    )
    print(final)
    print(f"Final 5 Hz Z/N/E archive: {FINAL}")


if __name__ == "__main__":
    main()
```

The script prints stage summaries. Inspect failures and warnings before using
subsequent products; it does not automatically stop after every partial failure.
Keep the guarded entry point for multiprocessing across platforms.

## Output layout

```text
data/obs/processed/
├── 01_archive/NET/STA/YYYY/*.sac          # 50 Hz counts, including BHH
├── 02_displacement_nm/NET/STA/YYYY/*.sac  # 25 Hz seismic displacement
├── 03_clock_shifted/NET/STA/YYYY/*.sac    # 25 Hz, corrected timestamps
├── 04_zne/NET/STA/YYYY/*.sac              # 25 Hz, upward Z and true N/E
└── 05_zne_5hz/NET/STA/YYYY/*.sac          # Final 5 Hz Z/N/E
```

Reports and logs accompany each stage. All inputs and the source XML remain
unchanged. Reuse `01_archive` in [OBS teleseismic events](obs-teleseismic.md);
`02_displacement_nm` through `05_zne_5hz` are already calibrated products and must
not be deconvolved again with the source response.

## Checks and interpretation

- **Response and rate:** the explicit pre-filter is SeisPy's default
  `(0.004, 0.006, 4.0, 5.0)` Hz. Integrated `[2]` decimation detrends and tapers
  the 50 Hz inputs, filters and reduces them to 25 Hz, then removes response.
  The 4/5 Hz corners remain below Nyquist. Keep the 50 Hz inventory unchanged;
  the integrated reduction does not cause an input sampling-rate warning.
  The ObsPy backend uses `water_level=None`.
- **Final rate:** after clock and orientation correction at 25 Hz, factor
  `[5]` applies anti-alias filtering and reduces the daily products to 5 Hz.
  Displacement remains in nanometers.
- **Clock:** a constant shift is evaluated at each record midpoint. Samples,
  count and sampling interval remain unchanged, with no interpolation or
  rounding to the 0.04 s sample grid. The endpoint residual bound is
  `abs(rate) * duration / 2`, recorded in the log. Adjacent corrected daily
  records can have small gaps or overlaps.
- **Axes:** `layout="archive"` pairs station/location/day records.
  `apply_tilt=False` corrects horizontal directions and retains upward Z.
  Different component coverage can yield different clock shifts; fractional
  misalignment is rejected. Rotation writes common valid coverage and preserves
  real gaps. Three-dimensional correction requires measured signed axis dips;
  the nominal OBS dips do not support it.
- **Repeats:** correction functions do not resume or skip existing outputs.
  Use a new stage directory when changing settings, or explicitly set
  `overwrite=True` to replace current outputs. SAC marks prevent correcting
  already corrected input. Response removal has separate integrity checks.

Check [reports and logs](batch-reports.md) for metadata warnings, filtered
traces, missing components, time alignment and trimmed coverage. Final SAC
`cmpaz/cmpinc` describe Z/N/E, while the original XML still describes raw axes.

[Clock correction](correct-clock-drift.md) ·
[Orientation correction](correct-orientation.md) ·
[Response removal](remove-response.md) ·
[OBS teleseismic events](obs-teleseismic.md)
