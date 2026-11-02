---
title: Process OBS records after noise correction
description: Archive 50 Hz ocean-bottom SAC, decimate to 25 Hz before response removal, shift clocks, and rotate axes.
---

# Process OBS records after noise correction

Ocean-bottom seismometers (OBS) in this example are released from a ship and
free-fall to the seafloor. Their horizontal axes need not point north and east,
and their autonomous clocks can accumulate drift during deployment. Ocean
currents and pressure variations also introduce tilt and seafloor-compliance
noise. Transfer functions between horizontal, pressure and vertical observations
can predict coherent noise for subtraction from the vertical component. See
[Improving ambient noise cross-correlations in the noisy ocean bottom environment of the Juan de Fuca plate](https://academic.oup.com/gji/article/210/3/1787/3898835)
for a published application.

This recipe begins after the supplied data have undergone these steps:

1. The instruments recorded **100 Hz MiniSEED**.
2. ObsPy `resample` reduced the records to **50 Hz**.
3. Transfer-function processing reduced noise on Z that was coherent with the
   horizontal components (tilt noise) and pressure observations (seafloor-compliance noise).
4. The prepared records were saved as daily SAC files under `data/obs/sac_Zdenoise`.

These steps are the input's processing history; the example does not repeat
this resampling or transfer-function noise correction. **Subtracting tilt noise
from Z is different from rotating three axes using their signed inclinations.**
This OBS example does not apply three-dimensional tilt correction.

Transfer-function noise correction is also distinct from instrument-response
removal. This example assumes the seismic inputs have not yet been deconvolved
into physical units and retain their original instrument-response amplitude scale.

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

**Archive at 50 Hz → decimate seismic channels to 25 Hz and remove their
responses → shift clocks → rotate horizontal axes → retain Z/N/E at 25 Hz.**

Response removal is evaluated at 25 Hz before clock shifting and before axes
are mixed, allowing each sensor axis to use its own response. Time labels are
then corrected before rotation on a shared sample grid. The program does not
interpolate mismatched components into alignment.

<a id="example"></a>

## Runnable example

Prepare the environment with `uv sync`. Save the complete code below as a
Python script in the repository root, then run `uv run python <script.py>`.
On a server, change `ROOT` to the directory containing the same input structure.
The script calls independent public functions; it does not require a separate
OBS pipeline interface.

```python
from pathlib import Path

from seispy import deconvolution, waveform


ROOT = Path("data/obs")
SRC = ROOT / "sac_Zdenoise"
INVENTORY = ROOT / "metadata" / "obs_inventory.xml"
WORK = ROOT / "processed"

ARCHIVE = WORK / "01_archive"
DECONVOLVED = WORK / "02_displacement_nm"
CLOCK = WORK / "03_clock_shifted"
ORIENTED = WORK / "04_zne"


def main():
    # Archive all four channels, including BHH.
    archived = waveform.archive_waveforms(
        SRC,
        ARCHIVE,
        output_format="sac",
        inventory=INVENTORY,
        pattern="*.sac",
        max_workers=4,
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
        max_workers=4,
        overwrite=False,
    )
    print(response)

    clocks = waveform.correct_clock_drift(
        DECONVOLVED,
        INVENTORY,
        output_dir=CLOCK,
        channels=("BHZ", "BHN", "BHE"),
        max_workers=4,
        overwrite=False,
    )
    print(clocks)

    orientation = waveform.correct_orientation(
        CLOCK,
        INVENTORY,
        output_dir=ORIENTED,
        components=("BHZ", "BHN", "BHE"),
        apply_tilt=False,
        max_workers=4,
        overwrite=False,
    )
    print(orientation)
    print(f"Final Z/N/E archive: {ORIENTED}")


if __name__ == "__main__":
    main()
```

Stage outputs and inputs use separate directory trees. Keep the `main()` call
inside the entry-point guard so the script works with multiprocessing across
platforms.

The script prints each summary; inspect failures and
warnings before using subsequent products, because it does not automatically
stop on a returned partial-failure summary.

## Settings and interpretation

`decimate_factors=[2]` runs inside response removal: remove the mean and linear
trend, apply the time taper, apply anti-alias FIR filtering and factor-two
decimation, then deconvolve. This does not repeat the upstream ObsPy `resample`.

At 25 Hz the Nyquist frequency is 12.5 Hz, above the 4/5 Hz high-side pre-filter
corners, so those corners remain unchanged. No post-response decimation is
requested. The input XML continues to describe the 50 Hz source records; do
not change its source sampling rate to 25 Hz merely to describe derived products.

| Operation | Setting | Result |
| --- | --- | --- |
| Archive | All `*.sac`, cross-checked with XML | BHH and three seismic channels preserved |
| Response | ObsPy; `decimate_factors=[2]`; pre-filter 0.004/0.006/4/5 Hz | BHZ/BHN/BHE displacement in nm at 25 Hz |
| Time taper | Existing default: Hann, at most 5% and 150 s per edge | Same validated response preprocessing |
| Water level | Existing backend uses `water_level=None` | Pre-filter limits the inverse-response band |
| Sample rate | 50 Hz archive, factor-two pre-response decimation | Subsequent products remain 25 Hz |
| Clock | Offset evaluated at the record midpoint | Constant time-label shift, no interpolation |
| Orientation | XML directions, `apply_tilt=False` | True N/E, upward Z preserved |

Clock shifting retains the calculated fractional-sample offset without rounding
to the 0.04 s output sampling interval. Samples, count and sampling interval do
not change. This is the agreed **midpoint-shift approximation, not a correction
of the entire linear time scale**.

Each clock record reports the endpoint residual bound
`abs(rate) * duration / 2`. For the largest drift rate in this sample, a day has
about a 6.2 ms endpoint residual. Shifted adjacent days may have small apparent
gaps or overlaps. The example does not fill them or claim a perfectly
continuous corrected time scale.

Components must share a sample grid before rotation. Different record lengths
can yield different midpoint clock offsets; fractional-sample misalignment is
reported rather than interpolated. Compatible segments can merge losslessly.
Rotation writes only common valid coverage and preserves real gaps. Review any
reported trimming.

`apply_tilt=False` still writes Z alongside N/E. For future land-station data,
`apply_tilt=True` requires actual Azimuth and signed Dip for all three axes.
The current OBS XML marks dips as nominal and cannot support three-dimensional
tilt correction; a scalar tilt magnitude does not establish the missing direction.

## Outputs and checks

All four stages use the canonical archive layout:

```text
processed/
├── 01_archive/1A/<station>/2024/*.sac
├── 02_displacement_nm/1A/<station>/2024/*.sac
├── 03_clock_shifted/1A/<station>/2024/*.sac
└── 04_zne/1A/<station>/2024/*.sac
```

The supplied 2024-07-20 sample has 12 stations and four channels per station:
expect 48 archived SAC files and 36 seismic files in each subsequent stage.
The archive remains 50 Hz; subsequent products are 25 Hz, with a complete day
reduced from 4,320,000 to 2,160,000 samples. Orientation counts 12
station/location/day groups and writes 36 files. If an additional station has
no XML orientation, resolve that metadata before rotating it.

Inspect [batch reports and logs](batch-reports.md) at each stage, especially
archive metadata warnings, quality-filtered response traces, clock shifts and
residuals, missing components and trimmed coverage. `summary.ok` reflects
archive warnings/failures, but quality-filtered response skips are not
necessarily failures; review the counters too.

Clock and orientation stages do not resume or skip existing outputs. Existing
destinations fail unless `overwrite=True`; no `.corrections` records are written.
Response removal has its own output-integrity checks.
Use new stage directories when changing settings, especially when replacing
previous 50 Hz products with 25 Hz products. If an interruption leaves an
incomplete correction group, inspect the log and use that function's
`overwrite=True` repair mode.

Final SAC headers describe upward Z, true N and E, with updated `cmpaz/cmpinc`;
amplitudes remain nm. The original XML describes source responses and raw axes,
not instructions to deconvolve or rotate final products again. Inputs and the
source XML remain unchanged.

[Clock correction details](correct-clock-drift.md) ·
[Orientation details](correct-orientation.md) ·
[Response removal details](remove-response.md)
