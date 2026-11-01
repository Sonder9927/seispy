---
title: Correct station clock drift
description: Shift SAC timestamps from a StationXML linear clock model without resampling.
---

# Correct station clock drift

<a id="interface"></a>

`waveform.correct_clock_drift(source_dir, inventory, *, output_dir, ...)`

**Input:** SAC files anywhere below a source directory, plus StationXML.<br>
**Output:** canonical network/station/year SAC copies with corrected timestamps.

<a id="example"></a>

## Example

Run this after external station-name normalization, archival and seismic
response removal. The current sample rate comes from the input SAC: OBS
recordings acquired at 100 Hz and previously resampled to 50 Hz remain 50 Hz.
BHH is excluded by the default channel selection.

```python
from seispy import waveform


def main():
    summary = waveform.correct_clock_drift(
        "data/obs/sac_deconvolved",
        "data/obs/metadata/obs_stations.xml",
        output_dir="data/obs/sac_clock_corrected",
        max_workers=2,
    )
    print(summary.succeeded, summary.failed, summary.skipped, summary.ignored)


if __name__ == "__main__":
    main()
```

`channels=("BHZ", "BHN", "BHE")` selects header channel codes, independent of
filenames. For another instrument pass its codes explicitly. `pattern="*.sac"`
controls file discovery. Sources and outputs must be separate directory trees;
source samples, headers and inventory are never modified.

## Model and precision

The function reads these fields from `Station.extra`, with `Channel.extra`
overrides when present. ObsPy represents each extension as a dictionary with
`value` and `namespace`. It reads the explicit fields below; it does not parse
free-form comments or execute the formula strings stored in the OBS XML.

| Field | Meaning |
| --- | --- |
| `clock_model` | Must be `linear_recorded_timestamp` |
| `clock_reference_time` | Model start and zero-time reference, UTC |
| `clock_end_time` | Model validity end, UTC |
| `clock_initial_offset_seconds` | Clock offset at the reference time |
| `clock_rate_seconds_per_second` | Signed slope, seconds/second |
| `clock_sign` | Must be `observed_time_minus_GPS_time` |
| `clock_model_argument` | If present, must describe recorded timestamps |
| `clock_correction_applied` | Must not be true |

For record midpoint `tm`, offset is
`initial_offset + rate * (tm - reference_time)`. The new start time is the old
start minus this offset. Positive clock offsets move timestamps earlier.
The **complete record** must fit both the NSLC epoch and clock model interval;
missing metadata and out-of-range records are reported as failures.

This is a constant midpoint shift, matching the original correction algorithm.
It is **not** a full correction of the linear time scale. Samples, sample count
and sampling interval do not change; there is no interpolation or rounding to
an integer sample shift. A 50 Hz record can shift by a fractional 0.02 seconds.
The per-file record reports the applied shift and maximum endpoint residual
`abs(rate) * duration / 2`, in seconds and sample intervals. Adjacent shifted
days can have small apparent gaps/overlaps; no continuity is fabricated.

SAC writing avoids intermediate microsecond rounding of the relative start
field. Distant reference times are rebased when necessary to preserve the start
in float32 SAC headers. The output is reread and must preserve start time to
within one microsecond, all sample values and the original sample interval.
SAC reference changes preserve relative-header absolute times subject to SAC's
float32 precision; this operation does not reinterpret event origins or picks
as clock-corrected observations.

## Reports, retries and repeated corrections

Summary counters count selected input **files**. `ignored` counts unselected
channels, including BHH. Files are discovered recursively from header identity.

Each successful file has a `.corrections/<fingerprint>.json` record containing
source hashes, model/settings, timing diagnostics and output hashes. These
records are required for `overwrite=False` retries, even when `save_report=False`.
Existing outputs are skipped only when a complete record and hashes match.
Conflicting outputs fail rather than being silently reused. Use a new output
directory when changing models or time coverage; `overwrite=True` replaces
current destinations but does not clean historical paths from other settings.

SAC `kuser0="SPCLK"` marks corrected samples. Applying clock correction again
to marked input is rejected. This header slot must be empty before first use.
Report/log persistence follows the normal [batch contract](batch-reports.md).
`max_workers=1` runs serially; larger values use bounded process pools.
`files_per_pool=1024` limits files per worker-pool generation.

## Next step

[Correct orientation](correct-orientation.md) on the corrected seismic channels.
Identical three-component windows receive identical time shifts. If component
windows differ, check the output grids; rotation will not interpolate them.

[See all parameters](../api/waveform.md#correct-clock-drift).
