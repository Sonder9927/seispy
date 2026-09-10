---
title: Resample waveforms
description: Change SAC sampling rates using ObsPy or external SAC.
---

# Resample waveforms

## ObsPy example

For the ObsPy backend, `delta` is the target sampling rate in hertz:

```python
from seispy import resample_by_station

summary = resample_by_station(
    "data/deconvolved",
    delta=1.0,
    method="obspy",
    output_dir="data/resampled",
    remove_original=False,
    max_workers=2,
)

print(f"Resampled: {summary.succeeded}/{summary.total}")
print(f"Failed: {summary.failed}")
```

The target rate must be an integer divisor of the source rate. Resampling uses
one or more filtered `decimate` stages (for example, 100 Hz to 1 Hz uses
`10 × 10`) rather than FFT resampling, so anti-alias filtering is applied at
every stage.

## SAC example

For the SAC backend, values are sequential decimation factors:

```python
summary = resample_by_station(
    "data/deconvolved",
    delta=[2, 2, 5],
    method="sac",
    output_dir="data/resampled",
    remove_original=False,
    sac_batch_size=100,
)
```

SAC processes files in bounded batches. A failed batch is not committed and
the original files remain unchanged.

!!! warning "Different meanings"

    `delta` means target hertz for ObsPy but decimation factors for SAC. Do not
    copy a value between backends without checking the intended rate.

[See all parameters →](../api/resample.md#resample-station-data)
