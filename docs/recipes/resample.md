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

## SAC example

For the SAC backend, values are sequential decimation factors:

```python
summary = resample_by_station(
    "data/deconvolved",
    delta=[2, 2, 5],
    method="sac",
    output_dir="data/resampled",
    remove_original=False,
)
```

!!! warning "Different meanings"

    `delta` means target hertz for ObsPy but decimation factors for SAC. Do not
    copy a value between backends without checking the intended rate.

[See all parameters →](../api/resample.md#resample-station-data)
