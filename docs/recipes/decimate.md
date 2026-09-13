---
title: Decimate waveforms
description: Phase-preserving SAC-compatible waveform decimation.
---

# Decimate waveforms

## SciPy example

Pass the ordered decimation factors. Their product is the total reduction;
`[5, 5, 4]` changes 100 Hz data to 1 Hz:

```python
from seispy import waveform

summary = waveform.decimate_waveforms(
    "data/deconvolved",
    factors=[5, 5, 4],
    backend="scipy",
    output_dir="data/decimated",
    remove_original=False,
    max_workers=2,
    batch_size=100,
)

print(f"Decimated: {summary.succeeded}/{summary.total}")
print(f"Failed: {summary.failed}")
```

Reports and logs are enabled by default, track every file batch, and flush
progress periodically. See [Batch reports and logs](batch-reports.md).

The SciPy adapter reads the symmetric FIR coefficients from the local licensed
SAC installation and applies them with delay-compensated polyphase filtering.
It does not start SAC, nor does it add detrending or tapering; perform those
once during response removal when needed. Set `SACAUX` or `SACHOME` if SAC is
not installed under `/usr/local/sac`.

Files are collected recursively and then split into global batches, regardless
of their station directories. This balances workers when stations contain
different numbers of files while preserving every relative output path.

## SAC example

For the SAC backend, values are sequential decimation factors:

```python
summary = waveform.decimate_waveforms(
    "data/deconvolved",
    factors=[2, 2, 5],
    backend="sac",
    output_dir="data/decimated",
    remove_original=False,
    batch_size=100,
)
```

SAC processes files in bounded batches. A failed batch is not committed and
the original files remain unchanged.

!!! warning "Factor order matters"

    Both adapters interpret `factors` identically and apply them in the given
    order. Each factor must be an integer from 2 through 7.

[See all parameters →](../api/decimate.md#decimate-files)
