---
title: Reduce the waveform sampling rate
description: Phase-preserving SAC-compatible waveform decimation.
---

# Reduce the waveform sampling rate

<a id="interface"></a>

`waveform.decimate_waveforms(src_dir, factors, ...)`

**Input:** SAC files and an ordered sequence of integer factors.<br>
**Output:** SAC files at the reduced sampling rate in a separate directory tree.

Replace the example paths with your own input and output directories. The output
directory is created automatically and preserves relative file paths. Input and
output trees must not overlap. Save each complete example as a Python script;
the entry-point guard is required for worker processes on macOS and Windows.

| Backend | Requirements |
| --- | --- |
| `scipy` (default) | No SAC installation required; FIR coefficients are bundled |
| `sac` | A configured SAC installation with `sac` available on `PATH` |

## SciPy example

Pass the ordered decimation factors. Their product is the total reduction;
`[5, 5, 4]` changes 100 Hz data to 1 Hz:

```python
from seispy import waveform

def main():
    summary = waveform.decimate_waveforms(
        "data/deconvolved",
        factors=[5, 5, 4],
        backend="scipy",
        pattern="*.sac",
        output_dir="data/decimated_scipy",
        max_workers=2,
        batch_size=100,
    )

    print(f"Decimated: {summary.succeeded}/{summary.total}")
    print(f"Failed: {summary.failed}")
    print(f"Output: {summary.output_dir}")


if __name__ == "__main__":
    main()
```

Reports and logs are enabled by default, track every file batch, and flush
progress periodically. See [Batch reports and logs](batch-reports.md).

The SciPy adapter uses fixed, bundled SAC Community FIR coefficients and applies
them with delay-compensated polyphase filtering. It needs no SAC installation,
executable, or `SACAUX`/`SACHOME` settings. The coefficient files for factors
2 through 7 are pinned to an upstream version and distributed under Apache-2.0;
see the [upstream project](https://github.com/EarthScope/sac-community).
It does not add detrending or tapering; perform those once during response
removal when needed. A local SAC installation or modified local filters do not
change SciPy results.

Files are collected recursively and then split into global batches, regardless
of their station directories. This balances workers when stations contain
different numbers of files while preserving every relative output path.

## SAC example

This example also changes 100 Hz data to 1 Hz, using SAC's `DECIMATE` command:

```python
from seispy import waveform

def main():
    summary = waveform.decimate_waveforms(
        "data/deconvolved",
        factors=[5, 5, 4],
        backend="sac",
        pattern="*.sac",
        max_workers=2,
        output_dir="data/decimated_sac",
        batch_size=100,
    )

    print(f"Decimated: {summary.succeeded}/{summary.total}")
    print(f"Failed: {summary.failed}")
    print(f"Output: {summary.output_dir}")


if __name__ == "__main__":
    main()
```

Both backends require a separate output tree. Source files are never replaced
or removed; a failed batch commits no output for the affected files.

!!! warning "Factor order matters"

    Both adapters interpret `factors` identically and apply them in the given
    order. Each factor must be an integer from 2 through 7.

[See all parameters →](../api/decimate.md#decimate-files)
