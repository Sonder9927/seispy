---
title: Reduce the waveform sampling rate
description: Phase-preserving SAC-compatible waveform decimation.
---

# Reduce the waveform sampling rate

<a id="interface"></a>

`waveform.decimate_waveforms(src_dir, factors, ...)`

**Input:** SAC or MiniSEED files and an ordered sequence of integer factors.<br>
**Output:** SAC or MiniSEED files at the reduced sampling rate in a separate directory tree.

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

## MiniSEED archives

Use `pattern="*.mseed"` with the SciPy backend. By default, the input format
and relative path are preserved. Set `output_format="mseed"` or `"sac"` to
choose the output format explicitly. MiniSEED output uses FLOAT32 encoding;
filtered samples are never quantized back to integer STEIM encoding. SAC output
requires one trace per input file. The external SAC backend accepts SAC only.

```python
if __name__ == "__main__":
    waveform.decimate_waveforms(
        "data/archive_100hz", 4, pattern="*.mseed",
        output_format="mseed", output_dir="data/archive_25hz",
        max_workers=4,
    )
```

See [Teleseismic processing](teleseismic-processing.md) for both event-processing
paths and the expected response-removal warning after prior decimation.

## Numerical validation of decimation order

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
