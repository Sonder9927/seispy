---
title: Remove instrument response
description: Deconvolve SAC files with StationXML while preserving source data.
---

# Remove instrument response

## Prerequisite

Download response-level StationXML that covers the network, stations, channels,
and recording interval. See [Station metadata](download-inventory.md).

## Example

```python
from seispy import response

summary = response.remove_instrument_response(
    "data/sac",
    "data/metadata/stations.xml",
    backend="obspy",
    pattern="*.sac",
    output_dir="data/deconvolved",
    remove_original=False,
    max_workers=2,
)

print(f"Processed: {summary.succeeded}/{summary.total}")
print(f"Failed: {summary.failed}")
```

Reports and logs are enabled by default, track every station, and flush progress
periodically. See [Batch reports and logs](batch-reports.md).

By default, response removal does not change the sampling rate. To decimate as
part of this workflow, pass `decimate_factors=[5, 5, 4]`, for example, to change
100 Hz data to 1 Hz. Each factor must be an integer from 2 through 7. SeisPy
removes the mean and trend, tapers the trace, then applies the SAC-compatible
anti-alias filter at each decimation stage before removing the instrument
response. The final Nyquist frequency must remain above the second `pre_filt`
corner; otherwise that file fails with a clear error. Upper `pre_filt` corners
are reduced when needed to finish the frequency taper below the final Nyquist
frequency.

The ObsPy backend also accepts MiniSEED by selecting it with, for example,
`pattern="*.mseed"`. Output is always SAC: a single trace keeps the input stem
with a `.sac` suffix, while a multi-trace MiniSEED file produces one uniquely
named SAC file per trace. With `remove_original=True`, the MiniSEED source is
removed only after every trace has been written and validated successfully.
The SAC backend rejects MiniSEED input before processing; use `backend="obspy"`
for MiniSEED.

The default pre-filter is `(0.004, 0.006, 4.0, 5.0)` Hz. Both backends taper
at most 5% from each edge and cap each edge at 150 seconds for daily records.
Only gaps of one second or less are interpolated; longer gaps fail explicitly.
Before processing, inventories with overlapping response epochs trigger a
lightweight header preflight. `summary.response_conflicts` reports how many
waveform files do not have one unambiguous response covering the complete
trace. Those files fail safely; the remaining files continue and no response
is selected arbitrarily.

## Why these processing defaults are used

The project defaults target the current surface-wave and body-wave research
workflow:

- Surface-wave periods of interest do not exceed 150 seconds. The low-frequency
  pre-filter transition from 0.004 to 0.006 Hz protects the passband beginning
  near `1 / 150 s = 0.0067 Hz`, while the time-domain taper is capped at 150
  seconds per edge.
- Body-wave frequencies of interest do not exceed 2 Hz. The default high-side
  pre-filter corners at 4 and 5 Hz place the transition above that research
  band when the sampling rate permits it.
- Decimation factors should be chosen from the required research band, not only
  from the desired file size. For example, 100 Hz to 25 Hz with factor 4 leaves
  a 12.5 Hz Nyquist frequency and preserves the complete default pre-filter.
  Reducing the data to 1 Hz leaves a 0.5 Hz Nyquist frequency and is therefore
  suitable for the surface-wave workflow, but cannot retain body waves up to
  2 Hz.

!!! warning "Nyquist adjustment during integrated decimation"

    When `decimate_factors` is set, SeisPy evaluates `pre_filt` against the
    final sampling rate before removing the response. If the requested fourth
    corner reaches or exceeds the final Nyquist frequency, the third and fourth
    corners automatically fall back to at most `0.80 × Nyquist` and
    `0.95 × Nyquist`. This keeps the frequency taper valid, but narrows the
    usable high-frequency band. If the second corner reaches or exceeds the
    adjusted fourth corner (`0.95 × Nyquist`), processing fails instead of
    inventing an invalid rolloff; reaching Nyquist also fails. Always confirm
    that the adjusted third corner remains above the highest frequency required
    by the analysis.

Every generated file is validated before it replaces or accompanies the
source. Empty, constant, non-finite, truncated, time-shifted, or sampling-rate
mismatched output is rejected. ObsPy sample values are checked directly in the
already-loaded processed stream; SAC output uses its stored amplitude extrema.
Only fixed-size headers are reread, avoiding a second full read of every daily
waveform.

## Inspect issues

```python
for issue in summary.issue_samples:
    print(issue.source, issue.status, issue.error)
```

!!! note "SAC backend"

    `backend="sac"` uses the same StationXML input and selects the response by
    network, station, location, channel, and recording time. Matching temporary
    pole-zero files are cached by response epoch. The SAC backend processes at
    most 100 files per SAC process by default, so two years of daily data does
    not create one oversized session. Set `sac_batch_size` to tune the balance
    between startup overhead and failure isolation. If a SAC process fails,
    its batch is split recursively until the individual bad file is isolated;
    valid neighbors are retained without slowing down successful batches.

[See all parameters →](../api/response.md#remove-instrument-responses)
