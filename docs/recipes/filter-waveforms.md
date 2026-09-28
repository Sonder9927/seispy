---
title: Filter waveform traces
description: Keep only traces that satisfy a reusable quality policy.
---

# Filter waveform traces

<a id="interface"></a>

`waveform.filter_waveforms(source_dir, output_dir, trace_filter=..., ...)`

**Input:** any directory tree containing MiniSEED and/or SAC files.<br>
**Output:** a separate tree with the same relative paths, holding only accepted
traces. Source files are never modified.

Filtering is a **skip, not a failure**: a file whose traces are all filtered is
reported as `skipped`.

## Example

```python
from seispy import waveform
from seispy.waveform import TraceFilter

summary = waveform.filter_waveforms(
    "data/mseed",
    "data/mseed-filtered",
    trace_filter=TraceFilter(min_duration_seconds=60, min_samples=6000),
    low_frequency=0.004,
    extensions=(".mseed", ".sac"),
    max_workers=4,
)

print(
    f"kept {summary.traces_written} traces, "
    f"filtered {summary.traces_filtered}, "
    f"skipped {summary.skipped} files, failed {summary.failed}"
)
```

Output paths mirror the input tree. A multi-trace MiniSEED input keeps every
accepted trace in one file; a SAC input keeps its single accepted trace.


## One policy everywhere

`TraceFilter` is shared by `filter_waveforms`,
`archive_waveforms`, and `deconvolve_waveforms`, so the same policy
means the same thing in every workflow.

| Field | Default | Rejects when |
| --- | --- | --- |
| `min_duration_seconds` | `10.0` | the covered duration is shorter |
| `min_samples` | `100` | the trace has fewer samples |
| `min_periods` | `None` | duration is below `min_periods / low_frequency` |
| `reject_unusable_samples` | `True` | samples are empty, constant, or non-finite |
| `max_flatline_amplitude` | `None` | peak-to-peak amplitude is at or below this value (tolerant flat-line) |

Header rules always apply. `reject_unusable_samples` rejects exactly
constant traces; `max_flatline_amplitude` additionally catches stuck
channels that jitter within a small amplitude. The sample check needs loaded
data, so it runs for the files read here and for the ObsPy deconvolution
backend, but not for the head-only SAC backend. `archive_waveforms` skips
empty, constant, and non-finite traces by default even without a policy and
counts them under `traces_filtered`.

## Choosing reasonable thresholds

Tie the floor to the lowest frequency you intend to keep:

| Goal | Suggested policy |
| --- | --- |
| Drop only obviously broken fragments | `TraceFilter()` defaults |
| Surface waves up to 150 s period | `TraceFilter(min_duration_seconds=120, min_periods=3)` with `low_frequency=0.004` (a 750 s floor) |
| Body waves only | `TraceFilter(min_duration_seconds=60, min_samples=6000)` |
| Disable filtering | `TraceFilter(min_duration_seconds=0, min_samples=0, reject_unusable_samples=False)` |

`min_periods` is only enforced when a `low_frequency` is supplied.
`deconvolve_waveforms` passes its own `pre_filt` low corner;
`filter_waveforms` takes `low_frequency` explicitly.

## Inspect what was filtered

```python
for item in summary.issue_samples:
    print(item.source, item.status, item.error)
```

## Reuse the same policy

```python
from seispy import deconvolution, waveform
from seispy.waveform import TraceFilter

policy = TraceFilter(min_duration_seconds=120, min_periods=3)

waveform.archive_waveforms(
    "data/waveform-staging", "data/mseed", trace_filter=policy
)

deconvolution.deconvolve_waveforms(
    "data/sac",
    "data/metadata/stations.xml",
    output_dir="data/deconvolved",
    backend="obspy",
    pattern="*.sac",
    trace_filter=policy,
)
```

## Next steps

- Remove responses from the filtered tree:
  [Remove instrument response](remove-response.md).
- Re-measure coverage: [Measure network coverage](waveform-coverage.md).
- Full parameter list: [Waveform interface](../api/waveform.md).
