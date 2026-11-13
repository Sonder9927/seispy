---
title: Trim existing event windows
---

# Trim existing event windows

Use `event.trim_event_waveforms` to extract 90 minutes from already cut
three-hour SAC events. It writes a separate output tree and never modifies
inputs.

```python
from seispy import event

summary = event.trim_event_waveforms(
    src="data/events_30-120deg",
    output_dir="data/events_90min",
    time_window=(0, 90 * 60),
    reference="origin",
    pattern="*.sac",
    max_workers=5,
    errors_csv=None,
)
```

## Start and end: numeric offsets versus None

`time_window=(start, end)` uses seconds relative to the precise SAC event
origin (reference time plus `o`). Numeric offsets never refer to the trace's
first sample. Only `reference="origin"` is supported. Negative numeric offsets
are allowed. Each bound must be finite or `None`; when both are numeric,
`start < end` is required. The default remains `(0, 5400)`.

| Bound | Meaning |
|---|---|
| `start=None` | Keep this file's original first sample, even if the event beginning is missing. It does **not** mean zero seconds after the origin. |
| Numeric `start` | Require the original recording to start at or before `origin + start`; otherwise skip the file as `missing_start`. Retain the first original sample at or after the requested start. |
| `end=None` | Keep this file's original last sample, including that sample. No fixed end-time coverage is required. |
| Numeric `end` | Stop before `origin + end` (exclusive). Require all original-grid samples up to this boundary; otherwise skip as `missing_end`. |

Examples:

```python
# Require coverage from the origin; extract the first 90 event-relative minutes.
time_window = (0, 5400)

# Keep the original beginning, stop at 90 minutes after the origin.
# A recording beginning 15 minutes late yields only 75 minutes, not 90.
time_window = (None, 5400)

# Require coverage from minute 10 and stop at minute 90.
# A recording beginning at minute 15 is skipped, not shifted or padded.
time_window = (600, 5400)

# Require coverage from minute 10, keeping the original end.
time_window = (600, None)

# No trimming: copy each original file byte-for-byte, including all samples.
# This case does not require an origin header.
time_window = (None, None)
```

For numeric `start=600`, a recording beginning at 300 or exactly 600 seconds
is accepted; one beginning at 900 seconds is skipped. The start check is a
comparison of times, not just sample indices: even a delay smaller than one
sample interval counts as missing coverage. Only up to 1 microsecond of time
representation error is tolerated. If the requested start lies between sample
instants, use the first original sample at or after it without changing that
sample's time. The function never invents a sample at the exact start.

Keeping the original beginning does not relax a numeric end: `(None, 5400)`
still skips a record ending too early. If the chosen bounds contain no samples
(for example, the fixed end is before the retained original start), the file
is skipped as `empty_window`.

Files are searched recursively with a basename glob, retaining their paths
relative to src. Existing output files fail unless `overwrite=True`.

## Samples and coverage

When both bounds are numeric, the interval is half-open:
`[origin + start, origin + end)`. A complete fixed 90-minute interval yields
5400 samples at 1 Hz or 135000 at 25 Hz, without an extra endpoint. With a
`None` bound the duration and sample count depend on that individual record;
`(None, 5400)` does not mean 5400 seconds from its original start.

For a numeric end, an input ending with the last required grid sample is
sufficient; no sample is required exactly at the exclusive end. Coverage
checks happen before loading waveform data. Missing start, missing end and
empty windows are deliberate skips, with no padding or partial fallback.
If both start and end coverage are missing, `missing_start` is counted first.
Missing or invalid `o` is a failure when either bound is numeric. Unreadable
files and output errors are also failures.

Files and segments are handled independently: no merging, component alignment,
interpolation or rate conversion is performed. Grid-based selection uses
ObsPy's normalized SAC sample interval to avoid accumulated float32 delta error
at rates such as 25 Hz.

The output preserves the original SAC reference and `o/a/f/t0..t9` offsets,
keeping their absolute times, even for picks outside the trimmed interval.
`b/e/npts` and amplitude statistics are updated. SAC `b` has float32 precision;
representing the selected first sample has that format's inherent rounding.
Output samples and time/identity headers are verified before atomic commit.
No Inventory is required. These are standard binary SAC files, as accepted
by ObsPy's strict SAC reader.

## Follow-up rotation

Trimming each component to the same event-relative interval can remove
unequal tails when all components fully cover that interval. For example,
MAVZ event 20210111194436 covers the first 90 minutes on both horizontal axes.
It does not repair a noninteger sampling-grid offset, or restore missing event
beginnings such as URZ event 20191203222312. `(0, 5400)` skips that missing
beginning; `(None, 5400)` can retain its later start if it covers the fixed end,
but does not align different component starts. Component completeness is not
required: a station can have only some components written. Inspect the summary
before running `waveform.rotate_event_horizontals` on trimmed output.

## Reporting

The function prints counts and up to five error samples. It returns `total`,
`succeeded`, `skipped`, `failed`, `skipped_by_reason` (`missing_start`,
`missing_end`, `empty_window`), `issue_samples`, `output_dir`, `errors_csv`, and `has_issues`.
Coverage skips do not set `has_issues`. Optional `errors_csv` contains all
failures in `source,error` columns and replaces an existing report; coverage
skips are counted in the summary only. It must be outside the source tree and
must not collide with output waveform files. No other report or log is created.

## API

::: seispy.event.trimming.trim_event_waveforms

::: seispy.event.trimming.EventTrimSummary
