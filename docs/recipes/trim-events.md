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

Time offsets are seconds relative to the precise SAC event origin (reference
time plus `o`), not relative to each trace's first sample. Only `origin` is
supported as the reference. Negative offsets are allowed when input data cover
the requested pre-event interval. Bounds must be finite and strictly increasing.
Files are searched recursively with a basename glob, retaining their paths
relative to src. Existing output files fail unless `overwrite=True`.

## Samples and coverage

The interval is half-open: `[origin + start, origin + end)`. Only original
samples inside it are retained. For a 90-minute interval this yields 5400
samples at 1 Hz or 135000 at 25 Hz, without an extra endpoint. Samples need
not fall exactly on the window boundaries: each trace retains its original
sampling grid. Coverage is complete if every grid sample inside the requested
interval is present in that file. A boundary between samples is valid; an
input ending with the last required sample is sufficient.

Incomplete coverage skips the file without writing partial results or padding.
A sub-sample interval containing no grid samples is also skipped. Missing or
invalid origin metadata, unreadable files and output errors count as failures.
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
beginnings such as URZ event 20191203222312. Component completeness is not
required: a station can have only some components written. Inspect the summary
before running `waveform.rotate_event_horizontals` on trimmed output.

## Reporting

The function prints counts and up to five error samples. It returns `total`,
`succeeded`, `skipped`, `failed`, `skipped_by_reason` (`incomplete_coverage`,
`empty_window`), `issue_samples`, `output_dir`, `errors_csv`, and `has_issues`.
Coverage skips do not set `has_issues`. Optional `errors_csv` contains all
failures in `source,error` columns and replaces an existing report; coverage
skips are counted in the summary only. It must be outside the source tree and
must not collide with output waveform files. No other report or log is created.

## API

::: seispy.event.trimming.trim_event_waveforms

::: seispy.event.trimming.EventTrimSummary
