---
title: Select existing event waveforms
---

# Select existing event waveforms

Use `event.select_event_waveforms` to copy selected single-trace SAC files
from an existing event archive, such as `events_deconvolved`, into a separate
output directory. Samples and SAC headers are copied byte-for-byte.

```python
from seispy import event, waveform

summary = event.select_event_waveforms(
    src="data/events_deconvolved",
    output_dir="data/events_selected",
    pattern="*.BHZ.*.sac",
    starttime="2024-01-01",
    endtime="2025-01-01",
    distance_deg=(30, 90),
    trace_filter=waveform.TraceFilter(min_duration_seconds=90 * 60),
    errors_csv="data/selection_errors.csv",  # Optional; no report by default.
)
```

`src` also accepts a list of event directories and/or SAC file paths. Directories
are searched recursively; `pattern` is a basename glob applied to explicit files
as well. Duplicate resolved paths are processed once. Inputs must follow the
existing event layout, with files directly under their event directory. Output
paths preserve `<event-directory>/<filename>`; multiple sources mapping to the
same destination fail rather than silently replacing one another.

Time limits refer to the precise UTC event origin (SAC reference time plus `o`),
not the trace start. The start is inclusive and the end exclusive. Distance
bounds are inclusive and may have one open end, such as `(30, None)`.
Use either `distance_deg=(30, 90)` for SAC `gcarc`, or
`distance_km=(3000, 10000)` for SAC `dist`. They cannot be combined. No distance
conversion or coordinate calculation is performed. Missing or invalid required
headers count as failures; unused headers are not required.

`trace_filter=None` disables quality filtering. Pass a `waveform.TraceFilter`
for duration, sample-count, and sample-quality checks; its duration convention
is `npts / sampling_rate`. Selection does not trim or merge waveforms.
Rejections are counted once, in time, distance, quality order.

Existing output files fail unless `overwrite=True`. Output directories must
be separate from input trees. Files rejected by conditions are skipped;
unreadable files, required missing metadata, and copy conflicts are failures.
Only events with copied files get an event directory.

The function prints a compact final summary and up to five error examples.
It returns `total`, `succeeded`, `skipped`, `failed`, `skipped_by_reason`,
`issue_samples`, `output_dir`, `errors_csv`, and the `has_issues` property.
Optional `errors_csv` writes all failures with just `source,error` columns
(and only a header when there are none). An existing error CSV is replaced.
No persistent log, JSON report, or selection manifest is produced.

## API

::: seispy.event.selection.select_event_waveforms

::: seispy.event.selection.EventSelectionSummary
