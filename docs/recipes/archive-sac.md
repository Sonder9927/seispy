---
title: Organize SAC files into an archive
description: Validate scattered SAC files and place them in canonical network/station/year paths.
---

# Organize SAC files into an archive

<a id="interface"></a>

`waveform.archive_waveforms(source_dir, output_dir, ...)`

**Input:** a directory tree containing SAC files in any layout.<br>
**Output:** validated SAC files under
`<output>/<network>/<station>/<year>/`.

## Example

```python
from seispy import waveform
from seispy.waveform import TraceFilter

summary = waveform.archive_waveforms(
    "data/sac-unsorted",
    "data/sac",
    output_format="sac",
    pattern="*.sac",
    max_workers=5,
    trace_filter=TraceFilter(min_duration_seconds=60),
)

print(summary.succeeded, summary.failed)
```

SAC headers, not source filenames, determine the canonical destination. Each
output is validated and committed atomically. Source files are always kept.

`trace_filter=TraceFilter(min_duration_seconds=60)` drops traces shorter than
a minute before they are committed, so truncated or test files cannot enter the
archive; constant and non-finite traces are always rejected as unusable. Omit
`trace_filter` to archive every decodable trace, or see
[Filter waveforms before processing](filter-waveforms.md) to tune the policy.

## Report fields

`succeeded`, `skipped`, and `failed` describe mutually exclusive source-file
outcomes. A partially archived file can succeed while still reporting errors.
The former `errored` field is replaced by `files_with_errors` and
`files_with_warnings`; a source with both contributes once to each counter.

`error_counts` and `warning_counts` summarize all recorded events by reason
code, independently of the sample limit. Common codes include
`source_read_failed`, `existing_content_conflict`, `output_write_failed`,
`output_validation_failed`, `group_processing_failed`, `duplicate_path`, and
`inventory_mismatch` (warning). Unexpected worker failures use `worker_failed`;
other source-processing failures use `archive_failed`.

`issue_samples` contains at most `max_error_samples` events in total, with
`source`, `error` (the readable message), `severity`, and `reason_code`.
The run log records every event at its error or warning level without a
traceback. Normal policy filtering only increments `traces_filtered`.

## Output layout

```text
data/sac/
└── NZ/
    └── WEL/
        └── 2025/
            └── NZ.WEL.10.BHZ.2025.001.000000.sac
```

## Next step

Use [Merge continuous SAC files by day](merge-sac.md) when multiple segments
belong to the same channel and UTC day.

[See the interface reference →](../api/waveform.md#archive-waveform-files)
