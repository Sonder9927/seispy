---
title: Convert MiniSEED to SAC
description: Convert one file or a directory tree and inspect the batch summary.
---

# Convert MiniSEED to SAC

**Input:** one MiniSEED file or a directory tree.<br>
**Output:** SAC files grouped by network, station, year, and Julian day.

## Example

```python
from seispy import waveform

summary = waveform.convert_mseed_to_sac(
    "data/miniseed",
    "data/sac",
    pattern="*.mseed",
    max_workers=2,
    remove_original=False,
)

print(f"Inputs: {summary.total}")
print(f"Converted: {summary.succeeded}")
print(f"SAC traces: {summary.traces_written}")
print(f"Failed: {summary.failed}")
```

!!! warning "Keep the input while learning"

    `remove_original=False` is intentional. Set it to `True` only after the SAC
    output has been inspected.

## Handle failures

```python
for issue in summary.issue_samples:
    print(issue.source, issue.status, issue.error)

if summary.report_path:
    print(f"JSON report: {summary.report_path}")
print(f"Run log: {summary.log_path}")
```

Reports and logs are enabled by default, track every conversion batch, and
flush progress periodically. See [Batch reports and logs](batch-reports.md).

[See all parameters →](../api/waveform.md#convert-miniseed-to-sac)
