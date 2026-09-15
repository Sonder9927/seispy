---
title: Correct station timing and orientation
description: Apply clock-drift and three-component orientation corrections.
---

# Correct stations

## Correct clock drift

The drift table must identify each station and provide drift rate and validity
times expected by the correction workflow.

```python
from seispy import correct

summary = correct.correct_clock_drift(
    "data/sac/NZ",
    "data/drift-corrected/NZ",
    "data/metadata/clock-drift.csv",
    max_workers=2,
)

print(summary.succeeded, summary.failed, summary.skipped)
```

## Correct sensor orientation

The orientation table uses `station`, `orientation`, and `tilt` columns. Angular
values are in degrees.

```python
summary = correct.correct_orientation(
    "data/drift-corrected/NZ",
    "data/orientation-corrected/NZ",
    "data/metadata/orientation.csv",
    max_workers=2,
)

print(summary.succeeded, summary.failed)
```

Both workflows write a JSON report and text log by default. Their common
`CorrectionSummary` records completed, failed, and skipped inputs; an
interrupted run leaves its latest checkpoint in the report.

!!! tip "Validate three components"

    Test one station first and confirm that matching vertical, north, and east
    component files are present before running a large correction batch.

[Clock API →](../api/correct.md#correct-clock-drift) ·
[Orientation API →](../api/correct.md#correct-component-orientation)
