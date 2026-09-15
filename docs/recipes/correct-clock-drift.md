---
title: Correct station clock drift
description: Adjust SAC trace times using station clock-drift metadata.
---

# Correct station clock drift

## Interface

`correct.correct_clock_drift(net_dir, dest_dir, drift_csv, ...)`

**Input:** one network directory and a CSV containing station, drift,
drift rate, `starttime`, and `endtime`.<br>
**Output:** a corrected copy that preserves the station/year layout.

## Example

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

Files outside the correction validity interval are counted as skipped. The
source network directory remains unchanged.

## Next step

Inspect `summary.issue_samples` and validate corrected timestamps on one
station before processing a multi-year archive.

[See the interface reference →](../api/correct.md#correct-clock-drift)
