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

correct.clock_drift(
    "data/sac",
    "data/drift-corrected",
    "data/metadata/clock-drift.csv",
    max_workers=2,
)
```

## Correct sensor orientation

The orientation table uses `station`, `orientation`, and `tilt` columns. Angular
values are in degrees.

```python
correct.orientation(
    "data/drift-corrected",
    "data/orientation-corrected",
    "data/metadata/orientation.csv",
    max_workers=2,
)
```

!!! tip "Validate three components"

    Test one station first and confirm that matching vertical, north, and east
    component files are present before running a large correction batch.

[Clock API →](../api/correct.md#correct-clock-drift) ·
[Orientation API →](../api/correct.md#correct-component-orientation)
