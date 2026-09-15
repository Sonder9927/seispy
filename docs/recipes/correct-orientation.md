---
title: Correct sensor orientation
description: Rotate three-component SAC data using station orientation metadata.
---

# Correct sensor orientation

## Interface

`correct.correct_orientation(net_dir, dest_dir, cor_csv, ...)`

**Input:** one network directory and a CSV containing `station`,
`orientation`, and `tilt` in degrees. Matching `BHZ`, `BHN`, and `BHE` files
must be present.<br>
**Output:** corrected horizontal components in a copied station/year layout.

## Example

```python
from seispy import correct

summary = correct.correct_orientation(
    "data/sac/NZ",
    "data/orientation-corrected/NZ",
    "data/metadata/orientation.csv",
    max_workers=2,
)

print(summary.succeeded, summary.failed)
```

!!! tip "Validate one station first"

    Confirm that the three components have matching time spans and sample
    counts before running the complete network.

[See the interface reference →](../api/correct.md#correct-component-orientation)
