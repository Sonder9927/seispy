---
title: Sort and merge SAC files
description: Organize SAC files and merge continuous traces by day.
---

# Sort and merge SAC files

## 1. Copy files into a standard tree

```python
from seispy import collate

collate.sort_to(
    "data/sac-unsorted",
    "data/sac-sorted",
    pattern="*.sac",
)
```

The source files are copied into a station/year/Julian-day hierarchy.

## 2. Merge each daily directory

```python
collate.merge_by_day(
    "data/sac-sorted",
    pattern="*.sac",
    remove_src=False,
)
```

ObsPy sorts and merges the traces. Gaps of one second or less are interpolated;
longer gaps fail explicitly instead of being filled with synthetic data.

!!! warning "Source removal"

    `merge_by_day` defaults to `remove_src=True`. Always pass
    `remove_src=False` while testing a new archive layout.

[Sort API →](../api/collate.md#sort-sac-files) ·
[Merge API →](../api/collate.md#merge-daily-waveforms)
