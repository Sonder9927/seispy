---
title: Sort and merge SAC files
description: Organize SAC files and merge continuous traces by day.
---

# Sort and merge SAC files

## 1. Copy files into a standard tree

```python
from seispy import waveform

waveform.sort_waveforms(
    "data/sac-unsorted",
    "data/sac-sorted",
    pattern="*.sac",
)
```

SAC headers, rather than source filenames, determine the canonical
`network/station/year/file` destination.

## 2. Merge each channel-day

```python
waveform.merge_waveforms_by_day(
    "data/sac-sorted",
    pattern="*.sac",
    remove_src=False,
)
```

SeisPy reads the headers and groups traces by network, station, location,
channel, quality code, year, and Julian day. ObsPy then sorts and merges each
group. Gaps of one second or less are interpolated; longer gaps fail explicitly
instead of being filled with synthetic data.

!!! warning "Source removal"

    `merge_waveforms_by_day` defaults to `remove_src=True`. Always pass
    `remove_src=False` while testing a new archive layout.

[Sort API →](../api/waveform.md#sort-sac-files) ·
[Merge API →](../api/waveform.md#merge-daily-waveforms)
