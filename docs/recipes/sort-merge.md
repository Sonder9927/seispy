---
title: Archive and merge SAC files
description: Validate and organize SAC files, then merge continuous traces by day.
---

# Archive and merge SAC files

## 1. Copy files into a standard tree

```python
from seispy import waveform

summary = waveform.archive_waveforms(
    "data/sac-unsorted",
    "data/sac-sorted",
    output_format="sac",
    pattern="*.sac",
    max_workers=5,
)

print(summary.succeeded, summary.failed)
```

SAC headers, rather than source filenames, determine the canonical
`network/station/year/file` destination. Archival validates each output and
commits it atomically; source files remain in place unless
`remove_original=True` is requested.

## 2. Merge each channel-day

```python
waveform.merge_waveforms_by_day(
    "data/sac-sorted",
    pattern="*.sac",
    remove_src=False,
)
```

SeisPy reads the headers and groups traces by network, station, location,
channel, year, and Julian day. ObsPy then sorts and merges each group. Gaps of
one second or less are interpolated; longer gaps fail explicitly instead of
being filled with synthetic data.

!!! warning "Source removal"

    `merge_waveforms_by_day` defaults to `remove_src=True`. Always pass
    `remove_src=False` while testing a new archive layout.

[Archive API →](../api/waveform.md#archive-waveform-files) ·
[Merge API →](../api/waveform.md#merge-daily-waveforms)
