---
title: Merge continuous SAC files by day
description: Combine compatible SAC segments into one channel-day waveform.
---

# Merge continuous SAC files by day

## Interface

`waveform.merge_waveforms_by_day(src, ...)`

**Input:** a canonical SAC archive containing one or more segments per
network, station, location, channel, and UTC day.<br>
**Output:** one `.merged.sac` file for each compatible channel-day group.

## Example

```python
from seispy import waveform

waveform.merge_waveforms_by_day(
    "data/sac",
    pattern="*.sac",
    remove_src=False,
)
```

The function reads headers, groups compatible traces, sorts them, and merges
gaps of at most one second by interpolation. Longer gaps fail explicitly
instead of being filled with synthetic data.

!!! warning "Protect source segments"

    The interface defaults to `remove_src=True`. Use `remove_src=False` while
    learning or validating a new archive.

## Next step

Continue with [Remove instrument response](remove-response.md) or
[Measure waveform coverage](waveform-coverage.md).

[See the interface reference →](../api/waveform.md#merge-daily-waveforms)
