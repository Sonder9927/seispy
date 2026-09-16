---
title: Organize SAC files into an archive
description: Validate scattered SAC files and place them in canonical network/station/year paths.
---

# Organize SAC files into an archive

## Interface

`waveform.archive_waveforms(source_dir, output_dir, ...)`

**Input:** a directory tree containing SAC files in any layout.<br>
**Output:** validated SAC files under
`<output>/<network>/<station>/<year>/`.

## Example

```python
from seispy import waveform

summary = waveform.archive_waveforms(
    "data/sac-unsorted",
    "data/sac",
    output_format="sac",
    pattern="*.sac",
    max_workers=5,
)

print(summary.succeeded, summary.failed)
```

SAC headers, not source filenames, determine the canonical destination. Each
output is validated and committed atomically. Source files are always kept.

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
