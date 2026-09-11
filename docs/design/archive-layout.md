---
title: Waveform archive layout
description: Identity and storage rules for waveform files.
---

# Waveform archive layout

SeisPy stores waveforms below `network/station/year` without a separate Julian
day directory:

```text
archive/NZ/WEL/2025/NZ.WEL.10.BHZ.D.2025.001.000000.sac
```

The waveform header is authoritative. Directory names and filenames are
derived indexes; files that disagree with their headers are rejected instead
of silently reassigned.

- SAC identity uses network, station, location, channel, quality, and start
  time. MiniSEED quality is preserved in a free SAC `kuser` field.
- Standard MiniSEED combines one station and UTC start day in one file.
- MassDownloader uses separate location/channel/time chunks.
- A file may span midnight. Completeness checks and event searches use its
  actual header times, not only the date in its name.

Existing archives are not migrated automatically. Validate headers before
placing externally produced files into this layout.
