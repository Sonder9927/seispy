---
title: Waveform archive layout
description: Identity and storage rules for waveform files.
---

# Waveform archive layout

SeisPy stores waveforms below `network/station/year` without a separate Julian
day directory:

```text
data/
├── metadata/stations.xml
├── mseed/NZ/WEL/2025/NZ.WEL.10.BHZ.2025.001.mseed
└── sac/NZ/WEL/2025/NZ.WEL.10.BHZ.2025.001.000000.sac
```

Use `data/metadata` for StationXML and related station tables, `data/mseed` for
downloaded MiniSEED, and `data/sac` for downloaded or converted SAC. These are
the paths used consistently by the examples; callers may still choose other
roots when a project requires a different layout.

The waveform header is authoritative. Directory names and filenames are
derived indexes; files that disagree with their headers are rejected instead
of silently reassigned.

- SAC identity uses network, station, location, channel, and start time.
- Standard MiniSEED combines one station and UTC start day in one file.
- MassDownloader uses separate location/channel/time chunks.
- A file may span midnight. Completeness checks and event searches use its
  actual header times, not only the date in its name.

Existing archives are not migrated automatically. Validate headers before
placing externally produced files into this layout.
