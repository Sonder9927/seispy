---
title: Event cutting design
description: How continuous SAC or MiniSEED files are selected and cut into event windows.
---

# Event cutting design

`cut_event_waveforms` recursively builds one header index for the selected source
tree, then reuses it for every event. The source layout has no semantic meaning.
Records are indexed by network, station, and every UTC day they cover.
A record is selected only when its actual time interval overlaps the requested
event window.

Each cut window begins at the UTC integer second containing the event origin
(floor) and ends at that start plus `time_window`. The actual origin remains
in the SAC `o` marker; it is not rounded in the source
catalog. Sample-grid selection uses nearest samples without interpolation.

Work is partitioned by the network/station identities found in waveform headers, not
by station directories. Each process receives one station at a time and handles
its events chronologically. Full waveform reads use a bounded LRU cache; returned
windows are copied so processing one event cannot mutate cached data used by
another event. Cache retention is bounded by both file count and array bytes.
Overlapping source records are compared in full to preserve conflicts outside
the cut window. Workers publish bounded incremental counters through a bounded
process queue while retaining the station cache; completed futures advertise
their message counts so final queue messages are never lost.

Outputs use `output_dir/YYYYmmddHHMMSS/NET.STA.LOC.CHA.YYYYmmddHHMMSS.sac`,
with the complete network, station, location, and channel identity in each filename.
Catalogs with multiple events in the same UTC second are rejected before writing. Directly contiguous records and sample-identical overlaps are
merged without synthesizing samples. Gaps and conflicting overlaps are retained
as separate segments. Filenames always include the event UTC timestamp through
seconds; multiple segments append `.s01`, `.s02`, and so on. Existing outputs
are not overwritten. Invalid source files are reported as index issues rather
than inferred from misleading paths.

The public function intentionally keeps the output outside the source tree. This
prevents repeated runs from treating prior cut products as continuous input.

The external-tool adapter normalizes a temporary catalog to the same
integer-second windows and restores fractional origin markers in output SAC
headers. It does not change the tool's gap policy: the tested external cutter
fills gaps with zero, while Python preserves the available segments. Agreement
is expected for contiguous data on aligned integer-Hz grids with integer-second
windows, not for missing data or unresolved conflicts. External sources and
executables are user-supplied and are not part of this repository.

Geometry uses the shared SAC header calculation: WGS84 ellipsoidal distance
with GeographicLib and geocentric great-circle azimuths and angular distance.
See [Distance and azimuth convention](../recipes/format-headers.md#distance-and-azimuth-convention).
