---
title: Event cutting design
description: How continuous SAC files are selected and cut into event windows.
---

# Event cutting design

`cut_event_waveforms` recursively builds one header index for the selected source
tree, then reuses it for every event. The source layout has no semantic meaning.
Records are indexed by network, station, and every UTC day they cover.
A record is selected only when its actual time interval overlaps the requested
event window.

Work is partitioned by the network/station identities found in SAC headers, not
by station directories. Each process receives one station at a time and handles
its events chronologically. Full waveform reads use a bounded LRU cache; returned
streams are copied so processing one event cannot mutate cached data used by
another event.

Output grouping and filenames keep the complete network, station, location, and
channel identity. Directly contiguous records and sample-identical overlaps are
merged without synthesizing samples. Gaps and conflicting overlaps are retained
as timestamped segments. Existing outputs are not overwritten. Invalid source
files are reported as index issues rather than inferred from misleading paths.

The public function intentionally keeps the output outside the source tree. This
prevents repeated runs from treating prior cut products as continuous input.
