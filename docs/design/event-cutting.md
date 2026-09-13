---
title: Event cutting design
description: How continuous SAC files are selected and cut into event windows.
---

# Event cutting design

`cut_event_waveforms` builds one header index for the selected archive, then reuses it
for every event. Records are indexed by station and every UTC day they cover.
A record is selected only when its actual time interval overlaps the requested
event window.

Full waveform reads use a bounded LRU cache. Returned streams are copied so
processing one event cannot mutate cached data used by another event.

Output grouping keeps the complete network, station, location, channel, and
channel identity. Existing outputs are not overwritten. Invalid archive files
are reported as index issues rather than inferred from misleading filenames.

For best performance, build one index for a batch of events and avoid calling
the single-station helper repeatedly without passing that index.
