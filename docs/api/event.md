# Event interface

Prepare event catalogs and cut event windows from continuous waveform data.

Both cutters request windows beginning at the event origin floored to an
integer UTC second, ending at that start plus `time_window`. The precise origin
is retained in SAC `o`; samples are selected on the original grid without
interpolation. Python preserves gaps as separate files rather than padding
missing data with zero. See [time alignment, filenames, and discontinuous-data
behavior](../recipes/cut-events.md) before comparing backends.

External cutting tools are supplied separately; their source and binaries are
not distributed with SeisPy.

## Functions

### Filter event catalogs

::: seispy.event.catalog.filter_events

### Write an external-tool catalog

::: seispy.event.catalog.write_event_catalog

### Cut events with ObsPy

::: seispy.event.cutting.cut_event_waveforms

### Cut events with external tools

::: seispy.event.external.cut_events_binary

## Result models

The ObsPy cutting workflow returns this immutable summary. Applications normally
do not instantiate it directly.

### Event cutting summary

::: seispy.event.cutting.CutEventSummary
