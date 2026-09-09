# Event API

Prepare event catalogs and cut event windows from continuous waveform data.

## Functions

### Filter event catalogs

::: seispy.event.catalog.filter_events

### Write an external-tool catalog

::: seispy.event.catalog.write_event_catalog

### Cut events with ObsPy

::: seispy.event.cut.cut_events

### Cut events with external tools

::: seispy.event.cut_binary.cut_events_binary

## Result models

The ObsPy cutting workflow returns this immutable summary. Applications normally
do not instantiate it directly.

### Event cutting summary

::: seispy.event.cut.CutEventSummary
