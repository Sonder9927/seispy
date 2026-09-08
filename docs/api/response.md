# Response API

Prepare station metadata and remove instrument responses from waveform data.

## Functions

### Combine inventories

::: seispy.response.inventory.combine_inventories

### Select inventory channels

::: seispy.response.inventory.select_inventory

### Shift channel start times

::: seispy.response.inventory.shift_channel_starttime

### Write an inventory

::: seispy.response.inventory.write_inventory

### Remove instrument responses

::: seispy.response.remove_response.deconvolution_by_station

### Process one stream

::: seispy.response.remove_response.stream_removed_response

## Result models

The batch deconvolution function returns this immutable summary. Applications
normally do not instantiate it directly.

::: seispy.response.remove_response.DeconvolutionSummary
