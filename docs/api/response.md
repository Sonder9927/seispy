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

!!! note "Research-band defaults and Nyquist behavior"

    The default `pre_filt=(0.004, 0.006, 4.0, 5.0)` and the 150-second taper
    cap are selected for workflows studying surface-wave periods up to 150
    seconds and body-wave frequencies up to 2 Hz. When `decimate_factors` is
    provided, Nyquist is checked at the final sampling rate. High-frequency
    pre-filter corners that no longer fit are lowered automatically. The second
    corner must remain below the adjusted fourth corner (`0.95 × Nyquist`) so
    there is room for a valid rolloff; otherwise an error is raised. Automatic
    lowering guarantees a valid taper, but does not guarantee that the adjusted
    band still covers the user's scientific target.

::: seispy.response.remove_response.deconvolution_by_station

### Process one stream

::: seispy.response.remove_response.stream_removed_response

## Result models

The batch deconvolution function returns this immutable summary. Applications
normally do not instantiate it directly.

### Deconvolution summary

::: seispy.response.remove_response.DeconvolutionSummary
