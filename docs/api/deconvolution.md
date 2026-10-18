# Deconvolution interface

Remove instrument responses from waveform data. StationXML preparation belongs
to the separate [Inventory interface](inventory.md).

## Functions

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

`overwrite=True` recomputes and replaces outputs by default. With
`overwrite=False`, an input is skipped only if all expected SAC outputs pass
basic header and file-length checks; otherwise the whole input is recomputed,
including any existing sibling segments. MiniSEED is read and filtered before
this decision. `summary.existing_skipped` counts complete inputs skipped this
way and is included in `summary.skipped`. Keep the inventory, parameters and
software version unchanged when resuming. See [resuming interrupted runs](../recipes/remove-response.md#resume-after-an-interruption).

::: seispy.deconvolution.removal.deconvolve_waveforms

### Skip unusable traces

`deconvolve_waveforms` accepts a shared `TraceFilter` and skips traces that
are too short, too sparse, or unusable. Filtering is a skip, never a failure.
See [Filter waveform traces](../recipes/filter-waveforms.md).

### Process one stream

::: seispy.deconvolution.removal.remove_response_from_file

## Result models

The batch deconvolution function returns this immutable summary. Applications
normally do not instantiate it directly.

### Deconvolution summary

::: seispy.deconvolution.removal.DeconvolutionSummary
