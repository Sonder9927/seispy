# Decimation interface

The `scipy` backend uses the fixed FIR filter coefficients from SAC Community
Edition, bundled with SeisPy for factors 2 through 7. SciPy applies these
coefficients; it does not redesign the filters. No SAC installation, executable,
or `SACAUX`/`SACHOME` configuration is required. Only `backend="sac"` requires
an installed SAC executable. See the [backend examples](../recipes/decimate.md)
for complete calls and the [upstream project](https://github.com/EarthScope/sac-community)
for the source of the Apache-2.0 filter data.

## Functions

### Decimate files

::: seispy.waveform.decimation.decimate_waveforms

## Result models

The batch function returns this immutable summary. Applications normally do not
instantiate it directly.

### Decimation summary

::: seispy.waveform.decimation.DecimationSummary
