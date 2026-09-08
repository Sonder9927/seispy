# Download API

Use these functions to acquire metadata, catalogs, and waveform data from FDSN
services.

## Functions

### Download station metadata

::: seispy.download.download_inventory

### Download earthquake events

::: seispy.download.download_earthquake_events

### Download waveforms

::: seispy.download.download_waveforms

## Result models

Result models are returned by batch functions. Applications normally inspect
their fields or serialize them; they do not need to instantiate them directly.

::: seispy.download.WaveformDownloadSummary
