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

### Mass-download waveforms (experimental)

This interface delegates provider discovery, availability selection, and bulk
requests to ObsPy's `MassDownloader`. It writes one MiniSEED file per channel
and requested time chunk.

::: seispy.download.download_waveforms_mass

## Download statistics

### Check download status

::: seispy.download.download_status

The following functions are lower-level building blocks for custom workflows.

### Scan daily availability

::: seispy.download.scan_download_availability

### Summarize completeness

::: seispy.download.summarize_download_availability

### Plot availability

::: seispy.download.plot_download_availability

### Analysis result

::: seispy.download.DownloadAnalysis

## Result models

Result models are returned by batch functions. Applications normally inspect
their fields or serialize them; they do not need to instantiate them directly.

### Waveform download summary

::: seispy.download.WaveformDownloadSummary

### Mass-download result

::: seispy.download.MassDownloadResult
