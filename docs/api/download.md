# Download API

Use these functions to acquire metadata, catalogs, and waveform data from FDSN
services.

## Functions

### Download station metadata

::: seispy.download.stations.download_inventory

### Download earthquake events

::: seispy.download.catalog.download_earthquake_events

### Download waveforms

::: seispy.download.waveforms.download_waveforms

### Mass-download waveforms (experimental)

This interface delegates provider discovery, availability selection, and bulk
requests to ObsPy's `MassDownloader`. It writes one MiniSEED file per channel
and requested time chunk.

::: seispy.download.bulk.mass_download_waveforms

## Download statistics

### Check download status

::: seispy.download.availability.download_status

The following functions are lower-level building blocks for custom workflows.

### Scan daily availability

::: seispy.download.availability.scan_download_availability

### Summarize completeness

::: seispy.download.availability.summarize_download_availability

### Plot availability

::: seispy.download.availability.plot_download_availability

### Analysis result

::: seispy.download.availability.DownloadAvailabilityReport

## Result models

Result models are returned by batch functions. Applications normally inspect
their fields or serialize them; they do not need to instantiate them directly.

### Waveform download summary

::: seispy.download.waveforms.WaveformDownloadSummary

### Mass-download result

::: seispy.download.bulk.BulkDownloadSummary
