# Waveform API

Convert, organize, merge, and annotate waveform files.

## Functions

### Archive raw waveform responses

::: seispy.waveform.archiving.archive_waveforms

### Convert MiniSEED to SAC

::: seispy.waveform.conversion.convert_mseed_to_sac

### Measure waveform coverage

::: seispy.waveform.coverage.waveform_coverage

The scan, summary, and plot functions are also available separately for custom
workflows.

::: seispy.waveform.coverage.scan_waveform_coverage

::: seispy.waveform.coverage.summarize_waveform_coverage

::: seispy.waveform.coverage.plot_waveform_coverage

### Sort SAC files

::: seispy.waveform.organization.sort_waveforms

### Merge daily waveforms

::: seispy.waveform.merge.merge_waveforms_by_day

### Format SAC headers

::: seispy.waveform.headers.format_sac_headers

## Result models

These immutable summaries are returned by the corresponding batch functions.
Applications normally do not instantiate them directly.

### Waveform archive summary

::: seispy.waveform.archiving.WaveformArchiveSummary

### MiniSEED conversion summary

::: seispy.waveform.conversion.WaveformConversionSummary

### Waveform coverage report

::: seispy.waveform.coverage.WaveformCoverageReport

### SAC formatting summary

::: seispy.waveform.headers.SacHeaderSummary
