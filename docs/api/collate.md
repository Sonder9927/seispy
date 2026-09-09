# Collate API

Convert, organize, merge, and annotate waveform files.

## Functions

### Convert MiniSEED to SAC

::: seispy.collate.mseed2sac.mseed2sac

### Sort SAC files

::: seispy.collate.sort_to

### Merge daily waveforms

::: seispy.collate.merge_by_day

### Format SAC headers

::: seispy.collate.format_head

## Result models

These immutable summaries are returned by the corresponding batch functions.
Applications normally do not instantiate them directly.

### MiniSEED conversion summary

::: seispy.collate.Mseed2SacSummary

### SAC formatting summary

::: seispy.collate.FormatSummary
