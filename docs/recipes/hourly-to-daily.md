# Hourly MiniSEED → daily MiniSEED → response removal

Archive first, then merge, then remove the instrument response. Archiving validates
individual input files and preserves usable data independently of other hours.
Daily merging then joins continuous hours before tapering and deconvolution,
reducing processing edges at artificial hourly file boundaries.

Use three separate output trees and keep the raw input unchanged:

```python
from seispy import waveform
from seispy.deconvolution import deconvolve_waveforms


def main():
    archived = waveform.archive_waveforms(
        "data/raw-hourly", "data/archive", pattern="*.mseed",
        output_format="mseed", inventory="stations.xml",
    )
    if archived.has_issues:
        raise RuntimeError("Review the archive report before daily merging")

    merged = waveform.merge_waveforms_by_day(
        "data/archive", "data/daily", pattern="*.mseed",
        output_format="mseed", max_workers=5,
    )
    if merged.errors:
        raise RuntimeError("Review data/daily/merge-errors.txt before deconvolution")

    deconvolve_waveforms(
        "data/daily", "stations.xml", output_dir="data/deconvolved",
        backend="obspy", pattern="*.mseed",
    )


if __name__ == "__main__":
    main()
```

The daily merge groups canonical archive inputs by header NSLC and UTC date.
A sample at midnight belongs only to the following day. Each channel-day is
written as `NET/STA/YYYY/NET.STA.LOC.CHA.YYYY.JJJ.mseed` (`--` for an empty
location). A daily filename is a grouping key, not a claim of complete coverage.
Partial days are accepted, without padding to 24 hours.

Continuous samples and identical overlaps merge losslessly. Gaps remain separate
traces in the same daily file; no interpolation is performed. Conflicting overlaps
or inconsistent sampling rates fail that channel-day, leaving other days usable.
The ObsPy response-removal workflow processes continuous segments separately.
Review gap coverage before scientific analysis; successful merging does not imply
a complete day. Response metadata must cover all processed segments.

Repeating a MiniSEED merge skips only outputs matching the merged input samples.
When hours have been added or corrected, use `overwrite=True` to rebuild. Sources
are never modified. The return value reports `written`, `skipped`, `failed` and
`errors`; discovery errors are included in `errors` independently of day failures.

The default SAC mode remains available for existing callers and retains its
short-gap interpolation behavior. Use MiniSEED mode for the workflow above.

If acquisition starts before the first StationXML channel epoch, the archive keeps
the samples and reports an `inventory_mismatch` warning. Response removal refuses
the continuous segment because the response must cover its complete duration.
Resolve this before the production run: obtain metadata covering the missing
interval, or explicitly crop a separate processing copy to the covered interval
and record the excluded samples. Do not extend response epochs merely to bypass
validation. Day merging alone does not resolve metadata coverage gaps.
