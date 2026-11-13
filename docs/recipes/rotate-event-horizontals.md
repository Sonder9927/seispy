---
title: Rotate event horizontals in place
---

# Rotate event horizontals in place

`waveform.rotate_event_horizontals` replaces numbered horizontal SAC pairs
with geographic N/E files **inside the source directory**, then deletes the
original 1/2 files. Vertical and already geographic components are untouched.

```python
from seispy import waveform

summary = waveform.rotate_event_horizontals(
    src="data/events_30-120deg",
    inventory="data/geonet_nz/metadata/geonet_nz_100hz_selected.xml",
    pattern="NZ.*.HH[12].*.sac",
    max_workers=5,
    errors_csv=None,  # Optionally use a path outside src.
)
```

Both axes must match the basename glob. The function scans recursively,
reads SAC headers first, and groups by parent event directory and header
network/station/location/channel prefix. The default pattern is
`*.HH[12].*.sac`; other prefixes work with an explicit pattern. Filenames
must contain exactly one dot-delimited channel token matching the header.
That token is replaced with N in the component-1 filename and E in the
component-2 filename. Event names and segment suffixes are preserved.

Only pairs with equal sample rates and sample counts are accepted. Start
times may differ by at most the larger of 1 microsecond and 1e-5 samples.
The 1-microsecond floor accommodates SAC timestamp rounding at higher sample
rates (such as 25 Hz). Both outputs use component 1's start time. Segments
must pair uniquely without overlapping other segments of the same component. No trimming, interpolation, gap filling, segment
merging, or resampling takes place. Incompatible or unmatched files remain
unchanged and are reported separately from failed pairs.

The Inventory is read once and indexed by exact NSLC. Each channel's
metadata epoch must cover its entire trace. Azimuths define a two-axis
horizontal basis, solved for geographic north/east. Dips must be within
1 degree of horizontal; missing directions, ambiguous epochs, and singular
or ill-conditioned axes fail. Inventory and waveform sample rates may differ,
as with already decimated, response-corrected events. Use this horizontal
rotation on components with consistent physical units and calibration.

Output headers update channel/kcmpnm, cmpaz (N=0, E=90), cmpinc (90), and
kuser1 (`SPORI`). Other SAC event/station metadata are retained. HHZ is
neither required nor read or rewritten.

## Replacement and errors

An existing N/E file in the same event/NSL/prefix group is a conflict. Outputs
are never overwritten. Both outputs are staged, read back to verify samples,
identity and timing, committed, and synced to disk before either source is
removed. Ordinary output failures roll back newly committed outputs and
retain the originals. Once deletion begins, completed outputs are retained
even if deletion fails. A crash or deletion failure may leave old and new
components together: inspect reported paths before retrying; the function
will not overwrite the completed outputs to repair this automatically.
Do not run multiple writers against the same source tree concurrently.

The function prints a compact summary and up to five error samples. Optional
`errors_csv` writes all failures with `source,error` columns; an existing CSV
is replaced. No other log or manifest is created. The returned summary exposes
`total_pairs`, `rotated_pairs`, `failed_pairs`, `unpaired_files`, `invalid_files`,
`deletion_failed_pairs`, `issue_samples`, `errors_csv`, and `has_issues`.
Deletion failures are included in failed pairs. Unmatched and unreadable files
are separate counts, not invented pairs.

## API

::: seispy.waveform.event_rotation.rotate_event_horizontals

::: seispy.waveform.event_rotation.EventRotationSummary
