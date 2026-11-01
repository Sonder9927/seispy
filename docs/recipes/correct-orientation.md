---
title: Correct sensor orientation
description: Rotate SAC axes using StationXML directions, with optional measured tilt.
---

# Correct sensor orientation

<a id="interface"></a>

`waveform.correct_orientation(source_dir, inventory, *, output_dir, ...)`

**Input:** three-component SAC data and StationXML describing raw axis directions.<br>
**Output:** true-north/east and upward-vertical SAC, including all three channels.

<a id="example"></a>

## Example

Remove each seismic axis's response before mixing axes. For OBS, apply midpoint
clock shifts first, then rotate with tilt disabled. BHH stays in the original
archive and is ignored by both correction functions by default.

```python
from seispy import waveform


def main():
    summary = waveform.correct_orientation(
        "data/obs/sac_clock_corrected",
        "data/obs/metadata/obs_stations.xml",
        output_dir="data/obs/sac_oriented",
        components=("BHZ", "BHN", "BHE"),
        apply_tilt=False,
        max_workers=2,
    )
    print(summary.succeeded, summary.failed, summary.files_written)


if __name__ == "__main__":
    main()
```

## Component interface

`components` is an ordered tuple **(vertical, horizontal-1, horizontal-2)** of
three distinct three-character channel codes. Directions come from the
matching XML channel epoch, not filename replacements or N/E suffixes. For
example, use `components=("BH3", "BH1", "BH2")` when those are the recorded axes.
The default output prefix is the shared first two characters; set
`output_prefix="BH"` explicitly when inputs have different prefixes. Output
suffixes are always Z/N/E.

Azimuth is degrees clockwise from geographic true north. Dip is degrees down
from horizontal; an upward axis has Dip=-90 degrees. Horizontal rotation
solves the two actual azimuth directions and does not assume exactly 90 degrees
between them. With nominal N-axis azimuth theta and E-axis azimuth theta+90:

```text
N_true = cos(theta) * H1 - sin(theta) * H2
E_true = sin(theta) * H1 + cos(theta) * H2
```

### Tilt disabled (default)

`apply_tilt=False` ignores the tilt magnitude and horizontal dips. It rotates
only the horizontal axes, preserves an upward vertical's samples, and reverses
a downward vertical's polarity. A vertical Dip of zero cannot establish the
polarity and fails. All three outputs are written. This is the OBS setting.

### Measured three-dimensional directions

`apply_tilt=True` performs a full `rotate2zne` basis change using each axis's
actual Azimuth and signed Dip. Missing directions, nearly singular axes and
fields explicitly marked `nominal` or `unknown` in Station/Channel
`extra.dip_status` fail. A scalar tilt magnitude alone is insufficient.
The current OBS XML deliberately contains nominal dips and is rejected for
this mode. Standard land-station XML with measured directions can use it.

## Time coverage, grouping and output

Inputs are grouped by network, station, location and **coverage day** from their
headers. Filenames and directory depth do not determine component pairing.
An axis-direction change within one group requires inputs to be split at the
metadata epoch. Each input must fit a single full-coverage metadata epoch.

Compatible contiguous segments and identical overlaps merge losslessly.
The function writes common coverage only, trimming integer sample offsets;
trimmed sample counts appear in provenance. It preserves real gaps and rejects
conflicting overlaps, unequal sample rates and fractional-sample offsets.
There is no interpolation, padding or automatic resampling. A different
midpoint clock shift for differently sized component records can therefore
cause a clear alignment failure rather than an implicit interpolation.

Outputs use canonical paths calculated from corrected timestamps. SAC
`cmpaz` becomes 0/0/90 and `cmpinc` becomes 0/90/90 for Z/N/E.
`kuser1="SPORI"` prevents repeated rotation; this slot must be empty on first
input. Source metadata is unchanged and continues to describe the original
axes and raw response. Do not interpret that response as a calibrated response
for already corrected/rotated nm data. Direction and processing state are
recorded in the output headers and provenance.

Counters count station/location/day **groups**, including incomplete groups as
failures; `files_written` counts actual SAC outputs. `ignored` counts files of
unselected channels. All three outputs are staged and validated before commit;
ordinary write errors roll back newly committed siblings. A process or machine
crash can leave an incomplete group without its completion record: it is not
considered resumable success. Use `overwrite=True` to repair it after checking
logs. Existing destinations without matching provenance fail with
`overwrite=False`.

Both corrections use `.corrections` input/settings fingerprints and output
hashes for retries. Reprocessing source data with changed settings should use
a new output directory. Process pools keep bounded pending tasks and recycle
at `files_per_pool` task groups; they never load a station's whole recording
history at once. Separate source and output trees are required.

[See all parameters](../api/waveform.md#correct-component-orientation).
