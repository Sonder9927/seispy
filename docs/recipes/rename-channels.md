---
title: Rename SAC channels in place
---

# Rename SAC channels in place

Use `waveform.rename_channels` when the physical channel identity is already
known and only the channel code needs correcting. This does not rotate or
resample data, or change response units or Inventory metadata.

```python
from seispy import waveform

summary = waveform.rename_channels(
    src="data/events",
    mapping={"HHX": "BHE", "HHY": "BHN"},
    pattern="*.NZ29.*.sac",
    max_workers=5,
    errors_csv=None,  # Optionally provide a path outside src.
)
```

The function searches recursively using a basename glob. Mapping keys match
SAC header channel codes exactly. Codes must be three uppercase ASCII letters
or digits. Unmapped channels and identity mappings such as `HHZ: HHZ` are
skipped. Each source applies the mapping once: `HHX: BHE, BHE: BHN` does not
recursively turn an original HHX into BHN.

A filename must contain exactly one dot-delimited channel token matching its
header. For example:

```text
NZ.NZ29.10.HHX.20140511011015.sac
→ NZ.NZ29.10.BHE.20140511011015.sac
```

Both the filename and SAC `kcmpnm` change. The implementation copies binary
SAC bytes into a temporary file, patches only the eight-byte `kcmpnm` field,
and compares all remaining bytes with the original in bounded-size chunks.
Waveform bytes, timing, `cmpaz/cmpinc`, other headers and any v7 footer are
preserved exactly. Little- and big-endian, evenly sampled SAC time series are supported; SAC
text is not.
Changing HH to BH does not alter sample rate or validate band-code semantics.

Existing target files are never overwritten. Multiple sources mapping to one
output all fail. Occupied targets are checked before processing begins and
again when committing, so a later task cannot take a name freed by another
task. Swaps and occupied chains are not resolved; otherwise independent
renames may still succeed.

Verified temporary output is synced, committed without overwrite, and its
directory synced before the original is deleted. Before deletion, ordinary
output failures retain originals and remove newly committed output. After
deletion begins, errors retain completed output and report incomplete cleanup.
A crash may leave both names present; inspect reported paths before retrying.
Do not use another writer concurrently against the same source tree.

A compact summary and up to five errors are printed. The returned summary has
`total`, `renamed`, `skipped`, `failed`, `deletion_failed`, `issue_samples`,
`errors_csv`, and `has_issues`. Deletion failures are included in `failed`.
Optional `errors_csv` is replaced with all failures in `source,error` columns.
No other log or manifest is written.

## API

::: seispy.waveform.channel_rename.rename_channels

::: seispy.waveform.channel_rename.ChannelRenameSummary
