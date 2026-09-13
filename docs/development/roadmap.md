---
title: Roadmap
description: Current stability and near-term development priorities.
---

# Roadmap

SeisPy is in initial development. Public interfaces and archive rules may
still change before the first stable release.

## Current status

- Daily waveform download, archive validation, collation, response removal,
  decimation, and event cutting are the main supported workflows.
- `mass_download_waveforms` is experimental. Use it first on a small interval;
  see the [MassDownloader guide](../recipes/mass-download-waveforms.md).
- `correct_clock_drift` remains provisional and is not part of the current archive
  redesign.

## Near-term priorities

1. Stabilize archive and public API contracts.
2. Expand failure-path and end-to-end workflow coverage.
3. Evaluate MassDownloader behavior across providers and large requests.
4. Improve correction workflows after the primary data path stabilizes.

Roadmap items are guidance, not release commitments. Update this page in the
same change that materially alters a feature's stability.
