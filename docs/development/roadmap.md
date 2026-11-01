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
- `waveform.correct_clock_drift` provides sample-preserving midpoint shifts from
  StationXML. `waveform.correct_orientation` supports horizontal correction or
  measured three-dimensional axis directions.

## Near-term priorities

1. Stabilize archive and public API contracts.
2. Expand failure-path and end-to-end workflow coverage.
3. Evaluate MassDownloader behavior across providers and large requests.
4. Expand correction validation across instrument types and recording intervals.

Roadmap items are guidance, not release commitments. Update this page in the
same change that materially alters a feature's stability.
