---
title: GeoNet 100 Hz interactive notebook
description: Learn the complete workflow from raw StationXML analysis to XML-guided MiniSEED downloads with Marimo.
---

# GeoNet 100 Hz interactive notebook

Use the Chinese-language Marimo notebook to explore the complete GeoNet
download decision chain:

1. Download the raw StationXML.
2. Summarize stations, channels, and sample rates.
3. Identify stations with multiple location codes in the XML.
4. Interpret location and channel codes using GeoNet's official definitions.
5. Select one complete three-component location at any given time and generate
   a reduced XML inventory.
6. Use the reduced XML to guide exact MiniSEED requests with
   `download_waveforms`.

## Interactive learning

Run this command from the SeisPy repository root:

```bash
uv run marimo edit notebooks/geonet_nz_100hz_download.py
```

This opens the Marimo editor, where you can read, run, and modify individual
cells. StationXML acquisition and the large waveform download are both guarded
by explicit buttons, so opening the notebook does not start a network request.

For an application-only view without notebook editing, run:

```bash
uv run marimo run notebooks/geonet_nz_100hz_download.py
```

## Fixed study scope

The notebook currently uses this study configuration:

- Network: `NZ`
- Longitude: `170–180°`
- Latitude: `-43.5–-34°`
- Time: 2023-08-01 to 2025-05-01, with an exclusive end time
- Channels: active 100 Hz seismic channels in StationXML, excluding the `HDF`
  pressure channel

After generating the reduced XML, the notebook calls the optimized
`download_waveforms` function directly. NSLC identity, sample rate, and channel
epoch boundaries in the XML jointly determine the download tasks.
`station="*"`, `location="*"`, and `channel="*"` do not bypass the XML; they
mean that no additional selector is applied to the reduced inventory.

!!! warning "Confirm the large download"

    Waveform acquisition starts only after you click the notebook's
    **开始/继续下载全部 MiniSEED** button. Review the reduced XML summary and
    location-code decisions before starting the full download.
