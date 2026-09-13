# SeisPy

SeisPy is an [ObsPy](https://docs.obspy.org/)-based toolkit for reproducible
seismic-data workflows. It provides task-oriented APIs for acquiring,
organizing, processing, and preparing waveform data for analysis and inversion.

📖 **Documentation:** [sonder9927.github.io/seispy](https://sonder9927.github.io/seispy/)

> [!WARNING]
> SeisPy is in early development (`0.1.0`). Public APIs, input formats, and
> directory conventions may change. Keep backups of original data when using it
> for research workflows.

## Features

- Download StationXML, earthquake catalogs, and continuous waveforms from FDSN
  services
- Convert, organize, merge, and annotate MiniSEED and SAC data
- Remove instrument responses with ObsPy or SAC
- Apply phase-preserving, SAC-compatible waveform decimation
- Cut event windows and correct station clock drift or sensor orientation
- Prepare grid inputs and collect results for MCMC inversion

## Requirements

- Python 3.12 or later
- [uv](https://docs.astral.sh/uv/)
- A licensed [SAC](https://ds.iris.edu/ds/nodes/dmc/software/downloads/sac/)
  installation for SAC-backed response removal, external event cutting, and the
  FIR coefficients used by waveform decimation
- [GMT](https://www.generic-mapping-tools.org/) for PyGMT-based grid and
  dispersion workflows

SAC and GMT are required only by the workflows that use them.

## Installation

Clone the repository and create the locked uv environment:

```bash
git clone https://github.com/Sonder9927/seispy.git
cd seispy
uv sync
```

The base install covers acquisition and standard waveform processing. Install
only the optional workflows you need:

```bash
uv sync --extra plot       # download-statistics plots
uv sync --extra mcmc       # MCMC preparation and result collection
uv sync --extra notebook   # interactive tutorials
uv sync --all-extras       # every optional workflow
```

Run Python commands inside the project environment with `uv run`:

```bash
uv run python
```

## Quick example

Convert a MiniSEED directory to a SAC directory tree while preserving the
source files:

```python
from seispy import waveform

summary = waveform.convert_mseed_to_sac(
    "data/mseed",
    "data/sac",
    pattern="*.mseed",
    remove_original=False,
)

print(summary.succeeded, summary.failed)
```

For complete, copyable workflows, start with the
[quick start](https://sonder9927.github.io/seispy/quickstart.html) or choose a
[task-oriented recipe](https://sonder9927.github.io/seispy/task-guide.html).
The documentation covers expected directory layouts, input tables, safety
notes, output summaries, and the full public API.

## Interactive GeoNet tutorial

To learn the complete workflow from raw StationXML analysis and location-code
selection to XML-guided 100 Hz MiniSEED downloads, open the Chinese Marimo
notebook from the repository root:

```bash
uv run marimo edit notebooks/geonet_nz_100hz_download.py
```

Use `uv run marimo run notebooks/geonet_nz_100hz_download.py` for a read-only
application view. The notebook does not start the large waveform download until
you explicitly click its download button.

## Development

Install development and documentation dependencies:

```bash
uv sync --group dev --group docs
```

Run the checks and build the documentation:

```bash
uv run ruff check .
uv run pytest --cov --cov-report=term-missing
uv run --group docs mkdocs build --strict
```

See the
[documentation development guide](https://sonder9927.github.io/seispy/development.html)
for local preview and printable-manual instructions.

## License

SeisPy is released under the [MIT License](LICENSE).
