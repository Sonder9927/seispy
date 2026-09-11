# Documentation development

## Check code quality

Run the regular lint checks with Ruff:

```bash
uv run ruff check src tests
```

Run the focused cross-file duplicate-code check with Pylint:

```bash
uv run pylint src/seispy src/halo
```

## Preview the site

```bash
uv sync --group docs
uv run --group docs mkdocs serve
```

Open `http://127.0.0.1:8000/seispy/` and use the search field to locate an
interface.

## Build the site

```bash
uv run --group docs mkdocs build --strict
```

The static website is written to `site/`. Because directory-style URLs are
disabled, `site/index.html` can also be opened directly from the local disk.

## Save a local manual or PDF

The print-site plugin is kept in a separate dependency group and configuration.
It does not affect the normal server or CI build:

```bash
uv run --group docs --group pdf mkdocs build -f mkdocs-print.yml
```

Open `site/print_page.html` locally. It can be archived as HTML or saved as PDF
with the system browser's **Print / Save as PDF** command. This keeps
`mkdocs serve` independent of Chromium, Playwright, Cairo, and Pango. PDF
generation is intentionally not part of the regular CI pipeline.

## Document a public interface

Use an English Google-style docstring. Include units, filesystem effects,
exceptions, and a minimal executable example:

````python
def process(source: Path, scale: float = 1.0) -> Path:
    """Process one input file.

    Args:
        source: Input file path.
        scale: Dimensionless scaling factor.

    Returns:
        Path to the generated file.

    Raises:
        FileNotFoundError: If ``source`` does not exist.

    Examples:
        ```python
        output = process(Path("input.dat"), scale=2.0)
        output.name
        # => 'input.processed.dat'
        ```
    """
````

Export supported names through the nearest package `__init__.py` and add them
to `__all__`. The API pages intentionally use these public exports rather than
documenting every internal helper.
