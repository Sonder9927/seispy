---
title: Testing strategy
description: Scope and expectations for SeisPy tests.
---

# Testing strategy

Use the smallest test type that protects the behavior:

- **Example tests** document a concrete public contract or failure mode.
- **Hypothesis tests** cover identity combinations, calendar boundaries, and
  interval invariants.
- **Real I/O tests** verify SAC/MiniSEED round trips and safe file handling.
- **Mocked integration tests** verify provider calls without network access.

Do not duplicate a property with many fixed examples. Keep one readable
example when it helps explain the format. Import modules through their normal
package path so coverage remains accurate.

Before submitting a change, run:

```bash
uv run ruff check .
uv run pytest --cov --cov-report=term-missing
uv run --group docs mkdocs build --strict
```

Add a regression test for every corrected bug. Prefer behavior through public
interfaces; test private helpers only when they contain an important isolated
invariant.
