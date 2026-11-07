# Test suite

Organize tests around observable behavior: input validation, numerical results,
output integrity, and recovery from failures. Keep related input variations in
named parameter cases and share their setup and assertions where possible.
Each parameter case still receives fresh pytest fixtures.

Before adding a function, check whether an existing behavior test can express
the new case. Avoid separate tests for trivial helpers, implementation constants,
or a migration that the supported interface no longer promises. A function-count
reduction does not imply the same reduction in executed parameter cases.

## Keep tests focused

Do not add standalone tests for exact output filenames, directory spelling,
segment suffixes, retired aliases, or migration from obsolete naming schemes.
Document those conventions in the relevant recipe. Workflow tests should discover
outputs and check sample values, header identity, time alignment, and preservation
of source data rather than repeat filename templates.

Keep collision protection, input/output separation, atomic writes, and recovery
checks: those prevent data loss and are distinct from naming conventions. Prefer
observable results over assertions about private helper calls or a particular
optimization implementation.

For independent parameter axes, use representative combinations rather than a
full Cartesian product. Retain distinct numerical and failure modes, including
sampling boundaries, byte orders, supported file formats, and process failures.
Remove redundant cases instead of hiding them in a loop or disabling collection.
Report test-function counts and expanded case counts separately when describing
suite size; pytest's passed/skipped totals count expanded cases, not functions.

## MCMC

- `test_mcmc_config.py`: configuration loading, validation and Fortran ordering.
- `test_mcmc_inputs.py`: scalar fields, dispersion and reference velocity inputs.
- `test_mcmc_gridding.py`: grid geometry, alignment and spatial table validation.
- `test_mcmc_priors.py`: spline geometry, physical bounds and serialized priors.
- `test_mcmc_plotting.py`: complete figures from memory and saved point inputs.
- `test_mcmc_collection.py`: minimal output tables, failure isolation, source
  preservation and mean-profile LAB detection.
- `test_mcmc_workflow.py`: initialization, point diagnostics and the real
  `init_grids` to `collect_results` path.

Preserve independent numerical evidence, especially LAB boundary and ambiguity
cases.
Waveform tests retain sample preservation, atomic writes, process recycling,
recovery and numerical equivalence checks.

## Running

Run the suite with `pytest`. Compiled event-cutter comparisons are opt-in:
set `SEISPY_CUT_BIN_DIR` to a directory containing `mktraceiodb` and `cutevent`.
Without those executables, pytest reports these cases as skipped.
