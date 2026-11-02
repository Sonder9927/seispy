# Test suite

Organize tests around observable behavior: input validation, numerical results,
output integrity, and recovery from failures. Keep related input variations in
named parameter cases and share their setup and assertions where possible.
Each parameter case still receives fresh pytest fixtures.

Before adding a function, check whether an existing behavior test can express
the new case. Avoid separate tests for trivial helpers, implementation constants,
or a migration that the supported interface no longer promises. A function-count
reduction does not imply the same reduction in executed parameter cases.

## MCMC

- `test_mcmc_config.py`: configuration loading, validation and Fortran ordering.
- `test_mcmc_inputs.py`: scalar fields, dispersion and reference velocity inputs.
- `test_mcmc_gridding.py`: grid geometry, alignment and spatial table validation.
- `test_mcmc_priors.py`: spline geometry, physical bounds and serialized priors.
- `test_mcmc_plotting.py`: complete figures from memory and saved point inputs.
- `test_mcmc_collection.py`: collection products, failures, chain records,
  multiplicity-weighted statistics, Fortran reconstruction, LAB detection and
  LAB distribution summaries.
- `test_mcmc_workflow.py`: initialization, point diagnostics and the real
  `init_grids` to `collect_results` path.

Preserve independent numerical evidence, especially the external Fortran model
fixture, posterior repeat weights, and LAB boundary and ambiguity cases.
Waveform tests similarly retain sample preservation, atomic writes, process
recycling, recovery and numerical equivalence checks.

## Running

Run the suite with `pytest`. Compiled event-cutter comparisons are opt-in:
set `SEISPY_CUT_BIN_DIR` to a directory containing `mktraceiodb` and `cutevent`.
Without those executables, pytest reports these cases as skipped.
