---
title: Recipes
description: Copyable SeisPy examples organized by processing task.
---

# Recipes

Recipes are the main learning interface for SeisPy. They favor safe defaults and
small, complete examples over exhaustive parameter lists.

## Recommended workflow

```text
download metadata + waveforms
            ↓
convert / sort / merge SAC
            ↓
remove response + resample
            ↓
format headers / cut events / correct stations
            ↓
prepare and collect MCMC results
```

## How to use a recipe

1. Check the expected input layout.
2. Copy the smallest example and replace paths.
3. Keep destructive options disabled.
4. Inspect the returned summary and output files.
5. Open the linked API entry for advanced parameters.

Return to [Choose a task](../task-guide.md) for the complete task index.
