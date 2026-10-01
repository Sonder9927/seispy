---
title: Understand batch reports and logs
description: Track progress and diagnose completed, interrupted, and failed runs.
---

# Understand batch reports and logs

SeisPy's long-running batch functions persist both a machine-readable JSON
report and a human-readable text log by default:

- `download_waveforms`
- `mass_download_waveforms`
- `convert_mseed_to_sac`
- `format_sac_headers`
- `decimate_waveforms`
- `deconvolve_waveforms`
- `cut_event_waveforms`
- `correct_clock_drift`
- `correct_orientation`

Both artifacts use the same `run_id` and are stored below the function's output
root:

```text
<output>/logs/<task>-<run_id>.log
<output>/logs/reports/<task>-<run_id>.json
```

For in-place decimation or response removal, `<output>` is the source root.

## Interpret run status

The report is created before worker processing starts. Completed waveform
tasks, file batches, events, and stations update the in-memory state
immediately. The atomic JSON checkpoint is flushed approximately every five
seconds, while routine text progress is recorded approximately once per minute.
Warnings, errors, interruptions, and the final progress update are written
immediately. This keeps long-running logs readable while retaining recent
recovery state:

| `status` | Meaning |
| --- | --- |
| `running` | Work started and has not recorded a terminal state. A stale report usually means the process was forcibly terminated. |
| `completed` | The function returned normally. Inspect failure counters or `summary.ok` for partial failures. |
| `interrupted` | A keyboard interrupt or system exit was caught. |
| `failed` | The batch controller stopped because of an unexpected exception. |

Reports are replaced atomically, so an interruption cannot expose a partially
written JSON document. Progress counters describe completed work; workers that
were still in flight at interruption may have committed additional atomic
outputs afterward, and a resumed run should therefore use each function's
normal existing-output policy.

## Inspect artifacts

```python
summary = ...

print(summary.run_id)
print(summary.status)
print(summary.report_path)
print(summary.log_path)
print(summary.ok)
```

`summary.ok` is true only when the lifecycle status is `completed` and the
function-specific failure counters contain no issues.

Set `save_report=False` or `save_log=False` to disable either artifact. Passing
`save_report=None` keeps live checkpoints while the batch runs, but removes the
report after a clean completion to preserve the earlier issue-only behavior.

## Bounded waveform workers

`archive_waveforms`, `filter_waveforms`, `convert_mseed_to_sac`, and
`decimate_waveforms` accept `files_per_pool=1024`, matching response removal.
The quota counts input files across a whole pool, not files per worker. Set it
to a smaller positive integer for more frequent recycling, or `None` to keep
one pool. The initial default is tunable, not a measured optimum.

Only `2 * max_workers` tasks are submitted at a time. Batches are created on
demand and shortened at generation boundaries. The old pool drains and exits
before the next pool starts. Filtering, conversion and decimation retain only
cumulative counts and bounded samples; archiving already uses incremental
counters. Unexpected pool failures stop the run without automatic retries.

Workers use the platform default start method: `fork` on Linux, `spawn` on
macOS and Windows. On `spawn` platforms script entry points must use an
`if __name__ == "__main__":` guard, and calling one of these functions at module
level without it raises `RuntimeError` naming the offending line instead of
starting workers. Input paths are still collected in memory,
and an individual file can still require substantial decoding/processing
memory. Recycling is not a hard RSS limit. The 60-second progress logging and
5-second report checkpoint intervals are unchanged.
