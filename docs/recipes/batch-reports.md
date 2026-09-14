---
title: Batch reports and logs
description: Track progress and diagnose completed, interrupted, and failed runs.
---

# Batch reports and logs

SeisPy's long-running batch functions persist both a machine-readable JSON
report and a human-readable text log by default:

- `download_waveforms`
- `mass_download_waveforms`
- `convert_mseed_to_sac`
- `format_sac_headers`
- `decimate_waveforms`
- `remove_instrument_response`
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
