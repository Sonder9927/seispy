"""Bounded task submission and non-overlapping process-pool generations."""

from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from contextlib import closing, contextmanager

from seispy._main_guard import require_reimport_safe_entry_point
from seispy.progress import call_with_warnings


def validate_files_per_pool(value):
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, int) or value < 1
    ):
        raise ValueError("files_per_pool must be a positive integer or None")


@contextmanager
def process_batches(
    files,
    worker,
    args=(),
    *,
    max_workers,
    batch_size=1,
    files_per_pool=1024,
    initializer=None,
    initargs=(),
    run=None,
    operation="this operation",
):
    """Yield (input batch, completed Future), retaining at most 2W tasks.

    Quotas count input files across the entire pool. The consumer must consume
    each Future immediately; it must not retain results or submit more work.
    On consumer failure, cancel pending work and drain workers before returning.
    Broken pools propagate without retries. Workers use the platform default
    start method, so on spawn platforms callers from scripts must sit behind a
    ``__main__`` guard; ``operation`` names the public function in the error
    raised when they do not.
    """
    require_reimport_safe_entry_point(operation)
    validate_files_per_pool(files_per_pool)
    if max_workers < 1 or batch_size < 1:
        raise ValueError("max_workers and batch_size must be positive")
    with closing(
        _results(
            files,
            worker,
            args,
            max_workers,
            batch_size,
            files_per_pool,
            initializer,
            initargs,
            run,
        )
    ) as results:
        yield results


def _results(
    files, worker, args, workers, batch_size, quota, initializer, initargs, run
):
    if not files:
        return
    generation_size = quota or len(files)
    for start in range(0, len(files), generation_size):
        stop = min(start + generation_size, len(files))
        if run is not None:
            run.info(
                "pool_generation=%d files=%d",
                start // generation_size + 1,
                stop - start,
            )
        executor = ProcessPoolExecutor(
            max_workers=workers,
            initializer=initializer,
            initargs=initargs,
        )
        pending = {}
        index = start
        try:
            while pending or index < stop:
                while index < stop and len(pending) < 2 * workers:
                    batch = tuple(files[index : min(index + batch_size, stop)])
                    future = executor.submit(call_with_warnings, worker, batch, *args)
                    pending[future] = batch
                    index += len(batch)
                done, _ = wait(pending, return_when=FIRST_COMPLETED)
                broken = None
                for future in done:
                    batch = pending.pop(future)
                    if isinstance(future.exception(), BrokenProcessPool):
                        broken = future.exception()
                        continue
                    yield batch, future
                if broken is not None:
                    raise broken
        finally:
            executor.shutdown(wait=True, cancel_futures=True)
