"""Bounded scheduling, generation and cleanup contracts."""

import os
from concurrent.futures import Future
from concurrent.futures.process import BrokenProcessPool

import pytest

from seispy import _pool
from seispy.progress import resolve_worker_call

_WORKER_VALUE = None


def _initialize(value):
    global _WORKER_VALUE
    _WORKER_VALUE = value


def _identify(batch):
    return tuple(batch), os.getpid(), _WORKER_VALUE


@pytest.mark.parametrize("quota, generations", [(2, 3), (None, 1)])
def test_real_spawn_generations_and_initializers(quota, generations):
    pids = []
    received = []
    with _pool.process_batches(
        list(range(5)),
        _identify,
        max_workers=1,
        batch_size=3,
        files_per_pool=quota,
        initializer=_initialize,
        initargs=("ready",),
    ) as results:
        for batch, future in results:
            values, pid, initialized = resolve_worker_call(future.result())
            assert values == batch
            assert initialized == "ready"
            received.extend(values)
            pids.append(pid)
    assert sorted(received) == list(range(5))
    assert len(set(pids)) == generations


def test_submission_window_and_non_overlapping_generations(monkeypatch):
    active = False
    submitted = consumed = 0
    peak = 0
    generation_counts = []

    class Executor:
        def __init__(self, **kwargs):
            nonlocal active
            assert not active
            active = True
            self.count = 0

        def submit(self, function, worker, batch, *args):
            nonlocal submitted, peak
            submitted += 1
            self.count += len(batch)
            peak = max(peak, submitted - consumed)
            future = Future()
            future.set_result(batch)
            return future

        def shutdown(self, **kwargs):
            nonlocal active
            assert kwargs == dict(wait=True, cancel_futures=True)
            active = False
            generation_counts.append(self.count)

    monkeypatch.setattr(_pool, "ProcessPoolExecutor", Executor)
    values = []
    with _pool.process_batches(
        list(range(103)),
        _identify,
        max_workers=2,
        batch_size=3,
        files_per_pool=20,
    ) as results:
        for _batch, future in results:
            consumed += 1
            values.extend(future.result())
    assert peak <= 4
    assert generation_counts == [20, 20, 20, 20, 20, 3]
    assert sorted(values) == list(range(103))
    assert not active


def test_consumer_exception_closes_pool(monkeypatch):
    from unittest.mock import Mock

    future = Future()
    future.set_result(None)
    executor = Mock()
    executor.submit.return_value = future
    monkeypatch.setattr(_pool, "ProcessPoolExecutor", Mock(return_value=executor))
    with (
        pytest.raises(RuntimeError, match="consumer"),
        _pool.process_batches([1], _identify, max_workers=1) as results,
    ):
        next(results)
        raise RuntimeError("consumer")
    executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)


def test_broken_pool_is_not_returned_as_an_ordinary_file_failure(monkeypatch):
    from unittest.mock import Mock

    future = Future()
    future.set_exception(BrokenProcessPool("worker died"))
    executor = Mock()
    executor.submit.return_value = future
    factory = Mock(return_value=executor)
    monkeypatch.setattr(_pool, "ProcessPoolExecutor", factory)
    with (
        pytest.raises(BrokenProcessPool),
        _pool.process_batches(
            [1, 2], _identify, max_workers=1, files_per_pool=1
        ) as results,
    ):
        list(results)
    assert factory.call_count == executor.submit.call_count == 1
    executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)


@pytest.mark.parametrize("quota", [0, -1, True, 1.5])
def test_invalid_quota_rejected(quota):
    with (
        pytest.raises(ValueError, match="files_per_pool"),
        _pool.process_batches([], _identify, max_workers=1, files_per_pool=quota),
    ):
        pass


def test_empty_input_does_not_start_workers(monkeypatch):
    from unittest.mock import Mock

    factory = Mock()
    monkeypatch.setattr(_pool, "ProcessPoolExecutor", factory)
    with _pool.process_batches([], _identify, max_workers=1) as results:
        assert list(results) == []
    factory.assert_not_called()
