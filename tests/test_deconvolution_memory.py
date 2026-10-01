"""Bounded aggregation and process-generation contracts."""

import os
import weakref
from concurrent.futures import ProcessPoolExecutor
from importlib import import_module
from unittest.mock import Mock

import numpy as np
import pytest
from obspy import Stream, Trace
from obspy.core.inventory import Inventory, Network, Station

from seispy.progress import WorkerCall

removal = import_module("seispy.deconvolution.removal")


def _record_pid(function, targets, *args):
    for target in targets:
        target.write_text(str(os.getpid()))
    return WorkerCall(
        value=removal._WorkerSummary(total=len(targets), succeeded=len(targets))
    )


def _record_loaded_inventory(function, targets, *args):
    inventory = removal._WORKER_INVENTORY
    for target in targets:
        target.write_text(f"{type(inventory).__name__}:{len(inventory.networks)}")
    return WorkerCall(
        value=removal._WorkerSummary(total=len(targets), succeeded=len(targets))
    )


@pytest.mark.parametrize("limit", [0, 3])
def test_accumulator_retains_only_bounded_samples(tmp_path, limit):
    accumulator = removal._SummaryAccumulator(limit)
    issue = removal.DeconvolutionIssue(tmp_path, tmp_path, "trace_filtered", "short")
    for _ in range(10000):
        accumulator.add(
            removal._WorkerSummary(
                total=1,
                skipped=1,
                traces_filtered=2,
                issue_samples=(issue,),
                filter_samples=(issue,),
            )
        )
    snapshot = accumulator.snapshot()
    assert snapshot.total == snapshot.skipped == 10000
    assert snapshot.traces_filtered == 20000
    assert len(snapshot.issue_samples) == len(snapshot.filter_samples) == limit
    accumulator.add(removal._WorkerSummary(total=1, succeeded=1))
    assert snapshot.total == 10000


@pytest.mark.parametrize("quota, generations", [(2, 3), (None, 1)])
def test_real_process_generations_preserve_all_inputs(
    tmp_path, monkeypatch, quota, generations
):
    targets = [tmp_path / str(index) for index in range(5)]
    pools = []

    def pool(**kwargs):
        # Exercise real process generations/recycling without loading metadata.
        kwargs.pop("initializer")
        kwargs.pop("initargs")
        executor = ProcessPoolExecutor(**kwargs)
        pools.append(executor)
        return executor

    monkeypatch.setattr(removal, "ProcessPoolExecutor", pool)
    monkeypatch.setattr(removal, "call_with_warnings", _record_pid)
    result = removal._run_deconvolution_batches(
        targets,
        None,
        "obspy",
        None,
        tmp_path,
        tmp_path,
        1,
        removal.DEFAULT_PRE_FILTER,
        (),
        removal.DEFAULT_TRACE_FILTER,
        1,
        3,
        2,
        len(targets),
        Mock(),
        files_per_pool=quota,
    )
    assert result.total == result.succeeded == 5
    assert result.failed == 0
    assert len(pools) == generations
    pids = [target.read_text() for target in targets]
    assert len(set(pids)) == generations
    if quota:
        assert pids[0] == pids[1]
        assert pids[2] == pids[3]


def test_workers_load_the_inventory_from_a_file(tmp_path, monkeypatch):
    target = tmp_path / "0"
    inventory = Inventory([Network("NZ", stations=[Station("AAA", 0.0, 0.0, 0.0)])])
    responses_path = removal._write_worker_inventory(
        inventory, tmp_path / "inventory.pkl"
    )
    submitted = {}
    real_pool = ProcessPoolExecutor

    def pool(**kwargs):
        submitted.update(kwargs)
        return real_pool(**kwargs)

    monkeypatch.setattr(removal, "ProcessPoolExecutor", pool)
    monkeypatch.setattr(removal, "call_with_warnings", _record_loaded_inventory)
    result = removal._run_deconvolution_batches(
        [target],
        responses_path,
        "obspy",
        None,
        tmp_path,
        tmp_path,
        1,
        removal.DEFAULT_PRE_FILTER,
        (),
        removal.DEFAULT_TRACE_FILTER,
        1,
        1,
        2,
        1,
        Mock(),
    )

    assert result.total == result.succeeded == 1
    # The initializer argument stays a small path; the worker still gets the
    # inventory, loaded from the file the parent wrote once.
    assert submitted["initargs"] == (str(responses_path), "obspy", None)
    assert target.read_text() == "Inventory:1"


def test_previous_file_stream_is_released_before_next_read(tmp_path, monkeypatch):
    references = []

    def read(target):
        assert all(ref() is None for ref in references)
        stream = Stream([Trace(np.zeros(100, dtype=np.float32))])
        references.extend([weakref.ref(stream), weakref.ref(stream[0].data)])
        return stream

    monkeypatch.setattr(removal.obspy, "read", read)
    result = removal._process_obspy_targets(
        [tmp_path / "a.sac", tmp_path / "b.sac"],
        None,
        tmp_path,
        tmp_path / "out",
        1,
        removal.DEFAULT_PRE_FILTER,
        (),
    )
    assert result.skipped == 2
    assert all(ref() is None for ref in references)


def test_broken_pool_stops_without_retry(tmp_path, monkeypatch):
    from concurrent.futures import Future
    from concurrent.futures.process import BrokenProcessPool
    from unittest.mock import MagicMock

    executor = MagicMock()
    executor.__enter__.return_value = executor
    future = Future()
    future.set_exception(BrokenProcessPool("worker exited"))
    executor.submit.return_value = future
    factory = Mock(return_value=executor)
    monkeypatch.setattr(removal, "ProcessPoolExecutor", factory)
    run = Mock()
    with pytest.raises(BrokenProcessPool):
        removal._run_deconvolution_batches(
            [tmp_path / "a"],
            None,
            "obspy",
            None,
            tmp_path,
            tmp_path,
            1,
            removal.DEFAULT_PRE_FILTER,
            (),
            removal.DEFAULT_TRACE_FILTER,
            1,
            1,
            1,
            1,
            run,
            files_per_pool=1,
        )
    assert factory.call_count == executor.submit.call_count == 1
    assert run.checkpoint.call_args.kwargs["completed"] == 0
