import time
from typing import Type, List, Any, Mapping, Tuple, Optional

import pytest

from flo.parallel import Threads, Processes, JobLibParallel
from flo.lamb import star
from flo.parallel import PFascade
from flo.stopwatch import StopWatch

SLEEP_SECS = 0.2
NUM_WORKERS = 6
NUM_TASKS = NUM_WORKERS * 3


def for_each_impl(*args) -> List[Any]:
    return [(impl, *args) for impl in (Threads, Processes, JobLibParallel)]


def sleepy(duration, *args, **kwargs) -> Tuple[Tuple[Any, ...], Mapping[str, Any]]:
    import os
    import threading
    print('sleeping for', duration, 'pid', os.getpid(), 'thread', threading.current_thread())
    time.sleep(duration)
    print('done sleeping for', duration, 'pid', os.getpid(), 'thread', threading.current_thread())
    return args, kwargs


@pytest.mark.parametrize('concrete_impl,task,args,kwargs',
                         for_each_impl(sleepy, (1,), dict(foo='you')))
def test_submit(concrete_impl: Type[PFascade], task, args, kwargs):
    print('1 worker submit', concrete_impl, task, args, kwargs)
    with concrete_impl(max_workers=1) as pf:
        with StopWatch() as sw:
            futures = [pf.submit(task, SLEEP_SECS, args[0] + n, **kwargs) for n in range(NUM_TASKS)]
            results = [f.result() for f in futures]
    assert sw.duration() > NUM_TASKS * SLEEP_SECS
    assert results == [((args[0] + n,), kwargs) for n in range(NUM_TASKS)]

    print(NUM_WORKERS, 'worker submit', concrete_impl, task, args, kwargs)

    with concrete_impl(max_workers=NUM_WORKERS) as pf:
        for n in range(2):  # need to warm up pool first
            with StopWatch() as sw:
                futures = [pf.submit(task, SLEEP_SECS, args[0] + n, **kwargs) for n in range(NUM_TASKS)]
                results = [f.result() for f in futures]
    max_duration = 1.2 * NUM_TASKS * SLEEP_SECS / NUM_WORKERS
    assert sw.duration() < max_duration
    assert results == [((args[0] + n,), kwargs) for n in range(NUM_TASKS)]


@pytest.mark.parametrize('concrete_impl,task,args,kwargs',
                         for_each_impl(sleepy, (1,), dict(foo='you')))
def test_map_ordered(concrete_impl: Type[PFascade], task, args, kwargs):
    items = [(SLEEP_SECS / (n + 1), args[0] + n) for n in range(NUM_TASKS)]
    sequential_duration = sum(SLEEP_SECS / (n + 1) for n in range(NUM_TASKS))
    print(1, 'worker map_ordered', concrete_impl, task, args, kwargs)
    with concrete_impl(max_workers=1) as pf:
        with StopWatch() as sw:
            results = pf.map_ordered(star(sleepy), items, **kwargs)
    assert sw.duration() > sequential_duration
    assert results == [((args[0] + n,), kwargs) for n in range(NUM_TASKS)]

    print(NUM_WORKERS, 'worker map_ordered', concrete_impl, task, args, kwargs)
    with concrete_impl(max_workers=NUM_WORKERS) as pf:
        with StopWatch() as sw:
            results = pf.map_ordered(star(sleepy), items, **kwargs)
    max_duration = 1.1 * NUM_TASKS * sequential_duration / NUM_WORKERS
    assert sw.duration() < max_duration
    assert results == [((args[0] + n,), kwargs) for n in range(NUM_TASKS)]


@pytest.mark.parametrize('concrete_impl,batch_size,task,args,kwargs',
                         [*for_each_impl(None, sleepy, (1,), dict(foo='you')),
                          *for_each_impl(1, sleepy, (1,), dict(foo='you')),
                          *for_each_impl(2, sleepy, (1,), dict(foo='you'))
                          ])
def test_map_unordered(concrete_impl: Type[PFascade], batch_size: Optional[int], task, args, kwargs):
    items = [(SLEEP_SECS / (n + 1), args[0] + n) for n in range(NUM_TASKS)]
    sequential_duration = sum(SLEEP_SECS / (n + 1) for n in range(NUM_TASKS))
    print(1, 'worker map_unordered', concrete_impl, task, args, kwargs)
    with concrete_impl(max_workers=1) as pf:
        with StopWatch() as sw:
            results = list(pf.map_unordered(star(sleepy), items, batch_size=batch_size, **kwargs))
    assert results == [((args[0] + n,), kwargs) for n in range(NUM_TASKS)]
    assert sw.duration() > sequential_duration

    print(NUM_WORKERS, 'worker map_unordered', concrete_impl, task, args, kwargs)
    with concrete_impl(max_workers=NUM_WORKERS) as pf:
        with StopWatch() as sw:
            results = list(pf.map_unordered(star(sleepy), items, batch_size=batch_size, **kwargs))
    if batch_size is None:
        assert sw.duration() < 1.1 * NUM_TASKS * sequential_duration / NUM_WORKERS
    assert set((a, tuple(k.items())) for a, k in results) == \
           {((args[0] + n,), tuple(kwargs.items())) for n in range(NUM_TASKS)}
