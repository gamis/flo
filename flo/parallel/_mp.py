import warnings
from concurrent.futures import ProcessPoolExecutor
from functools import partial
from multiprocessing.context import BaseContext
from typing import *

from flo import as_fcn
from flo.parallel import PFascade, Future, I, R, Done, Mapper


class Processes(PFascade, ContextManager):
    _exec: ProcessPoolExecutor

    def __init__(self, max_workers: int = None, mp_context: BaseContext = None, initializer: Callable[[], None] = None, initargs: tuple = ()):
        PFascade.__init__(self, max_workers)
        self._exec = ProcessPoolExecutor(max_workers=max_workers,
                                         mp_context=mp_context,
                                         initializer=initializer,
                                         initargs=initargs)

    def submit(self, task: Callable[[], R], *args, **kwargs) -> Future:
        args = tuple(a.result() if isinstance(a, Future) else a for a in args)
        kwargs = {k: (a.result() if isinstance(a, Future) else a) for k, a in kwargs.items()}
        return self._exec.submit(task, *args, **kwargs)

    def broadcast(self, **kwargs) -> Optional[Mapping[str, Future]]:
        warnings.warn(f"{type(self).__name__} doesn't yet fully support the broadcast operation. "
                      "Values 'broadcast' are just going to be sent via submit/map anyway.")
        return {k: Done(result=v) for k, v in kwargs.items()}

    def map_ordered(self, mapper: Mapper, it: Sequence[I], timeout: float = None, chunksize: int = 1, **kwargs) -> Sequence[R]:
        fcn = as_fcn(mapper)
        if kwargs:
            fcn = partial(fcn, **kwargs)
        return list(self._exec.map(fcn, it, timeout=timeout, chunksize=chunksize))

    def __enter__(self) -> 'Processes':
        self._exec.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._exec.__exit__(exc_type, exc_val, exc_tb)
