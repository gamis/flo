import warnings
from functools import partial
from typing import *

from joblib import Parallel, delayed
from joblib._parallel_backends import ImmediateResult

from flo import as_fcn
from flo.parallel import PFascade, Future, I, R, Done, Mapper


class JobLibParallel(PFascade, ContextManager):
    _exec: Parallel

    def __init__(self, max_workers: int = None,
                 backend=None, verbose=0, timeout=None,
                 pre_dispatch='2 * n_jobs', batch_size='auto',
                 temp_folder=None, max_nbytes='1M', mmap_mode='r',
                 prefer=None, require=None):
        PFascade.__init__(self, max_workers)
        self._exec = Parallel(n_jobs=max_workers, backend=backend, verbose=verbose,
                              timeout=timeout, pre_dispatch=pre_dispatch, batch_size=batch_size,
                              temp_folder=temp_folder, max_nbytes=max_nbytes, mmap_mode=mmap_mode,
                              prefer=prefer, require=require)

    def submit(self, task: Callable[[], R], *args, **kwargs) -> Future:
        args = tuple(a.result() if isinstance(a, Future) else a for a in args)
        kwargs = {k: (a.result() if isinstance(a, Future) else a) for k, a in kwargs.items()}

        if not self._exec._managed_backend:
            self._exec._initialize_backend()
        part_task = partial(task, *args, **kwargs)
        future = self._exec._backend.apply_async(part_task)
        if isinstance(future, ImmediateResult):
            return Done(result=future.get())
        return future

    def broadcast(self, **kwargs) -> Optional[Mapping[str, Future]]:
        warnings.warn(f"{type(self).__name__} doesn't yet fully support the broadcast operation. "
                      "Values 'broadcast' are just going to be sent via map anyway.")
        return {k: Done(result=v) for k, v in kwargs.items()}

    def map_ordered(self, mapper: Mapper, it: Sequence[I], timeout: float = None, chunksize: int = 1, **kwargs) -> Sequence[R]:
        if chunksize != self._exec.batch_size:
            warnings.warn(f"{type(self).__name__} doesn't support a per-call chunksize. Please initialize with batch_size instead.")

        if timeout != self._exec.timeout:
            warnings.warn(f"{type(self).__name__} doesn't support a per-call timeout. Please initialize with timeout instead.")

        fcn = as_fcn(mapper)
        return self._exec(delayed(fcn)(e, **kwargs) for e in it)

    def map_unordered(self, mapper: Mapper, it: Iterable[I], timeout: float = None, batch_size: int = None, **kwargs) -> Iterable[R]:
        return self.map_ordered(mapper=mapper, it=it, timeout=timeout, chunksize=batch_size, **kwargs)

    def __enter__(self) -> 'JobLibParallel':
        self._exec.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._exec.__exit__(exc_type, exc_val, exc_tb)
