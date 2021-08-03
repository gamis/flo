import warnings
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Mapping, Callable, ContextManager

from flo.parallel import PFascade, Future, R, Done


class Threads(PFascade, ContextManager):
    _exec: ThreadPoolExecutor

    def __init__(self, max_workers: int = None, thread_name_prefix: str = '', initializer: Callable[[], None] = None, initargs: tuple = ()):
        PFascade.__init__(self, max_workers)
        self._exec = ThreadPoolExecutor(max_workers=max_workers,
                                        thread_name_prefix=thread_name_prefix,
                                        initializer=initializer,
                                        initargs=initargs)

    def submit(self, task: Callable[[], R], *args, **kwargs) -> Future:
        args = tuple(a.result() if isinstance(a, Future) else a for a in args)
        kwargs = {k: (a.result() if isinstance(a, Future) else a) for k, a in kwargs.items()}
        return self._exec.submit(task, *args, **kwargs)

    def broadcast(self, **kwargs) -> Optional[Mapping[str, Future]]:
        return {k: Done(result=v) for k, v in kwargs.items()}

    def __enter__(self) -> 'Threads':
        self._exec.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return self._exec.__exit__(exc_type, exc_val, exc_tb)
