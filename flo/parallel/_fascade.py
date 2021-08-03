import abc
import time
from abc import abstractmethod
from collections import deque
from functools import partial
from typing import *

from tqdm import tqdm

from flo.it2 import Mapper
from flo.lamb import as_fcn, UNASSIGNED

I = TypeVar('I')
R = TypeVar('R')


class PFascade(ContextManager, abc.ABC):

    def __init__(self, max_workers: int = None):
        pass

    @abstractmethod
    def submit(self, task: Callable[..., R], *args, **kwargs) -> 'Future':
        """Submits the task to the pool of parallel resources,
         returning a Future reference to the pending result."""
        pass

    def map_ordered(self, mapper: Mapper, it: Sequence[I], timeout: float = None, chunksize: int = 1, **kwargs) -> Sequence[R]:
        """Apply fcn to each element in it in parallel,
        returning a sequence in the same order as it.
        Equivalent to map(fcn,it) but in parallel

        Default impl just calls submit() but subclass may do something more fancy.
        :param mapper Function to apply to each element in it
        :param it Sequence to apply fcn
        :param timeout Single-element timeout
        :param chunksize How many items to send to a worker at a time.
        :param kwargs Additional keyword args to pass to mapper
        :return Fully-excuted result
        """
        fcn = as_fcn(mapper)
        futures = [self.submit(partial(fcn, e, **kwargs)) for e in it]
        return [f.result(timeout=timeout) for f in tqdm(futures)]

    def map_unordered(self, mapper: Mapper, it: Iterable[I], timeout: float = None, batch_size: int = None, **kwargs) -> Iterable[R]:
        """
        Apply fcn to each element in it in parallel,
        but return results as soon as they are available,
        regardless of order.
        Default impl just calls submit() but subclass may do something more fancy.
        :param mapper Function to apply to each element in it
        :param it Iterable to apply fcn
        :param timeout Single-element timeout
        :param batch_size: For extremely large iterables, you may wish to
            submit only a batch to the pool at a time. Note that this is atypical.
            In any case you'd want batch_size >> the number of pool workers.
        :param kwargs Additional keyword args to pass to mapper
        :return Iterable of results, in no particular order
        """
        fcn = as_fcn(mapper)
        src = (self.submit(partial(fcn, e, **kwargs)) for e in it)
        futures: Deque[Future] = deque()
        popper = partial(pop_done, futures, timeout)
        batch_size = max(batch_size or float('inf'), 1)

        for f in src:
            futures.append(f)
            if len(futures) >= batch_size:
                # for constrained batch sizes, check for done futures before adding any more
                ff = popper()
                assert ff != UNASSIGNED
                yield ff
        # all of src has been added to futures, so now we just drain the deque
        while (ff := popper()) != UNASSIGNED:
            yield ff

    @abstractmethod
    def broadcast(self, **kwargs) -> Optional[Mapping[str, 'Future']]:
        pass

    def __enter__(self) -> 'PFascade':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


def pop_done(futures: Deque['Future'], timeout: float) -> R:
    # First clean up empty slots
    while futures and futures[0] == UNASSIGNED:
        futures.popleft()
    while futures:
        # return first future we find that's actually done
        for n, ff in enumerate(futures):
            if ff != UNASSIGNED and ff.done():
                futures[n] = UNASSIGNED
                return ff.result(timeout=timeout)
        # none done? sleep and try again!
        time.sleep(0.1)
    return UNASSIGNED  # indicates no more futures


class Future(abc.ABC):
    def cancel(self) -> bool:
        """Cancel the future if possible.

        Returns True if the future was cancelled, False otherwise. A future
        cannot be cancelled if it is running or has already completed.
        """
        pass

    def cancelled(self) -> bool:
        """Return True if the future was cancelled."""
        pass

    def running(self) -> bool:
        """Return True if the future is currently executing."""
        pass

    def done(self) -> bool:
        """Return True of the future was cancelled or finished executing."""
        pass

    def add_done_callback(self, fn) -> None:
        """Attaches a callable that will be called when the future finishes.

        Args:
            fn: A callable that will be called with this future as its only
                argument when the future completes or is cancelled. The callable
                will always be called by a thread in the same process in which
                it was added. If the future has already completed or been
                cancelled then the callable will be called immediately. These
                callables are called in the order that they were added.
        """
        pass

    def result(self, timeout=None) -> R:
        """Return the result of the call that the future represents.

        Args:
            timeout: The number of seconds to wait for the result if the future
                isn't done. If None, then there is no limit on the wait time.

        Returns:
            The result of the call that the future represents.

        Raises:
            CancelledError: If the future was cancelled.
            TimeoutError: If the future didn't finish executing before the given
                timeout.
            Exception: If the call raised then that exception will be raised.
        """
        pass

    def exception(self, timeout=None) -> Exception:
        """Return the exception raised by the call that the future represents.

        Args:
            timeout: The number of seconds to wait for the exception if the
                future isn't done. If None, then there is no limit on the wait
                time.

        Returns:
            The exception raised by the call that the future represents or None
            if the call completed without raising.

        Raises:
            CancelledError: If the future was cancelled.
            TimeoutError: If the future didn't finish executing before the given
                timeout.
        """
        pass


class Done(Future):
    _result: R
    _exception: Exception

    def __init__(self, *, result: R = None, exception: Exception = None):
        self._result = result
        self._exception = exception

    def cancel(self) -> bool:
        return True

    def cancelled(self) -> bool:
        return False

    def running(self) -> bool:
        return False

    def done(self) -> bool:
        return True

    def add_done_callback(self, fn) -> None:
        fn(self)

    def result(self, timeout=None) -> R:
        if self._exception is not None:
            raise self._exception
        return self._result

    def exception(self, timeout=None) -> Exception:
        return self._exception
