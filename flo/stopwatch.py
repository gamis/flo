import time
from datetime import timedelta
from functools import wraps
from typing import *


def timed(fcn):
    @wraps
    def wrapper(*args, **kwargs):
        return timeit(fcn, *args, **kwargs)

    return wrapper


def timeit(fcn, *args, **kwargs):
    with StopWatch(fcn.__name__):
        return fcn(*args, **kwargs)


class StopWatch(ContextManager):
    _label: str
    _start: float = None
    _stop: float = None
    _laps: List['StopWatch']

    def __init__(self, label: str = 'StopWatch'):
        self._label = label
        self._laps = []

    def start(self) -> 'StopWatch':
        self._start = time.perf_counter()
        return self

    def stop(self) -> float:
        if self._start is None:
            raise ValueError('Called stop before start')
        self._stop = time.perf_counter()
        self.log()
        return self.duration()

    def duration(self) -> float:
        if self._start is None:
            raise ValueError('Called stop before start')
        return (self._stop or time.perf_counter()) - self._start

    def log(self, logger: Callable[[str], None] = print, prefix: str = "") -> str:
        if not self._start:
            msg = f"{prefix}{self._label}: NOT STARTED"
        else:
            msg = f"{prefix}{self._label}: {timedelta(seconds=self.duration())}"
            if not self._stop:
                msg += " RUNNING"
        logger(msg)
        for l in self._laps:
            l.log(logger, prefix=prefix + '|-')

    def lap(self, label: str) -> 'StopWatch':
        l = StopWatch(label)
        l._start = self._laps[-1]._stop if self._laps else self._start
        l.stop()
        self._laps.append(l)
        return l

    def __enter__(self) -> 'StopWatch':
        return self.start()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
